use anyhow::Result;
use sqd_messages::{ProstMsg, QueryExecuted};
use tokio_rusqlite::{named_params, Connection};
use tracing::instrument;

pub struct LogsStorage {
    db: Connection,
}

impl LogsStorage {
    pub async fn new(logs_path: &str) -> Result<Self> {
        let db = Connection::open(logs_path).await?;

        db.call(|db| {
            // timestamp is in milliseconds
            db.execute_batch(r"
                BEGIN;
                CREATE TABLE IF NOT EXISTS query_logs_v2(query_id STRING PRIMARY KEY, timestamp INTEGER, log_msg BLOB);
                CREATE INDEX IF NOT EXISTS idx_query_logs_v2_timestamp_query_id ON query_logs_v2(timestamp, query_id);
                COMMIT;"
            )?;
            Ok(())
        })
        .await?;

        Ok(Self { db })
    }

    #[instrument(skip_all)]
    pub async fn save_log(&self, log: QueryExecuted) -> Result<()> {
        let query = log.query.as_ref().expect("Log should be well formed");
        let timestamp = log.timestamp_ms;
        let id = query.query_id.clone();
        let log_msg = log.encode_to_vec();
        // TODO: consider storing fields in separate columns
        self.db
            .call(move |db| {
                db.prepare_cached(
                    "INSERT INTO query_logs_v2(query_id, timestamp, log_msg) VALUES(?, ?, ?)",
                )
                .expect("Couldn't prepare INSERT query")
                .execute((id, timestamp, log_msg))?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    // Loads all logs from the given range ordered by timestamps.
    // If from_query_id is given and there are multiple logs with the timestamp equal `from_timestamp_ms`,
    // the logs with query_id > from_query_id will be returned for that timestamp.
    //
    // Note that logs are not always added in the order of their timestamps,
    // so only the logs in the distant past should be fetched with this method.
    #[instrument(skip_all)]
    pub async fn get_logs(
        &self,
        from_timestamp_ms: u64,
        to_timestamp_ms: u64,
        from_query_id: Option<String>,
        max_bytes: usize,
    ) -> Result<sqd_messages::QueryLogs> {
        self.db
            .call_unwrap(move |db| {
                let mut stmt = db
                    .prepare_cached(
                        r"
                        SELECT log_msg, query_id
                        FROM query_logs_v2
                        WHERE (timestamp, query_id) > (:from_timestamp, IFNULL(:from_query_id, ''))
                            AND timestamp <= :to_timestamp
                        ORDER BY timestamp, query_id",
                    )
                    .expect("Couldn't prepare logs query");
                let mut rows = stmt.query(named_params! {
                    ":from_timestamp": from_timestamp_ms,
                    ":from_query_id": from_query_id,
                    ":to_timestamp": to_timestamp_ms,
                })?;

                let mut total_len = 10; // for other fields
                let mut logs = Vec::new();
                while let Some(row) = rows.next()? {
                    let log_msg: Vec<u8> = row.get(0)?;
                    total_len += log_msg.len() + 2; // each record is prepended with 2-bytes metadata
                    if total_len > max_bytes {
                        return Ok(sqd_messages::QueryLogs {
                            queries_executed: logs,
                            has_more: true,
                        });
                    }
                    let log = QueryExecuted::decode(&log_msg[..]).expect("Invalid log proto in DB");
                    logs.push(log);
                }
                Ok(sqd_messages::QueryLogs {
                    queries_executed: logs,
                    has_more: false,
                })
            })
            .await
    }

    pub async fn cleanup(&self, until: u64) -> Result<()> {
        self.db
            .call(move |db| {
                db.prepare_cached(
                    "
                    DELETE FROM query_logs_v2 WHERE timestamp < ?;
                    VACUUM;",
                )
                .expect("Couldn't prepare cleanup query")
                .execute([until])?;
                Ok(())
            })
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use core::panic;

    use sqd_messages::{query_error, query_executed::Result};

    use super::*;

    static REQUEST_ID: &str = "67e55044-10b1-426f-9247-bb680e5fe0c8";

    fn query_log(id: impl Into<String>, timestamp_ms: u64) -> QueryExecuted {
        QueryExecuted {
            query: Some(sqd_messages::Query {
                query_id: id.into(),
                request_id: REQUEST_ID.to_string(),
                dataset: "eth-main".to_owned(),
                query: [' '; 100].iter().collect(),
                timestamp_ms,
                ..Default::default()
            }),
            timestamp_ms,
            result: Some(Result::Ok(Default::default())),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_logs_storage() {
        let logs_storage = LogsStorage::new(":memory:").await.unwrap();

        let mut logs = [
            query_log("a", 10000), // 125+36+2 bytes
            query_log("b", 10100), // 140+36+2 bytes (because it is overwritten below)
            query_log("c", 10050), // 125+36+2 bytes
            query_log("d", 10100), // 125+36+2 bytes
            query_log("e", 10200), // 125+36+2 bytes
        ];
        logs[1].result = Some(query_error::Err::BadRequest("Invalid query".to_owned()).into());
        assert_eq!(logs[0].encoded_len(), 164);
        assert_eq!(logs[1].encoded_len(), 179);

        for log in logs.clone() {
            logs_storage.save_log(log).await.unwrap();
        }

        let sqd_messages::QueryLogs {
            queries_executed: loaded,
            has_more: false,
        } = logs_storage
            .get_logs(10000, 10500, None, 1000000)
            .await
            .unwrap()
        else {
            panic!("Expected all logs to be loaded");
        };
        assert_eq!(loaded.len(), 5);
        assert_eq!(loaded[0].query.as_ref().unwrap().query_id, "a");
        assert_eq!(loaded[0].result, Some(Result::Ok(Default::default())));
        assert_eq!(loaded[1].query.as_ref().unwrap().query_id, "c");
        assert_eq!(loaded[2].query.as_ref().unwrap().query_id, "b");
        assert_eq!(loaded[2].result, logs[1].result);
        assert_eq!(loaded[3].query.as_ref().unwrap().query_id, "d");
        assert_eq!(loaded[4].query.as_ref().unwrap().query_id, "e");

        for (index, (call, expected, has_more, bytes)) in [
            (
                logs_storage.get_logs(10000, 11000, None, 179),
                vec!["a"],
                true,
                167 + 2,
            ),
            (
                logs_storage.get_logs(10000, 11000, None, 346),
                vec!["a", "c"],
                true,
                334 + 2,
            ),
            (
                logs_storage.get_logs(10100, 11000, None, 1000),
                vec!["b", "d", "e"],
                false,
                516,
            ),
            (
                logs_storage.get_logs(10050, 11000, None, 528),
                vec!["c", "b", "d"],
                true,
                518,
            ),
            (
                logs_storage.get_logs(10200, 11000, None, 1000),
                vec!["e"],
                false,
                167,
            ),
            (
                logs_storage.get_logs(15000, 11000, None, 1000),
                vec![],
                false,
                0,
            ),
            (
                logs_storage.get_logs(10100, 11000, Some("d".to_string()), 177),
                vec!["e"],
                false,
                167,
            ),
            (
                logs_storage.get_logs(10100, 11000, Some("b".to_string()), 344),
                vec!["d", "e"],
                false,
                334,
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let logs = call.await.unwrap();

            assert_eq!(logs.has_more, has_more, "case #{}", index);
            assert_eq!(logs.encoded_len(), bytes, "case #{}", index);
            assert_eq!(
                logs.queries_executed
                    .into_iter()
                    .map(|log| log.query.unwrap().query_id)
                    .collect::<Vec<_>>(),
                expected,
                "case #{}",
                index,
            );
        }

        logs_storage.cleanup(10100).await.unwrap();
        for (index, (call, expected, has_more)) in [
            (
                logs_storage.get_logs(0, 11000, None, 1000),
                vec!["b", "d", "e"],
                false,
            ),
            (
                logs_storage.get_logs(10000, 11000, None, 195),
                vec!["b"],
                true,
            ),
            (
                logs_storage.get_logs(10100, 11000, Some("b".to_string()), 195),
                vec!["d"],
                true,
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let logs = call.await.unwrap();
            assert_eq!(logs.has_more, has_more);
            assert_eq!(
                logs.queries_executed
                    .into_iter()
                    .map(|log| log.query.unwrap().query_id)
                    .collect::<Vec<_>>(),
                expected,
                "case #{}",
                index,
            );
        }
    }
}
