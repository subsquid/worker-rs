use anyhow::Result;
use sqd_messages::{ProstMsg, QueryExecuted};
use tokio_rusqlite::{named_params, Connection};

pub const LOGS_PER_PAGE: usize = 256;

pub struct LogsStorage {
    db: Connection,
}

pub enum LoadedLogs {
    All(Vec<QueryExecuted>),
    Partial(Vec<QueryExecuted>),
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

    pub async fn save_log(&self, log: QueryExecuted) -> Result<()> {
        let query = log.query.as_ref().expect("Log should be well formed");
        let timestamp = query.timestamp_ms;
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
    pub async fn get_logs(
        &self,
        from_timestamp_ms: u64,
        to_timestamp_ms: u64,
        from_query_id: Option<String>,
        max_bytes: usize,
    ) -> Result<LoadedLogs> {
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

                let mut total_len = 0;
                let mut logs = Vec::new();
                while let Some(row) = rows.next()? {
                    let log_msg: Vec<u8> = row.get(0)?;
                    total_len += log_msg.len();
                    if total_len > max_bytes {
                        return Ok(LoadedLogs::Partial(logs));
                    }
                    let log = QueryExecuted::decode(&log_msg[..]).expect("Invalid log proto in DB");
                    logs.push(log);
                }
                Ok(LoadedLogs::All(logs))
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

    fn query_log(id: impl Into<String>, timestamp_ms: u64) -> QueryExecuted {
        QueryExecuted {
            query: Some(sqd_messages::Query {
                query_id: id.into(),
                dataset: "eth-main".to_owned(),
                query: [' '; 100].iter().collect(),
                timestamp_ms,
                ..Default::default()
            }),
            result: Some(Result::Ok(Default::default())),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_logs_storage() {
        let logs_storage = LogsStorage::new(":memory:").await.unwrap();

        let mut logs = [
            query_log("a", 10000), // 122 bytes
            query_log("b", 10100), // 137 bytes
            query_log("c", 10050), // 122 bytes
            query_log("d", 10100), // 122 bytes
            query_log("e", 10200), // 122 bytes
        ];
        logs[1].result = Some(query_error::Err::BadRequest("Invalid query".to_owned()).into());
        assert_eq!(logs[0].encoded_len(), 122);
        assert_eq!(logs[1].encoded_len(), 137);

        for log in logs.clone() {
            logs_storage.save_log(log).await.unwrap();
        }

        let LoadedLogs::All(loaded) = logs_storage
            .get_logs(10000, 10500, None, 1000000)
            .await
            .unwrap()
        else {
            panic!("Expected LoadedLogs::All");
        };
        assert_eq!(loaded.len(), 5);
        assert_eq!(loaded[0].query.as_ref().unwrap().query_id, "a");
        assert_eq!(loaded[0].result, Some(Result::Ok(Default::default())));
        assert_eq!(loaded[1].query.as_ref().unwrap().query_id, "c");
        assert_eq!(loaded[2].query.as_ref().unwrap().query_id, "b");
        assert_eq!(loaded[2].result, logs[1].result);
        assert_eq!(loaded[3].query.as_ref().unwrap().query_id, "d");
        assert_eq!(loaded[4].query.as_ref().unwrap().query_id, "e");

        for (index, (call, expected, drained)) in [
            (
                logs_storage.get_logs(10000, 11000, None, 122),
                vec!["a"],
                false,
            ),
            (
                logs_storage.get_logs(10000, 11000, None, 380),
                vec!["a", "c"],
                false,
            ),
            (
                logs_storage.get_logs(10100, 11000, None, 1000),
                vec!["b", "d", "e"],
                true,
            ),
            (
                logs_storage.get_logs(10050, 11000, None, 381),
                vec!["c", "b", "d"],
                false,
            ),
            (
                logs_storage.get_logs(10200, 11000, None, 1000),
                vec!["e"],
                true,
            ),
            (
                logs_storage.get_logs(15000, 11000, None, 1000),
                vec![],
                true,
            ),
            (
                logs_storage.get_logs(10100, 11000, Some("d".to_string()), 122),
                vec!["e"],
                true,
            ),
            (
                logs_storage.get_logs(10100, 11000, Some("b".to_string()), 300),
                vec!["d", "e"],
                true,
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let logs = match call.await.unwrap() {
                LoadedLogs::All(logs) => {
                    assert!(drained);
                    logs
                }
                LoadedLogs::Partial(logs) => {
                    assert!(!drained);
                    logs
                }
            };
            assert_eq!(
                logs.into_iter()
                    .map(|log| log.query.unwrap().query_id)
                    .collect::<Vec<_>>(),
                expected,
                "case #{}",
                index,
            );
        }

        logs_storage.cleanup(10100).await.unwrap();
        for (index, (call, expected, drained)) in [
            (
                logs_storage.get_logs(0, 11000, None, 1000),
                vec!["b", "d", "e"],
                true,
            ),
            (
                logs_storage.get_logs(10000, 11000, None, 150),
                vec!["b"],
                false,
            ),
            (
                logs_storage.get_logs(10100, 11000, Some("b".to_string()), 150),
                vec!["d"],
                false,
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let logs = match call.await.unwrap() {
                LoadedLogs::All(logs) => {
                    assert!(drained);
                    logs
                }
                LoadedLogs::Partial(logs) => {
                    assert!(!drained);
                    logs
                }
            };
            assert_eq!(
                logs.into_iter()
                    .map(|log| log.query.unwrap().query_id)
                    .collect::<Vec<_>>(),
                expected,
                "case #{}",
                index,
            );
        }
    }
}
