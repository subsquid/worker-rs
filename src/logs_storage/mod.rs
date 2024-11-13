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
                CREATE INDEX IF NOT EXISTS idx_query_logs_v2_timestamp ON query_logs_v2(timestamp);
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

    pub async fn get_logs(
        &self,
        from_timestamp_ms: u64,
        last_query_id: Option<String>,
        max_bytes: usize,
    ) -> Result<LoadedLogs> {
        self.db
            .call_unwrap(move |db| {
                let mut stmt = db
                    .prepare_cached(r"
                        SELECT log_msg, query_id
                        FROM query_logs_v2
                        WHERE
                            timestamp >= :timestamp
                            AND (
                                :last_query_id IS NULL
                                OR rowid > (SELECT rowid FROM query_logs_v2 WHERE query_id = :last_query_id)
                            )
                        ORDER BY timestamp, query_id
                        "
                    )
                    .expect("Couldn't prepare logs query");
                let mut rows = stmt.query(named_params! {
                    ":timestamp": from_timestamp_ms,
                    ":last_query_id": last_query_id,
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
                db.execute("DELETE FROM query_logs_v2 WHERE timestamp < ?", [until])?;
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

        let LoadedLogs::All(loaded) = logs_storage.get_logs(10000, None, 1000000).await.unwrap()
        else {
            panic!("Expected LoadedLogs::All");
        };
        assert_eq!(loaded.len(), 5);
        assert_eq!(loaded[0].query.as_ref().unwrap().query_id, "a");
        assert_eq!(loaded[0].result, Some(Result::Ok(Default::default())));
        assert_eq!(loaded[1].query.as_ref().unwrap().query_id, "b");
        assert_eq!(loaded[1].result, logs[1].result);
        assert_eq!(loaded[2].query.as_ref().unwrap().query_id, "c");
        assert_eq!(loaded[3].query.as_ref().unwrap().query_id, "d");
        assert_eq!(loaded[4].query.as_ref().unwrap().query_id, "e");

        for (index, (call, expected, drained)) in [
            (logs_storage.get_logs(10000, None, 122), vec!["a"], false),
            (
                logs_storage.get_logs(10000, None, 380),
                vec!["a", "b"],
                false,
            ),
            (
                logs_storage.get_logs(10100, None, 1000),
                vec!["b", "d", "e"],
                true,
            ),
            (
                logs_storage.get_logs(10050, None, 381),
                vec!["b", "c", "d"],
                false,
            ),
            (logs_storage.get_logs(10200, None, 1000), vec!["e"], true),
            (logs_storage.get_logs(15000, None, 1000), vec![], true),
            (
                logs_storage.get_logs(10000, Some("b".to_string()), 300),
                vec!["c", "d"],
                false,
            ),
            (
                logs_storage.get_logs(10000, Some("d".to_string()), 122),
                vec!["e"],
                true,
            ),
            (
                logs_storage.get_logs(10100, Some("b".to_string()), 300),
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
                logs_storage.get_logs(0, None, 1000),
                vec!["b", "d", "e"],
                true,
            ),
            (logs_storage.get_logs(10000, None, 150), vec!["b"], false),
            (
                logs_storage.get_logs(10000, Some("b".to_string()), 150),
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
