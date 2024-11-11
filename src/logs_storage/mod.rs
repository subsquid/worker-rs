use anyhow::Result;
use sqd_messages::{ProstMsg, QueryExecuted};
use tokio_rusqlite::{named_params, Connection};

pub const LOGS_PER_PAGE: usize = 256;

pub struct LogsStorage {
    db: Connection,
}

mod sql {
    // timestamp is in milliseconds
    pub const CREATE: &str = r#"
        BEGIN;
        CREATE TABLE IF NOT EXISTS query_logs_v2(query_id STRING PRIMARY KEY, timestamp INTEGER, log_msg BLOB);
        CREATE INDEX IF NOT EXISTS idx_query_logs_v2_timestamp ON query_logs_v2(timestamp);
        COMMIT;"#;

    pub const CLEANUP: &str = "DELETE FROM query_logs_v2 WHERE timestamp < ?";

    pub const INSERT: &str =
        "INSERT INTO query_logs_v2(query_id, timestamp, log_msg) VALUES(?, ?, ?)";

    pub const SELECT: &str = r#"
        SELECT log_msg, query_id FROM (
            SELECT query_id, log_msg, SUM(LENGTH(log_msg)) OVER (ORDER BY rowid) cum_len
            FROM query_logs_v2
            WHERE
                timestamp >= :timestamp
                AND (
                    :last_query_id IS NULL
                    OR rowid > (SELECT rowid FROM query_logs_v2 WHERE query_id = :last_query_id)
                )
        )
        WHERE cum_len <= :limit
        ORDER BY rowid"#;
}

impl LogsStorage {
    pub async fn new(logs_path: &str) -> Result<Self> {
        let db = Connection::open(logs_path).await?;

        db.call(|db| {
            db.execute_batch(sql::CREATE)?;
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
        self.db
            .call(move |db| {
                db.prepare_cached(sql::INSERT)
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
    ) -> Result<Vec<QueryExecuted>> {
        self.db
            .call_unwrap(move |db| {
                let mut stmt = db
                    .prepare_cached(sql::SELECT)
                    .expect("Couldn't prepare logs query");
                let rows = stmt.query(named_params! {
                    ":timestamp": from_timestamp_ms,
                    ":last_query_id": last_query_id,
                    ":limit":  max_bytes,
                })?;
                let logs = rows.and_then(|row| {
                    let log_msg: Vec<u8> = row.get(0)?;
                    let log = QueryExecuted::decode(&log_msg[..]).expect("Invalid log proto in DB");
                    Ok(log)
                });
                logs.collect()
            })
            .await
    }

    pub async fn cleanup(&self, until: u64) -> Result<()> {
        self.db
            .call(move |db| {
                db.execute(sql::CLEANUP, [until])?;
                Ok(())
            })
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
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

        let loaded = logs_storage.get_logs(10000, None, 1000000).await.unwrap();
        assert_eq!(loaded.len(), 5);
        assert_eq!(loaded[0].query.as_ref().unwrap().query_id, "a");
        assert_eq!(loaded[0].result, Some(Result::Ok(Default::default())));
        assert_eq!(loaded[1].query.as_ref().unwrap().query_id, "b");
        assert_eq!(loaded[1].result, logs[1].result);
        assert_eq!(loaded[2].query.as_ref().unwrap().query_id, "c");
        assert_eq!(loaded[3].query.as_ref().unwrap().query_id, "d");
        assert_eq!(loaded[4].query.as_ref().unwrap().query_id, "e");

        for (index, (call, expected)) in [
            (logs_storage.get_logs(10000, None, 122), vec!["a"]),
            (logs_storage.get_logs(10000, None, 380), vec!["a", "b"]),
            (
                logs_storage.get_logs(10100, None, 1000),
                vec!["b", "d", "e"],
            ),
            (logs_storage.get_logs(10050, None, 381), vec!["b", "c", "d"]),
            (logs_storage.get_logs(10200, None, 1000), vec!["e"]),
            (logs_storage.get_logs(15000, None, 1000), vec![]),
            (
                logs_storage.get_logs(10000, Some("b".to_string()), 300),
                vec!["c", "d"],
            ),
            (
                logs_storage.get_logs(10000, Some("d".to_string()), 122),
                vec!["e"],
            ),
            (
                logs_storage.get_logs(10100, Some("b".to_string()), 300),
                vec!["d", "e"],
            ),
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(
                call.await
                    .unwrap()
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
