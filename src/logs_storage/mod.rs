use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use prost::Message;
use subsquid_messages::QueryExecuted;
use tokio_rusqlite::Connection;

pub struct LogsStorage {
    db: Connection,
    has_next_seq_no: AtomicBool,
}

impl LogsStorage {
    pub async fn new(logs_path: &str) -> Result<Self> {
        let db = Connection::open(logs_path).await?;
        let has_next_seq_no = db
            .call(|db| {
                db.execute_batch(
                    r#"
                    BEGIN;
                    CREATE TABLE IF NOT EXISTS query_logs(seq_no INTEGER PRIMARY KEY, log_msg BLOB);
                    CREATE TABLE IF NOT EXISTS next_seq_no(seq_no);
                    COMMIT;"#,
                )?;
                let has_next_seq_no = db.prepare("SELECT seq_no FROM next_seq_no")?.exists(())?;
                Ok(has_next_seq_no)
            })
            .await?;

        Ok(Self {
            db,
            has_next_seq_no: AtomicBool::new(has_next_seq_no),
        })
    }

    pub fn is_initialized(&self) -> bool {
        self.has_next_seq_no.load(Ordering::SeqCst)
    }

    pub async fn save_log(&self, mut log: QueryExecuted) -> Result<()> {
        assert!(self.is_initialized());
        self.db
            .call(move |db| {
                let tx = db.transaction()?;
                log.timestamp_ms = Some(timestamp_now_ms());
                tx.prepare_cached("INSERT INTO query_logs SELECT seq_no, ? FROM next_seq_no")
                    .expect("Couldn't prepare logs insertion query")
                    .execute([log.encode_to_vec()])?;
                tx.prepare_cached("UPDATE next_seq_no SET seq_no = seq_no + 1")
                    .expect("Couldn't prepare next_seq_no update query")
                    .execute(())?;
                tx.commit()?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// All logs with sequence numbers up to `last_collected_seq_no` have been saved by the logs collector
    /// and should be discarded from the storage.
    pub async fn logs_collected(&self, last_collected_seq_no: Option<u64>) {
        tracing::debug!(
            "Logs up to {} collected",
            last_collected_seq_no.unwrap_or(0)
        );
        let next_seq_no = last_collected_seq_no.map(|x| x + 1).unwrap_or(0);
        if self.is_initialized() {
            self.db
                .call_unwrap(move |db| {
                    db.prepare_cached("DELETE FROM query_logs WHERE seq_no < ?")
                        .expect("Couldn't prepare logs deletion query")
                        .execute([next_seq_no])
                })
                .await
                .expect("Couldn't delete logs from DB");
        } else {
            self.db
                .call_unwrap(move |db| {
                    db.execute("INSERT INTO next_seq_no VALUES(?)", [next_seq_no])
                })
                .await
                .expect("Couldn't initialize logs storage");
            if self.has_next_seq_no.swap(true, Ordering::SeqCst) {
                panic!("Tried to initialize logs storage twice");
            }
            tracing::info!("Initialized logs storage");
        }
    }

    pub async fn get_logs(&self) -> Result<Vec<QueryExecuted>> {
        self.db
            .call_unwrap(|db| {
                let mut stmt = db
                    .prepare_cached("SELECT seq_no, log_msg FROM query_logs")
                    .expect("Couldn't prepare logs query");
                let logs = stmt.query([])?.and_then(|row| {
                    let seq_no: u64 = row.get(0)?;
                    let log_msg: Vec<u8> = row.get(1)?;
                    let mut log: QueryExecuted =
                        QueryExecuted::decode(&log_msg[..]).expect("Invalid log proto in DB");
                    log.seq_no = Some(seq_no);
                    Ok(log)
                });
                logs.collect::<Result<Vec<_>>>()
            })
            .await
    }
}

#[inline(always)]
fn timestamp_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Invalid current time")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use subsquid_messages::query_executed::Result;

    use super::*;

    #[tokio::test]
    async fn test_logs_storage() {
        let logs_storage = LogsStorage::new(":memory:").await.unwrap();
        assert!(!logs_storage.is_initialized());
        logs_storage.logs_collected(Some(2)).await;
        assert!(logs_storage.is_initialized());
        logs_storage
            .save_log(QueryExecuted {
                query: Some(subsquid_messages::Query {
                    query_id: Some("0".to_owned()),
                    dataset: Some("eth-main".to_owned()),
                    query: Some("{}".to_owned()),
                    ..Default::default()
                }),
                result: Some(Result::Ok(Default::default())),
                ..Default::default()
            })
            .await
            .unwrap();
        logs_storage
            .save_log(QueryExecuted {
                query: Some(subsquid_messages::Query {
                    query_id: Some("1".to_owned()),
                    dataset: Some("eth-main".to_owned()),
                    query: Some("{}".to_owned()),
                    ..Default::default()
                }),
                result: Some(Result::BadRequest("Invalid query".to_owned())),
                ..Default::default()
            })
            .await
            .unwrap();

        let logs = logs_storage.get_logs().await.unwrap();
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0].seq_no, Some(3));
        assert_eq!(
            logs[0].query.as_ref().unwrap().query_id.as_deref(),
            Some("0")
        );
        assert_eq!(logs[0].result, Some(Result::Ok(Default::default())));
        assert_eq!(logs[1].seq_no, Some(4));
        assert_eq!(
            logs[1].query.as_ref().unwrap().query_id.as_deref(),
            Some("1")
        );
        assert_eq!(
            logs[1].result,
            Some(Result::BadRequest("Invalid query".to_owned()))
        );

        logs_storage.logs_collected(Some(3)).await;
        let logs = logs_storage.get_logs().await.unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].seq_no, Some(4));
        assert_eq!(
            logs[0].query.as_ref().unwrap().query_id.as_deref(),
            Some("1")
        );
        assert_eq!(
            logs[0].result,
            Some(Result::BadRequest("Invalid query".to_owned()))
        );

        logs_storage.logs_collected(Some(100)).await;
        let logs = logs_storage.get_logs().await.unwrap();
        assert!(logs.is_empty());
    }
}
