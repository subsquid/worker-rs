use std::sync::atomic::Ordering;

use anyhow::Result;
use atomic_enum::atomic_enum;
use sqd_messages::{ProstMsg, QueryExecuted};
use tokio_rusqlite::Connection;

pub const LOGS_PER_PAGE: usize = 256;

pub struct LogsStorage {
    db: Connection,
    init_state: AtomicInitState,
}

#[atomic_enum]
#[derive(PartialEq)]
enum InitState {
    NotInitialized = 0,
    Initializing = 1,
    Initialized = 2,
}

// TODO: reimplement according to https://github.com/subsquid/sqd-network/issues/122
impl LogsStorage {
    pub async fn new(logs_path: &str) -> Result<Self> {
        let db = Connection::open(logs_path).await?;
        let state = db
            .call(|db| {
                db.execute_batch(
                    r#"
                    BEGIN;
                    CREATE TABLE IF NOT EXISTS query_logs(seq_no INTEGER PRIMARY KEY, log_msg BLOB);
                    CREATE TABLE IF NOT EXISTS next_seq_no(seq_no);
                    COMMIT;"#,
                )?;
                let has_next_seq_no = db
                    .prepare_cached("SELECT seq_no FROM next_seq_no")?
                    .exists(())?;
                match has_next_seq_no {
                    false => Ok(InitState::NotInitialized),
                    true => Ok(InitState::Initialized),
                }
            })
            .await?;

        Ok(Self {
            db,
            init_state: state.into(),
        })
    }

    pub fn is_initialized(&self) -> bool {
        self.init_state.load(Ordering::SeqCst) == InitState::Initialized
    }

    pub async fn save_log(&self, _log: QueryExecuted) -> Result<()> {
        // assert!(self.is_initialized());
        // self.db
        //     .call(move |db| {
        //         let tx = db.transaction()?;
        //         log.timestamp_ms = timestamp_now_ms();
        //         tx.prepare_cached("INSERT INTO query_logs SELECT seq_no, ? FROM next_seq_no")
        //             .expect("Couldn't prepare logs insertion query")
        //             .execute([log.encode_to_vec()])?;
        //         tx.prepare_cached("UPDATE next_seq_no SET seq_no = seq_no + 1")
        //             .expect("Couldn't prepare next_seq_no update query")
        //             .execute(())?;
        //         tx.commit()?;
        //         Ok(())
        //     })
        //     .await?;
        Ok(())
    }

    /// All logs with sequence numbers up to `last_collected_seq_no` have been saved by the logs collector
    /// and should be discarded from the storage.
    pub async fn _logs_collected(&self, last_collected_seq_no: Option<u64>) {
        tracing::info!(
            "Logs up to {} collected",
            last_collected_seq_no.unwrap_or(0)
        );
        let next_seq_no = last_collected_seq_no.map(|x| x + 1).unwrap_or(0);
        match self.init_state.compare_exchange(
            InitState::NotInitialized,
            InitState::Initializing,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(InitState::NotInitialized) => {
                self.db
                    .call_unwrap(move |db| {
                        db.execute("INSERT INTO next_seq_no VALUES(?)", [next_seq_no])
                    })
                    .await
                    .expect("Couldn't initialize logs storage");
                self.init_state
                    .store(InitState::Initialized, Ordering::SeqCst);
                tracing::info!("Initialized logs storage");
            }
            Err(InitState::Initializing) => {
                tracing::warn!("Trying to initialize logs storage concurrently");
            }
            Err(InitState::Initialized) => {
                self.db
                    .call_unwrap(move |db| {
                        let stored_next_seq_no: u64 = db
                            .prepare_cached("SELECT seq_no FROM next_seq_no")
                            .expect("Couldn't prepare next_seq_no query")
                            .query_row((), |row| row.get(0))
                            .expect("Couldn't find next_seq_no");
                        if stored_next_seq_no < next_seq_no {
                            panic!(
                                "Trying to collect logs up to seq_no {} while next_seq_no is {}",
                                next_seq_no, stored_next_seq_no
                            );
                        }
                        db.prepare_cached("DELETE FROM query_logs WHERE seq_no < ?")
                            .expect("Couldn't prepare logs deletion query")
                            .execute([next_seq_no])
                    })
                    .await
                    .expect("Couldn't delete logs from DB");
            }
            _ => unreachable!("Invalid compare_exchange result while handling collected logs"),
        };
    }

    pub async fn _get_logs(&self, from_seq_no: Option<u64>) -> Result<Vec<QueryExecuted>> {
        let from_seq_no = from_seq_no.unwrap_or(0);
        self.db
            .call_unwrap(move |db| {
                let mut stmt = db
                    .prepare_cached(
                        "SELECT seq_no, log_msg FROM query_logs WHERE seq_no >= ? ORDER BY seq_no LIMIT ?",
                    )
                    .expect("Couldn't prepare logs query");
                let logs = stmt.query([from_seq_no, LOGS_PER_PAGE as u64])?.and_then(|row| {
                    let seq_no: u64 = row.get(0)?;
                    let log_msg: Vec<u8> = row.get(1)?;
                    let mut log: QueryExecuted =
                        QueryExecuted::decode(&log_msg[..]).expect("Invalid log proto in DB");
                    // log.seq_no = Some(seq_no);
                    Ok(log)
                });
                logs.collect::<Result<Vec<_>>>()
            })
            .await
    }
}
