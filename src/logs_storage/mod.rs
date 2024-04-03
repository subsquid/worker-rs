use subsquid_messages::QueryExecuted;

#[derive(Default)]
pub struct LogsStorage {
    logs: Vec<(usize, QueryExecuted)>,
    next_seq_no: Option<usize>,
}

impl LogsStorage {
    pub fn is_initialized(&self) -> bool {
        self.next_seq_no.is_some()
    }

    pub fn save_log(&mut self, mut log: QueryExecuted) {
        let next_seq_no = self.next_seq_no.expect("Logs storage is not initialized");
        self.next_seq_no = Some(next_seq_no + 1);
        log.seq_no = Some(next_seq_no as u64);
        log.timestamp_ms = Some(timestamp_now_ms());
    }

    pub fn logs_collected(&mut self, last_collected_seq_no: Option<usize>) {
        let next_seq_no = last_collected_seq_no.map(|x| x + 1).unwrap_or(0);
        self.logs.retain(|(seq_no, _log)| seq_no >= &next_seq_no);
        self.next_seq_no = Some(next_seq_no);
    }

    pub fn take_logs(&mut self) -> Vec<QueryExecuted> {
        self.logs.drain(..).map(|(_seq_no, log)| log).collect()
    }
}

#[inline(always)]
fn timestamp_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Invalid current time")
        .as_millis() as u64
}
