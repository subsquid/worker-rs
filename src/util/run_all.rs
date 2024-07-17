// Runs all futures concurrently. If any of them finishes, cancels the rest.
#[macro_export]
macro_rules! run_all {
    ( $cancellation_token:expr, $( $fut:expr ),+ $(,)?) => {
        tokio::join!(
            $(
                async {
                    let result = $fut.await;
                    $cancellation_token.cancel();
                    result
                }
            ),*
        )
    };
}

#[cfg(test)]
mod tests {
    use tokio_util::sync::CancellationToken;

    #[tokio::test(start_paused = true)]
    async fn test_run_all() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use tokio::time::{sleep, Duration};

        let cancellation_token = CancellationToken::new();
        let cancellation_token_clone = cancellation_token.clone();

        let counter = Arc::new(AtomicUsize::new(0));
        let fut1 = async {
            tokio::select! {
                _ = cancellation_token_clone.cancelled() => return,
                _ = sleep(Duration::from_secs(1)) => counter.fetch_add(1, Ordering::Relaxed),
            };
        };
        let cancellation_token_clone = cancellation_token.clone();
        let fut2 = async {
            tokio::select! {
                _ = cancellation_token_clone.cancelled() => return,
                _ = sleep(Duration::from_secs(2)) => counter.fetch_add(1, Ordering::Relaxed),
            };
        };
        let cancellation_token_clone = cancellation_token.clone();
        let fut3 = async {
            tokio::select! {
                _ = cancellation_token_clone.cancelled() => return,
                _ = sleep(Duration::from_secs(3)) => counter.fetch_add(1, Ordering::Relaxed),
            };
        };

        run_all!(cancellation_token, fut1, fut2, fut3);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }
}
