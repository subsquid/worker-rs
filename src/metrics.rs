use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::info::Info;
use prometheus_client::registry::Registry;

lazy_static::lazy_static! {
    pub static ref QUERY_OK: Counter = Default::default();
    pub static ref BAD_REQUEST: Counter = Default::default();
    pub static ref SERVER_ERROR: Counter = Default::default();
}

pub fn register_metrics(registry: &mut Registry, info: Info<Vec<(String, String)>>) {
    registry.register("worker_info", "Worker info", info);
    registry.register(
        "num_successful_queries",
        "Number of queries which executed successfully",
        QUERY_OK.clone(),
    );
    registry.register(
        "num_bad_requests",
        "Number of received invalid queries",
        BAD_REQUEST.clone(),
    );
    registry.register(
        "num_server_errors",
        "Number of queries which resulted in server error",
        SERVER_ERROR.clone(),
    );
}
