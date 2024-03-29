use std::collections::HashMap;

use contract_client::{Address, GatewayCluster, U256};
use subsquid_network_transport::PeerId;

#[derive(Default)]
pub struct ComputeUnitsStorage {
    operators: HashMap<Address, Operator>,
    operator_by_gateway_id: HashMap<PeerId, Address>,
}

pub enum Status {
    Spent,
    NotEnoughCU,
}

struct Operator {
    pub allocated_cus: U256,
    pub spent_cus: U256,
}

impl ComputeUnitsStorage {
    pub fn update_allocations(&mut self, clusters: Vec<GatewayCluster>) {
        self.operators.drain();
        self.operator_by_gateway_id.drain();

        for cluster in clusters {
            self.operators.insert(cluster.operator_addr, Operator {
                allocated_cus: cluster.allocated_computation_units,
                spent_cus: 0.into(),
            });
            for gateway in cluster.gateway_ids {
                self.operator_by_gateway_id.insert(gateway, cluster.operator_addr);
            }
        }
    }

    pub fn try_spend_cus(&mut self, gateway_id: PeerId, used_units: u64) -> Status {
        let operator_id = match self.operator_by_gateway_id.get(&gateway_id) {
            Some(id) => id,
            None => {
                return Status::NotEnoughCU;
            }
        };
        let operator = self.operators.get_mut(operator_id).unwrap();
        if operator.spent_cus + used_units < operator.allocated_cus {
            operator.spent_cus += used_units.into();
            Status::Spent
        } else {
            Status::NotEnoughCU
        }
    }
}