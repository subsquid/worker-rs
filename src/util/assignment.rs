use core::str;
use std::collections::{BTreeMap, HashMap, VecDeque};

use anyhow::anyhow;
use crypto_box::{
    aead::{Aead, AeadCore, OsRng},
    PublicKey, SalsaBox, SecretKey,
};
use curve25519_dalek::edwards::CompressedEdwardsY;
use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::base64::Base64;
use serde_with::serde_as;
use sha2::Digest;
use sha2::Sha512;
use sha3::digest::generic_array::GenericArray;

use crate::util::signature::timed_hmac_now;

// TODO: Move to common crate to be shared with Scheduler and Portal

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Chunk {
    pub id: String,
    pub base_url: String,
    pub files: HashMap<String, String>,
    pub size_bytes: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Dataset {
    pub id: String,
    pub base_url: String,
    pub chunks: Vec<Chunk>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Headers {
    worker_id: String,
    worker_signature: String,
}

#[serde_as]
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct EncryptedHeaders {
    #[serde_as(as = "Base64")]
    identity: Vec<u8>,
    #[serde_as(as = "Base64")]
    nonce: Vec<u8>,
    #[serde_as(as = "Base64")]
    ciphertext: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WorkerAssignment {
    status: String,
    chunks_deltas: Vec<u64>,
    encrypted_headers: EncryptedHeaders,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Assignment {
    datasets: Vec<Dataset>,
    worker_assignments: HashMap<String, WorkerAssignment>,
    #[serde(skip)]
    chunk_map: Option<HashMap<String, u64>>,
    #[serde(skip)]
    pub id: String,
}

#[derive(Serialize, Deserialize)]
pub struct NetworkAssignment {
    pub(crate) url: String,
    pub(crate) id: String,
}

#[derive(Serialize, Deserialize)]
pub struct NetworkState {
    pub(crate) network: String,
    pub(crate) assignment: NetworkAssignment,
}

impl Assignment {
    pub fn add_chunk(&mut self, chunk: Chunk, dataset_id: String, dataset_url: String) {
        match self
            .datasets
            .iter_mut()
            .find(|dataset| dataset.id == dataset_id)
        {
            Some(dataset) => dataset.chunks.push(chunk),
            None => self.datasets.push(Dataset {
                id: dataset_id,
                base_url: dataset_url,
                chunks: vec![chunk],
            }),
        }
        self.chunk_map = None
    }

    pub async fn try_download(
        url: String,
        previous_id: Option<String>,
    ) -> Result<Option<Self>, anyhow::Error> {
        let response_state = reqwest::get(url).await?;
        let network_state: NetworkState = response_state.json().await?;
        if Some(network_state.assignment.id.clone()) == previous_id {
            return Ok(None);
        }
        let assignment_url = network_state.assignment.url;
        let response_assignment = reqwest::get(assignment_url).await?;
        let compressed_assignment = response_assignment.bytes().await?;
        let decoder = GzDecoder::new(&compressed_assignment[..]);
        let mut result: Assignment = serde_json::from_reader(decoder)?;
        result.id = network_state.assignment.id;
        Ok(Some(result))
    }

    pub fn insert_assignment(&mut self, peer_id: String, status: String, chunks_deltas: Vec<u64>) {
        self.worker_assignments.insert(
            peer_id.clone(),
            WorkerAssignment {
                status,
                chunks_deltas,
                encrypted_headers: Default::default(),
            },
        );
    }

    pub fn dataset_chunks_for_peer_id(&self, peer_id: String) -> Option<Vec<Dataset>> {
        let local_assignment = match self.worker_assignments.get(&peer_id) {
            Some(worker_assignment) => worker_assignment,
            None => return None,
        };
        let mut result: Vec<Dataset> = Default::default();
        let mut idxs: VecDeque<u64> = Default::default();
        let mut cursor = 0;
        for v in &local_assignment.chunks_deltas {
            cursor += v;
            idxs.push_back(cursor);
        }
        cursor = 0;
        for u in &self.datasets {
            if idxs.is_empty() {
                break;
            }
            let mut filtered_chunks: Vec<Chunk> = Default::default();
            for v in &u.chunks {
                if idxs[0] < cursor {
                    return None; // Malformed diffs
                }
                if idxs[0] == cursor {
                    filtered_chunks.push(v.clone());
                    idxs.pop_front();
                }
                if idxs.is_empty() {
                    break;
                }
                cursor += 1;
            }
            if !filtered_chunks.is_empty() {
                result.push(Dataset {
                    id: u.id.clone(),
                    base_url: u.base_url.clone(),
                    chunks: filtered_chunks,
                });
            }
        }
        Some(result)
    }

    pub fn headers_for_peer_id(
        &self,
        peer_id: String,
        secret_key: &Vec<u8>,
    ) -> Result<BTreeMap<String, String>, anyhow::Error> {
        let Some(local_assignment) = self.worker_assignments.get(&peer_id) else {
            return Err(anyhow!("Can not find assignment for {peer_id}"));
        };
        let EncryptedHeaders {
            identity,
            nonce,
            ciphertext,
        } = &local_assignment.encrypted_headers;
        let temporary_public_key = PublicKey::from_slice(identity.as_slice())?;
        let big_slice = Sha512::default().chain_update(secret_key).finalize();
        let worker_secret_key = SecretKey::from_slice(&big_slice[00..32])?;
        let shared_box = SalsaBox::new(&temporary_public_key, &worker_secret_key);
        let generic_nonce = GenericArray::clone_from_slice(nonce);
        let Ok(decrypted_plaintext) = shared_box.decrypt(&generic_nonce, &ciphertext[..]) else {
            return Err(anyhow!("Can not decrypt payload"));
        };
        let plaintext_headers = std::str::from_utf8(&decrypted_plaintext)?;
        let headers = serde_json::from_str::<Value>(plaintext_headers)?;
        let mut result: BTreeMap<String, String> = Default::default();
        let Some(headers_dict) = headers.as_object() else {
            return Err(anyhow!("Can not parse encrypted map"));
        };
        for (k, v) in headers_dict {
            result.insert(k.to_string(), v.as_str().unwrap().to_string());
        }
        Ok(result)
    }

    pub fn chunk_index(&mut self, chunk_id: String) -> Option<u64> {
        if self.chunk_map.is_none() {
            let mut chunk_map: HashMap<String, u64> = Default::default();
            let mut idx = 0;
            for dataset in &self.datasets {
                for chunk in &dataset.chunks {
                    chunk_map.insert(chunk.id.clone(), idx);
                    idx += 1;
                }
            }
            self.chunk_map = Some(chunk_map);
        };
        self.chunk_map.as_ref().unwrap().get(&chunk_id).cloned()
    }

    pub fn regenerate_headers(&mut self, cloudflare_storage_secret: String) {
        let temporary_secret_key = SecretKey::generate(&mut OsRng);
        let temporary_public_key_bytes = *temporary_secret_key.public_key().as_bytes();

        for (worker_id, worker_assignment) in &mut self.worker_assignments {
            let worker_signature = timed_hmac_now(worker_id, &cloudflare_storage_secret);

            let headers = Headers {
                worker_id: worker_id.to_string(),
                worker_signature,
            };

            let pub_key_edvards_bytes = &bs58::decode(worker_id).into_vec().unwrap()[6..];
            let public_edvards_compressed =
                CompressedEdwardsY::from_slice(pub_key_edvards_bytes).unwrap();
            let public_edvards = public_edvards_compressed.decompress().unwrap();
            let public_montgomery = public_edvards.to_montgomery();
            let worker_public_key = PublicKey::from(public_montgomery);

            let shared_box = SalsaBox::new(&worker_public_key, &temporary_secret_key);
            let nonce = SalsaBox::generate_nonce(&mut OsRng);
            let plaintext = serde_json::to_vec(&headers).unwrap();
            let ciphertext = shared_box.encrypt(&nonce, &plaintext[..]).unwrap();

            worker_assignment.encrypted_headers = EncryptedHeaders {
                identity: temporary_public_key_bytes.to_vec(),
                nonce: nonce.to_vec(),
                ciphertext,
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use sqd_network_transport::Keypair;

    use super::*;

    #[test]
    fn it_works() {
        let mut assignment: Assignment = Default::default();
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id().to_base58();
        let private_key = keypair.try_into_ed25519().unwrap().secret();

        assignment.insert_assignment(peer_id.clone(), "Ok".to_owned(), Default::default());
        assignment.regenerate_headers("SUPERSECRET".to_owned());
        let headers = assignment
            .headers_for_peer_id(peer_id.clone(), &private_key.as_ref().to_vec())
            .unwrap();
        let decrypted_id = headers.get("worker-id").unwrap();
        assert_eq!(peer_id, decrypted_id.to_owned());
    }
}
