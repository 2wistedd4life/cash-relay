pub mod relay_manager;

use hyper::{client::HttpConnector, Client as HyperClient, Uri};
use tracing::warn;

pub fn parse_uri_warn(uri_str: &str) -> Option<Uri> {
    let uri = uri_str.parse();
    match uri {
        Ok(some) => Some(some),
        Err(err) => {
            warn!(message = "uri parsing failed", error=%err, uri = %uri_str);
            None
        }
    }
}

#[derive(Clone)]
pub struct PeerHandler {
    peers: Vec<Uri>,
    client: HyperClient<HttpConnector>,
}

impl PeerHandler {
    pub fn new(peers: Vec<Uri>) -> Self {
        Self {
            peers,
            client: HyperClient::new(),
        }
    }

    pub fn sample() {
        // TODO
    }

    pub fn broadcast() {
        // TODO
    }
}
