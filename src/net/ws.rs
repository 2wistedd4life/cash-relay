use std::sync::Arc;

use bitcoincash_addr::Address;
use dashmap::DashMap;
use futures::prelude::*;
use thiserror::Error;
use tokio::{
    sync::broadcast,
    time::{interval, Duration},
};
use tracing::error;
use warp::{
    ws::{Message, WebSocket, Ws},
    Reply,
};

use crate::SETTINGS;

const BROADCAST_CHANNEL_CAPACITY: usize = 256;

pub type MessageBus = Arc<DashMap<Vec<u8>, broadcast::Sender<Vec<u8>>>>;

pub fn upgrade_ws(addr: Address, ws: Ws, msg_bus: MessageBus) -> impl Reply {
    // Convert address
    let pubkey_hash = addr.into_body();

    // Upgrade socket
    ws.on_upgrade(move |socket| connect_ws(pubkey_hash, socket, msg_bus))
}

#[derive(Debug, Error)]
enum WsError {
    #[error("websocket send failed: {0}")]
    SinkError(warp::Error),
    #[error("broadcast failure: {0}")]
    BusError(broadcast::RecvError),
}

pub async fn connect_ws(pubkey_hash: Vec<u8>, ws: WebSocket, msg_bus: MessageBus) {
    let rx = msg_bus
        .entry(pubkey_hash.clone())
        .or_insert(broadcast::channel(BROADCAST_CHANNEL_CAPACITY).0)
        .subscribe()
        .map_ok(Message::binary)
        .map_err(WsError::BusError);

    let (user_ws_tx, _) = ws.split();

    // Setup periodic ping
    let periodic_ping = interval(Duration::from_millis(SETTINGS.websocket.ping_interval))
        .map(move |_| Ok(Message::ping(vec![])));
    let merged = stream::select(rx, periodic_ping);

    if let Err(err) = merged
        .forward(user_ws_tx.sink_map_err(WsError::SinkError))
        .await
    {
        error!(message = "forwarding error", error = %err);
    }

    // TODO: Double check this is atomic
    msg_bus.remove_if(&pubkey_hash, |_, sender| sender.receiver_count() == 0);
}
