use std::sync::Arc;

use bitcoincash_addr::Address;
use dashmap::DashMap;
use futures::prelude::*;
use tokio::{sync::broadcast, time::{interval, Duration}};
use warp::{
    ws::{Message, WebSocket, Ws},
    Reply,
};

use crate::SETTINGS;

const BROADCAST_CHANNEL_CAPACITY: usize = 256;

// pubkey hash:serialized timed message
pub type MessageBus = Arc<DashMap<Vec<u8>, broadcast::Sender<Vec<u8>>>>;

pub fn upgrade_ws(addr: Address, ws: Ws, msg_bus: MessageBus) -> impl Reply {
    // Convert address
    let pubkey_hash = addr.into_body();

    // Upgrade socket
    ws.on_upgrade(move |socket| connect_ws(pubkey_hash, socket, msg_bus))
}

#[derive(Debug)]
enum WsError {
    SinkError(warp::Error),
    BusError(broadcast::RecvError),
}

pub async fn connect_ws(pubkey_hash: Vec<u8>, ws: WebSocket, msg_bus: MessageBus) {
    let rx = msg_bus
        .entry(pubkey_hash.clone())
        .or_insert(broadcast::channel(BROADCAST_CHANNEL_CAPACITY).0)
        .subscribe()
        .map_ok(|res| Message::binary(res))
        .map_err(WsError::BusError);

    let (user_ws_tx, _) = ws.split();

    // Setup periodic ping
    let periodic_ping = interval(Duration::from_millis(SETTINGS.ping_interval)).map(move |_| {
        Ok(Message::ping(vec![]))
    });
    let merged = stream::select(rx, periodic_ping);

    if let Err(err) = merged
        .forward(user_ws_tx.sink_map_err(WsError::SinkError))
        .await
    {
        log::error!("{:#?}", err);
    }

    // TODO: Double check this is atomic
    msg_bus.remove_if(&pubkey_hash, |_, sender| sender.receiver_count() == 0);
}
