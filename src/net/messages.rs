use std::{
    convert::TryFrom,
    time::{SystemTime, UNIX_EPOCH},
};

use bitcoincash_addr::Address;
use bytes::Bytes;
use cashweb::{
    bitcoin_client::{BitcoinClient, HttpClient, HttpError, NodeError},
    relay::{stamp::StampError, *},
};
use futures::future;
use hex::FromHexError;
use prost::Message as _;
use ring::digest::{digest, SHA256};
use ripemd160::{Digest, Ripemd160};
use rocksdb::Error as RocksError;
use serde::Deserialize;
use thiserror::Error;
use tracing::warn;
use warp::{http::Response, hyper::Body, reject::Reject};

use super::{ws::MessageBus, IntoResponse};
use crate::{
    db::{self, Database},
    SETTINGS,
};

#[derive(Debug, Deserialize)]
pub struct Query {
    start_digest: Option<String>,
    end_digest: Option<String>,
    start_time: Option<u64>,
    end_time: Option<u64>,
    digest: Option<String>,
}

#[derive(Debug, Error)]
pub enum GetMessageError {
    #[error("failed to read from database: {0}")]
    DB(RocksError),
    #[error("failed to decode digest: {0}")]
    DigestDecode(FromHexError),
    #[error("destination malformed")]
    DestinationMalformed,
    #[error("message not found")]
    NotFound,
    #[error("both start time and digest given")]
    StartBothGiven,
    #[error("failed to decode start digest: {0}")]
    StartDigestMalformed(FromHexError),
    #[error("start digest not found")]
    StartDigestNotFound,
    #[error("no start found")]
    MissingStart,
    #[error("both end time and digest given")]
    EndBothGiven,
    #[error("failed to decode end digest: {0}")]
    EndDigestMalformed(FromHexError),
    #[error("end digest not found")]
    EndDigestNotFound,
}

impl From<RocksError> for GetMessageError {
    fn from(err: RocksError) -> Self {
        Self::DB(err)
    }
}

impl Reject for GetMessageError {}

impl IntoResponse for GetMessageError {
    fn to_status(&self) -> u16 {
        match self {
            Self::DB(_) => 500,
            Self::NotFound => 404,
            _ => 400,
        }
    }
}

fn get_unix_now() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_millis(),
    )
    .expect("we're in the distant future")
}

fn construct_prefixes(
    addr_payload: &[u8],
    query: Query,
    database: &Database,
) -> Result<(Vec<u8>, Option<Vec<u8>>), GetMessageError> {
    // Get start prefix
    let start_prefix = match (query.start_time, query.start_digest) {
        (Some(start_time), None) => db::msg_prefix(addr_payload, start_time),
        (None, Some(start_digest_hex)) => {
            let start_digest =
                hex::decode(start_digest_hex).map_err(GetMessageError::StartDigestMalformed)?;
            database
                .get_msg_key_by_digest(addr_payload, &start_digest)?
                .ok_or(GetMessageError::StartDigestNotFound)?
        }
        (Some(_), Some(_)) => return Err(GetMessageError::StartBothGiven),
        _ => return Err(GetMessageError::MissingStart),
    };

    // Get end prefix
    let end_prefix = match (query.end_time, query.end_digest) {
        (Some(end_time), None) => Some(db::msg_prefix(addr_payload, end_time)),
        (None, Some(end_digest_hex)) => {
            let start_digest =
                hex::decode(end_digest_hex).map_err(GetMessageError::EndDigestMalformed)?;
            let msg_key = database
                .get_msg_key_by_digest(addr_payload, &start_digest)?
                .ok_or(GetMessageError::EndDigestNotFound)?;
            Some(msg_key)
        }
        (Some(_), Some(_)) => return Err(GetMessageError::EndBothGiven),
        _ => None,
    };

    Ok((start_prefix, end_prefix))
}

pub async fn get_payloads(
    addr: Address,
    query: Query,
    database: Database,
) -> Result<Response<Body>, GetMessageError> {
    // Extract address payload
    let address_payload = addr.as_body();

    // If digest query then get single payload
    if let Some(digest) = query.digest {
        let raw_digest = hex::decode(digest).map_err(GetMessageError::DigestDecode)?;
        let raw_message = database
            .get_message_by_digest(&address_payload, &raw_digest[..])?
            .ok_or(GetMessageError::NotFound)?;
        let message = Message::decode(&raw_message[..]).unwrap(); // This is safe
        return Ok(Response::builder()
            .body(Body::from(message.payload))
            .unwrap());
    }

    let (start_prefix, end_prefix) = construct_prefixes(&address_payload, query, &database)?;
    let message_page =
        database.get_messages_range(&start_prefix, end_prefix.as_ref().map(|v| &v[..]))?;
    let payload_page = message_page.into_payload_page();

    // Serialize messages
    let mut raw_payload_page = Vec::with_capacity(payload_page.encoded_len());
    payload_page.encode(&mut raw_payload_page).unwrap();

    // Respond
    Ok(Response::builder()
        .body(Body::from(raw_payload_page))
        .unwrap()) // TODO: Headers
}

pub async fn get_messages(
    addr: Address,
    query: Query,
    database: Database,
) -> Result<Response<Body>, GetMessageError> {
    // Extract address payload
    let address_payload = addr.as_body();

    // If digest query then get single message
    if let Some(digest) = query.digest {
        let raw_digest = hex::decode(digest).map_err(GetMessageError::DigestDecode)?;
        let message = database
            .get_message_by_digest(&address_payload, &raw_digest[..])?
            .ok_or(GetMessageError::NotFound)?;
        return Ok(Response::builder().body(Body::from(message)).unwrap());
    }

    let (start_prefix, end_prefix) = construct_prefixes(&address_payload, query, &database)?;
    let message_set =
        database.get_messages_range(&start_prefix, end_prefix.as_ref().map(|v| &v[..]))?;

    // Serialize messages
    let mut raw_message_page = Vec::with_capacity(message_set.encoded_len());
    message_set.encode(&mut raw_message_page).unwrap();

    // Respond
    Ok(Response::builder()
        .body(Body::from(raw_message_page))
        .unwrap()) // TODO: Headers
}

pub async fn remove_messages(
    addr: Address,
    query: Query,
    database: Database,
) -> Result<Response<Body>, GetMessageError> {
    // Convert address
    let address_payload = addr.as_body();

    // If digest query then get single message
    if let Some(digest) = query.digest {
        let raw_digest = hex::decode(digest).map_err(GetMessageError::DigestDecode)?;
        database
            .remove_message_by_digest(&address_payload, &raw_digest[..])?
            .ok_or(GetMessageError::NotFound)?;
        return Ok(Response::builder().body(Body::empty()).unwrap());
    }

    let (start_prefix, end_prefix) = construct_prefixes(&address_payload, query, &database)?;
    database.remove_messages_range(&start_prefix, end_prefix.as_ref().map(|v| &v[..]))?;

    // Respond
    Ok(Response::builder().body(Body::empty()).unwrap()) // TODO: Headers
}

#[derive(Debug, Error)]
pub enum PutMessageError {
    #[error("failed to write to database: {0}")]
    DB(RocksError),
    #[error("destination malformed")]
    DestinationMalformed,
    #[error("failed to decode message: {0}")]
    MessagesDecode(prost::DecodeError),
    #[error("failed to parse message: {0}")]
    MessageParsing(ParseError),
    #[error("failed to decode payload: {0}")]
    PayloadDecode(prost::DecodeError),
    #[error("failed verify stamp: {0}")]
    StampVerify(StampError),
    #[error("failed to broadcast stamp: {0}")]
    StampBroadcast(HttpError),
}

impl From<RocksError> for PutMessageError {
    fn from(err: RocksError) -> Self {
        Self::DB(err)
    }
}

impl Reject for PutMessageError {}

impl IntoResponse for PutMessageError {
    fn to_status(&self) -> u16 {
        match self {
            Self::DB(_) => 500,
            Self::StampVerify(_) => 400,
            Self::StampBroadcast(err) => match err {
                NodeError::Rpc(_) => 400,
                _ => 500,
            },
            _ => 400,
        }
    }
}

pub async fn put_message(
    addr: Address,
    messages_raw: Bytes,
    database: Database,
    bitcoin_client: BitcoinClient<HttpClient>,
    msg_bus: MessageBus,
) -> Result<Response<Body>, PutMessageError> {
    // Time now
    let timestamp = get_unix_now();

    // Decode message
    let message_set =
        MessageSet::decode(&messages_raw[..]).map_err(PutMessageError::MessagesDecode)?;

    for mut message in message_set.messages.into_iter() {
        // Set received time
        message.received_time = timestamp as i64;

        // Get sender public key
        let source_pubkey = &message.source_public_key;
        let destination_pubkey = &message.destination_public_key;
        let source_pubkey_hash = Ripemd160::digest(digest(&SHA256, &source_pubkey).as_ref());
        let destination_pubkey_hash =
            Ripemd160::digest(digest(&SHA256, &destination_pubkey).as_ref());

        // Check if URL address is correct
        if addr.as_body() == &destination_pubkey_hash[..] {
            // TODO: What do we do here? Exit
        }

        // Serialze message which is stored in database
        let encoded_length = message.encoded_len();
        let mut raw_message = Vec::with_capacity(encoded_length);
        let mut ws_message = message.clone();

        message.encode(&mut raw_message).unwrap(); // This is safe

        // TODO: Parse does not enforce there is *ACTUALLY* a payload, only that there is a
        // payload digest. If the client is putting a message without a payload and only
        // a payload digest, there won't be any way to recover it and it'll create downstream
        // errors.
        //
        // This needs to be fixed.
        let parsed_message = message.parse().map_err(PutMessageError::MessageParsing)?;

        // If serialized payload too long then remove it
        let raw_message_ws =
            if ws_message.payload.len() > SETTINGS.websocket.truncation_length as usize {
                ws_message.payload = Vec::with_capacity(0);
                ws_message.payload_digest = parsed_message.payload_digest[..].to_vec();
                let mut pruned_raw_message = Vec::with_capacity(encoded_length);
                ws_message.encode(&mut pruned_raw_message).unwrap(); // This is safe
                pruned_raw_message
            } else {
                raw_message.clone()
            };

        let is_self_send = destination_pubkey_hash != source_pubkey_hash;

        // If sender is not self then check stamp
        if is_self_send {
            parsed_message
                .verify_stamp()
                .map_err(PutMessageError::StampVerify)?;
        }

        // Try broadcast stamp transactions
        let broadcast = parsed_message
            .stamp
            .stamp_outpoints
            .into_iter()
            .map(move |stamp_oupoint| stamp_oupoint.stamp_tx)
            .map(|stamp_tx| {
                let bitcoin_client_inner = bitcoin_client.clone();
                async move {
                    let stamp_tx = stamp_tx;
                    bitcoin_client_inner.send_tx(&stamp_tx).await
                }
            });

        future::try_join_all(broadcast)
            .await
            .map_err(PutMessageError::StampBroadcast)?;

        // Push to source key
        database.push_message(
            &source_pubkey_hash,
            timestamp,
            &raw_message[..],
            &parsed_message.payload_digest[..],
        )?;

        // Push to destination key
        database.push_message(
            &destination_pubkey_hash,
            timestamp,
            &raw_message[..],
            &parsed_message.payload_digest[..],
        )?;

        // Send to source
        if is_self_send {
            if let Some(sender) = msg_bus.get(&source_pubkey_hash.to_vec()) {
                // TODO: Why is this moving an immutable variable?
                if let Err(err) = sender.send(raw_message_ws.clone()) {
                    warn!(message = "failed to broadcast to self", error = ?err);
                    // TODO: Make prettier
                }
            }
        }

        // Send to destination
        if let Some(sender) = msg_bus.get(&destination_pubkey_hash.to_vec()) {
            if let Err(err) = sender.send(raw_message_ws) {
                warn!(message = "failed to broadcast to destination", error = ?err);
            }
        }
    }

    // Respond
    Ok(Response::builder().body(Body::empty()).unwrap())
}
