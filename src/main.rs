#[macro_use]
extern crate clap;

pub mod db;
pub mod models;
pub mod net;
pub mod settings;

#[cfg(feature = "monitoring")]
pub mod monitoring;

use std::{env, sync::Arc, time::Duration};

use cashweb::{
    payments::{preprocess_payment, wallet::Wallet},
    token::schemes::hmac_bearer::HmacScheme,
};
use dashmap::DashMap;
use futures::prelude::*;
use lazy_static::lazy_static;
use tracing::info;
use tracing_subscriber::{fmt, EnvFilter};
use warp::{
    http::{header, Method},
    Filter,
};

#[cfg(feature = "monitoring")]
use prometheus::{Encoder, TextEncoder};

use cashweb::bitcoin_client::BitcoinClient;
use db::{Database, FEED_NAMESPACE, MESSAGE_NAMESPACE};
use net::{payments, protection};
use settings::Settings;

const DASHMAP_CAPACITY: usize = 2048;

const PROFILES_PATH: &str = "profiles";
const WS_PATH: &str = "ws";
const MESSAGES_PATH: &str = "messages";
const PAYLOADS_PATH: &str = "payloads";
const FEEDS_PATH: &str = "feeds";
pub const PAYMENTS_PATH: &str = "payments";

lazy_static! {
    // Static settings
    pub static ref SETTINGS: Settings = Settings::new().expect("couldn't load config");
}

#[tokio::main]
async fn main() {
    if env::var_os("RUST_LOG").is_none() {
        env::set_var("RUST_LOG", "info");
    }
    let subscriber = fmt::Subscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("no global subscriber has been set");

    info!(message = "starting", version = crate_version!());

    // Database state
    info!(message = "opening database", path = %SETTINGS.db_path);
    let db = Database::try_new(&SETTINGS.db_path).expect("failed to open database");
    let db_state = warp::any().map(move || db.clone());

    // Message broadcast state
    info!("constructing message bus");
    let message_bus = Arc::new(DashMap::with_capacity(DASHMAP_CAPACITY));
    let msg_bus_state = warp::any().map(move || message_bus.clone());

    // Feed broadcast state
    info!("constructing feed bus");
    let feed_bus = Arc::new(DashMap::with_capacity(DASHMAP_CAPACITY));
    let feed_bus_state = warp::any().map(move || feed_bus.clone());

    // Wallet state
    info!(
        message = "constructing wallet",
        timeout = SETTINGS.payments.timeout
    );
    let wallet = Wallet::new(Duration::from_millis(SETTINGS.payments.timeout));
    let wallet_state = warp::any().map(move || wallet.clone());

    // Bitcoin client state
    info!(message = "constructing bitcoin client", address = %SETTINGS.bitcoin_rpc.address);
    let bitcoin_client = BitcoinClient::new(
        SETTINGS.bitcoin_rpc.address.clone(),
        SETTINGS.bitcoin_rpc.username.clone(),
        SETTINGS.bitcoin_rpc.password.clone(),
    );
    let bitcoin_client_state = warp::any().map(move || bitcoin_client.clone());

    // Address string converter
    let addr_base = warp::path::param().and_then(|addr_str: String| async move {
        net::address_decode(&addr_str).map_err(warp::reject::custom)
    });

    // Token generator
    let key =
        hex::decode(&SETTINGS.payments.hmac_secret).expect("unable to interpret hmac key as hex");
    let token_scheme = Arc::new(HmacScheme::new(&key));
    let token_scheme_state = warp::any().map(move || token_scheme.clone());

    // Protection
    let addr_protected = addr_base
        .clone()
        .and(warp::header::headers_cloned())
        .and(token_scheme_state.clone())
        .and(wallet_state.clone())
        .and(bitcoin_client_state.clone())
        .and_then(move |addr, headers, token_scheme, wallet, bitcoin| {
            protection::pop_protection(addr, headers, token_scheme, wallet, bitcoin)
                .map_err(warp::reject::custom)
        });

    info!("constructing handlers");

    // Message handlers
    let messages_get = warp::path(MESSAGES_PATH)
        .and(addr_protected.clone())
        .and(warp::get())
        .and(warp::query())
        .and(db_state.clone())
        .and_then(move |addr, query, db| {
            net::get_messages(addr, query, db, MESSAGE_NAMESPACE).map_err(warp::reject::custom)
        });
    let messages_put = warp::path(MESSAGES_PATH)
        .and(addr_base)
        .and(warp::put())
        .and(warp::body::content_length_limit(
            SETTINGS.limits.message_size,
        ))
        .and(warp::body::bytes())
        .and(db_state.clone())
        .and(bitcoin_client_state.clone())
        .and(msg_bus_state.clone())
        .and_then(move |addr, body, db, bitcoin_client, msg_bus| {
            net::put_message(addr, body, db, bitcoin_client, msg_bus, MESSAGE_NAMESPACE)
                .map_err(warp::reject::custom)
        });
    let messages_delete = warp::path(MESSAGES_PATH)
        .and(addr_protected.clone())
        .and(warp::delete())
        .and(warp::query())
        .and(db_state.clone())
        .and_then(move |addr, query, db| {
            net::remove_messages(addr, query, db, MESSAGE_NAMESPACE).map_err(warp::reject::custom)
        });

    // Feed handlers
    let feeds_get = warp::path(FEEDS_PATH)
        .and(addr_base)
        .and(warp::get())
        .and(warp::query())
        .and(db_state.clone())
        .and_then(move |addr, query, db| {
            net::get_messages(addr, query, db, FEED_NAMESPACE).map_err(warp::reject::custom)
        });
    let feeds_put = warp::path(FEEDS_PATH)
        .and(addr_protected.clone())
        .and(warp::put())
        .and(warp::body::content_length_limit(
            SETTINGS.limits.message_size,
        ))
        .and(warp::body::bytes())
        .and(db_state.clone())
        .and(bitcoin_client_state.clone())
        .and(msg_bus_state.clone())
        .and_then(move |addr, body, db, bitcoin_client, msg_bus| {
            net::put_message(addr, body, db, bitcoin_client, msg_bus, FEED_NAMESPACE)
                .map_err(warp::reject::custom)
        });
    let feeds_delete = warp::path(FEEDS_PATH)
        .and(addr_protected.clone())
        .and(warp::delete())
        .and(warp::query())
        .and(db_state.clone())
        .and_then(move |addr, query, db| {
            net::remove_messages(addr, query, db, FEED_NAMESPACE).map_err(warp::reject::custom)
        });

    // Payload handlers
    let payloads_get = warp::path(PAYLOADS_PATH)
        .and(addr_protected.clone())
        .and(warp::get())
        .and(warp::query())
        .and(db_state.clone())
        .and_then(move |addr, query, db| {
            net::get_payloads(addr, query, db, MESSAGE_NAMESPACE).map_err(warp::reject::custom)
        });

    // Websocket handlers
    let websocket_messages = warp::path(WS_PATH)
        .and(warp::path(MESSAGES_PATH))
        .and(addr_protected.clone())
        .and(warp::ws())
        .and(msg_bus_state)
        .map(net::upgrade_ws);
    let websocket_feeds = warp::path(WS_PATH)
        .and(warp::path(FEEDS_PATH))
        .and(addr_base)
        .and(warp::ws())
        .and(feed_bus_state)
        .map(net::upgrade_ws);

    // Profile handlers
    let profile_get = warp::path(PROFILES_PATH)
        .and(addr_base)
        .and(warp::get())
        .and(db_state.clone())
        .and_then(move |addr, db| net::get_profile(addr, db).map_err(warp::reject::custom));
    let profile_put = warp::path(PROFILES_PATH)
        .and(addr_protected)
        .and(warp::put())
        .and(warp::body::content_length_limit(
            SETTINGS.limits.profile_size,
        ))
        .and(warp::body::bytes())
        .and(db_state)
        .and_then(move |addr, body, db| {
            net::put_profile(addr, body, db).map_err(warp::reject::custom)
        });

    // Payment handler
    let payments = warp::path(PAYMENTS_PATH)
        .and(warp::post())
        .and(warp::header::headers_cloned())
        .and(warp::body::content_length_limit(
            SETTINGS.limits.profile_size,
        ))
        .and(warp::body::bytes())
        .and_then(move |headers, body| {
            preprocess_payment(headers, body)
                .map_err(payments::PaymentError::Preprocess)
                .map_err(warp::reject::custom)
        })
        .and(wallet_state.clone())
        .and(bitcoin_client_state.clone())
        .and(token_scheme_state)
        .and_then(
            move |payment, wallet, bitcoin_client, token_state| async move {
                net::process_payment(payment, wallet, bitcoin_client, token_state)
                    .await
                    .map_err(warp::reject::custom)
            },
        );

    // Root handler
    let root = warp::path::end()
        .and(warp::get())
        .and(warp::fs::file("./static/index.html"));

    // CORs
    let cors = warp::cors()
        .allow_any_origin()
        .allow_methods(vec![Method::GET, Method::PUT, Method::POST, Method::DELETE])
        .allow_headers(vec![header::AUTHORIZATION, header::CONTENT_TYPE])
        .expose_headers(vec![
            header::AUTHORIZATION,
            header::ACCEPT,
            header::LOCATION,
        ])
        .build();

    // If monitoring is enabled
    #[cfg(feature = "monitoring")]
    {
        info!(monitoring = true);

        // Init Prometheus server
        let prometheus_server = warp::path("metrics").map(monitoring::export);
        let prometheus_task = warp::serve(prometheus_server).run(SETTINGS.bind_prom);

        // Init REST API
        let rest_api = root
            .or(payments)
            .or(websocket)
            .or(messages_get)
            .or(messages_delete)
            .or(messages_put)
            .or(feeds_get)
            .or(feeds_delete)
            .or(feeds_put)
            .or(payloads_get)
            .or(profile_get)
            .or(profile_put)
            .recover(net::handle_rejection)
            .with(cors)
            .with(warp::trace::request())
            .with(warp::log::custom(monitoring::measure));
        let rest_api_task = warp::serve(rest_api).run(SETTINGS.bind);

        // Spawn servers
        tokio::spawn(prometheus_task);
        tokio::spawn(rest_api_task).await.unwrap(); // Unrecoverable
    }

    // If monitoring is disabled
    #[cfg(not(feature = "monitoring"))]
    {
        info!(monitoring = false);

        // Init REST API
        let rest_api = root
            .or(payments)
            .or(websocket_messages)
            .or(websocket_feeds)
            .or(messages_get)
            .or(messages_delete)
            .or(feeds_get)
            .or(feeds_delete)
            .or(feeds_put)
            .or(messages_put)
            .or(payloads_get)
            .or(profile_get)
            .or(profile_put)
            .recover(net::handle_rejection)
            .with(cors)
            .with(warp::trace::request());
        let rest_api_task = warp::serve(rest_api).run(SETTINGS.bind);
        tokio::spawn(rest_api_task).await.unwrap(); // Unrecoverable
    }
}
