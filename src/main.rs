mod config;
mod indexer;
mod state_store;

use crate::config::ServerConfig;
use crate::indexer::StellarMixerIndexer;
use crate::state_store::PersistentTreeStore;

use anyhow::Result;
use axum::routing::get;
use axum::Json;
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};
use treepir_core::TreePirServer as CoreTreePirServer;
use stellar_mixer_treepir_server::{app as treepir_api, SERVER_DEPTH};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .or_else(|_| std::env::var("TREEPIR_LOG"))
                .unwrap_or_else(|_| "stellar_mixer_treepir_server=info,info".to_string()),
        )
        .init();

    let config = ServerConfig::from_env()?;

    info!(?config, "starting stellar-mixer-treepir-server");

    let store = PersistentTreeStore::load_or_create(
        config.db_path.clone(),
        config.mixer_contract_id.clone(),
        config.start_ledger,
    )?;

    let tree = store.build_tree::<SERVER_DEPTH>()?;
    let state = Arc::new(RwLock::new(CoreTreePirServer::<SERVER_DEPTH>::new(tree)));

    let mut indexer =
        StellarMixerIndexer::<SERVER_DEPTH>::new(config.clone(), store, state.clone());

    indexer.catch_up_once().await?;

    let indexer_task = tokio::spawn(async move {
        if let Err(error) = indexer.run_forever().await {
            error!(%error, "stellar mixer indexer stopped");
            std::process::exit(1);
        }
    });

    let ready_state = state.clone();

    let app = treepir_api(state).route(
        "/ready",
        get(move || {
            let ready_state = ready_state.clone();
            async move { ready(ready_state).await }
        }),
    );

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    info!(addr = %config.bind_addr, "stellar-mixer-treepir-server is listening");

    let server = axum::serve(listener, app).with_graceful_shutdown(shutdown_signal());

    tokio::select! {
        result = server => {
            result?;
        }
        result = indexer_task => {
            result?;
        }
    }

    Ok(())
}
#[derive(Debug, Serialize)]
struct ReadyResponse {
    ready: bool,
    depth: usize,
    leaf_count: usize,
    root_hex: String,
}

async fn ready(state: Arc<RwLock<CoreTreePirServer<SERVER_DEPTH>>>) -> Json<ReadyResponse> {
    let server = state.read().await;

    Json(ReadyResponse {
        ready: true,
        depth: SERVER_DEPTH,
        leaf_count: server.leaf_count(),
        root_hex: hex::encode(server.root()),
    })
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
