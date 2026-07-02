use axum::{
    extract::{DefaultBodyLimit, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};
use thiserror::Error;
use tokio::sync::RwLock;
use treepir_core::{
    inspire_packing_params_fast, InspirePirError, SeededClientQuery, ServerCrs, ServerResponse,
    TreePirLayout, TreePirLevelResponse, TreePirOwnedLevelRequest, TreePirOwnedPathRequest,
    TreePirPathResponse, TreePirServer as CoreTreePirServer, INSPIRE_ENTRY_SIZE,
};

pub const SERVER_DEPTH: usize = 45;

pub type TreePirServerCore = CoreTreePirServer<SERVER_DEPTH>;
pub type SharedState = Arc<RwLock<TreePirServerCore>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayoutDto {
    pub leaf_count: usize,
    pub root_hex: String,
    pub level_lens: Vec<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayoutQuery {
    pub crs_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParamsResponse {
    pub entry_size: usize,
    pub params: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterClientRequest {
    pub crs_bincode_base64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterClientResponse {
    pub crs_hash: String,
    pub registered: bool,
}

/// Server-visible PIR path request.
///
/// Privacy invariant: this type must not contain `leaf_index`,
/// `sibling_index`, or any selected database index. The client keeps those
/// values locally in `TreePirPathQuery`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathQueryRequest {
    pub crs_hash: String,
    pub layout: LayoutDto,
    pub levels: Vec<LevelQueryDto>,
}

/// Server-visible per-level PIR query.
///
/// `level` is public layout metadata. `query_json_base64` is the encrypted PIR
/// query. The selected index is not part of this DTO.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LevelQueryDto {
    pub level: usize,
    pub query_json_base64: String,
}

/// Server-visible PIR path response.
///
/// Privacy invariant: this type must not contain `leaf_index` or
/// `sibling_index`. The client already has that private state locally.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathQueryResponse {
    pub layout: LayoutDto,
    pub levels: Vec<LevelResponseDto>,
}

/// Server-visible per-level PIR response.
///
/// `level` is public layout metadata. `level_len` is intentionally omitted from
/// the wire response because it is derivable from `layout.level_lens`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LevelResponseDto {
    pub level: usize,
    pub response_json_base64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub ok: bool,
    pub service: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("{0}")]
    TreePir(#[from] InspirePirError),

    #[error("invalid layout: {0}")]
    InvalidLayout(String),

    #[error("invalid CRS hash")]
    InvalidCrsHash,

    #[error("invalid client CRS: {0}")]
    InvalidClientCrs(String),

    #[error("invalid path request: {0}")]
    InvalidPathRequest(String),

    #[error("invalid path response: {0}")]
    InvalidPathResponse(String),

    #[error("stale layout; refresh layout and rebuild PIR query")]
    StaleLayout,

    #[error("invalid base64: {0}")]
    Base64(String),

    #[error("invalid bincode: {0}")]
    Bincode(String),

    #[error("invalid json wire payload: {0}")]
    JsonWire(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::TreePir(InspirePirError::MissingClientRegistration { .. }) => {
                StatusCode::UNAUTHORIZED
            }
            Self::TreePir(error) if is_stale_treepir_error(error) => StatusCode::CONFLICT,
            Self::StaleLayout => StatusCode::CONFLICT,
            Self::TreePir(InspirePirError::InvalidIndex { .. }) => StatusCode::BAD_REQUEST,
            Self::TreePir(InspirePirError::InvalidLevel { .. }) => StatusCode::BAD_REQUEST,
            Self::TreePir(InspirePirError::InvalidRequest(_)) => StatusCode::BAD_REQUEST,
            Self::InvalidLayout(_) => StatusCode::BAD_REQUEST,
            Self::InvalidCrsHash => StatusCode::BAD_REQUEST,
            Self::InvalidClientCrs(_) => StatusCode::BAD_REQUEST,
            Self::InvalidPathRequest(_) => StatusCode::BAD_REQUEST,
            Self::InvalidPathResponse(_) => StatusCode::BAD_REQUEST,
            Self::Base64(_) => StatusCode::BAD_REQUEST,
            Self::Bincode(_) => StatusCode::BAD_REQUEST,
            Self::JsonWire(_) => StatusCode::BAD_REQUEST,
            Self::TreePir(_) => StatusCode::BAD_REQUEST,
        };

        let error = match &self {
            Self::TreePir(InspirePirError::MissingClientRegistration { .. }) => {
                "missing client registration".to_string()
            }
            Self::TreePir(error) if is_stale_treepir_error(error) => {
                "stale layout; refresh layout and rebuild PIR query".to_string()
            }
            _ => self.to_string(),
        };

        (status, Json(ErrorResponse { error })).into_response()
    }
}

pub fn app(state: SharedState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/pir-params", get(params))
        .route("/v1/layout", get(layout))
        .route("/v1/clients/register", post(register_client))
        .route("/v1/path", post(query_path))
        .layer(DefaultBodyLimit::max(128 * 1024 * 1024))
        .with_state(state)
}

pub async fn run(
    addr: SocketAddr,
    state: SharedState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app(state)).await?;
    Ok(())
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        ok: true,
        service: "stellar-mixer-treepir-server",
    })
}

async fn params() -> Result<Json<ParamsResponse>, ApiError> {
    let params = inspire_packing_params_fast();

    Ok(Json(ParamsResponse {
        entry_size: INSPIRE_ENTRY_SIZE,
        params: serde_json::json!({
            "ring_dim": params.ring_dim,
            "q": params.q,
            "crt_moduli": params.crt_moduli,
            "p": params.p,
            "sigma": params.sigma,
            "gadget_base": params.gadget_base,
            "gadget_len": params.gadget_len,
            "security_level": format!("{:?}", params.security_level),
        }),
    }))
}

async fn layout(
    State(state): State<SharedState>,
    Query(query): Query<LayoutQuery>,
) -> Result<Json<LayoutDto>, ApiError> {
    let crs_hash = query
        .crs_hash
        .as_deref()
        .ok_or_else(|| missing_registration_error(""))?;

    validate_crs_hash_format(crs_hash)?;

    let server = state.read().await;

    if !server.is_client_registered(crs_hash) {
        return Err(missing_registration_error(crs_hash));
    }

    // Layout is cheap/public-to-registered metadata. Do not prepare EncodedDB here.
    Ok(Json(LayoutDto::from_treepir(server.current_layout())))
}

async fn register_client(
    State(state): State<SharedState>,
    Json(request): Json<RegisterClientRequest>,
) -> Result<Json<RegisterClientResponse>, ApiError> {
    let crs: ServerCrs = decode_bincode_base64(&request.crs_bincode_base64)?;
    validate_registered_crs_params(&crs)?;

    let mut server = state.write().await;
    let crs_hash = server.register_client_crs_raw(crs)?;

    Ok(Json(RegisterClientResponse {
        crs_hash,
        registered: true,
    }))
}

async fn query_path(
    State(state): State<SharedState>,
    Json(request): Json<PathQueryRequest>,
) -> Result<Json<PathQueryResponse>, ApiError> {
    validate_crs_hash_format(&request.crs_hash)?;

    let layout = request.layout.to_treepir()?;

    {
        let server = state.read().await;

        if !server.is_client_registered(&request.crs_hash) {
            return Err(missing_registration_error(&request.crs_hash));
        }

        // Stale-layout rejection happens before decoding large PIR payloads and
        // before lazy EncodedDB preparation.
        if server.current_layout() != &layout {
            return Err(ApiError::StaleLayout);
        }
    }

    // Shape validation is also intentionally before payload decoding.
    validate_level_query_shape(&request.levels, &layout)?;

    let mut levels = Vec::with_capacity(request.levels.len());

    for level in request.levels {
        let query: SeededClientQuery = decode_json_base64(&level.query_json_base64)?;
        levels.push(TreePirOwnedLevelRequest::new(level.level, query));
    }

    let owned_request = TreePirOwnedPathRequest::new(request.crs_hash, layout, levels);

    let mut server = state.write().await;
    let response = server.respond_owned_path(&owned_request)?;

    Ok(Json(PathQueryResponse::from_treepir(&response)?))
}

impl LayoutDto {
    pub fn from_treepir(layout: &TreePirLayout<SERVER_DEPTH>) -> Self {
        Self {
            leaf_count: layout.leaf_count,
            root_hex: hex_encode(&layout.root),
            level_lens: layout.level_lens.to_vec(),
        }
    }

    pub fn to_treepir(&self) -> Result<TreePirLayout<SERVER_DEPTH>, ApiError> {
        if self.level_lens.len() != SERVER_DEPTH {
            return Err(ApiError::InvalidLayout(format!(
                "expected {} level lengths, got {}",
                SERVER_DEPTH,
                self.level_lens.len()
            )));
        }

        let root = hex_decode_32(&self.root_hex)?;

        let mut level_lens = [0usize; SERVER_DEPTH];
        level_lens.copy_from_slice(&self.level_lens);

        Ok(TreePirLayout {
            leaf_count: self.leaf_count,
            root,
            level_lens,
        })
    }
}

impl PathQueryResponse {
    pub fn from_treepir(response: &TreePirPathResponse<SERVER_DEPTH>) -> Result<Self, ApiError> {
        debug_assert_core_response_shape(response);

        let mut levels = Vec::with_capacity(response.levels().len());

        for level in response.levels() {
            levels.push(LevelResponseDto {
                level: level.level(),
                response_json_base64: encode_json_base64(level.response())?,
            });
        }

        Ok(Self {
            layout: LayoutDto::from_treepir(response.layout()),
            levels,
        })
    }

    pub fn to_treepir(self) -> Result<TreePirPathResponse<SERVER_DEPTH>, ApiError> {
        let layout = self.layout.to_treepir()?;

        if self.levels.len() != SERVER_DEPTH {
            return Err(ApiError::InvalidPathResponse(format!(
                "expected {} level PIR responses, got {}",
                SERVER_DEPTH,
                self.levels.len()
            )));
        }

        let mut levels = Vec::with_capacity(self.levels.len());

        for (expected_level, level) in self.levels.into_iter().enumerate() {
            if level.level != expected_level {
                return Err(ApiError::InvalidPathResponse(format!(
                    "path response levels must be canonical 0..{}; at position {expected_level}, got level {}",
                    SERVER_DEPTH.saturating_sub(1),
                    level.level
                )));
            }

            let response: ServerResponse = decode_json_base64(&level.response_json_base64)?;
            let level_len = layout.level_lens[level.level];

            levels.push(TreePirLevelResponse::new(level.level, level_len, response));
        }

        Ok(TreePirPathResponse::new(layout, levels))
    }
}

pub fn encode_bincode_base64<T: serde::Serialize>(value: &T) -> Result<String, ApiError> {
    let bytes = bincode::serialize(value).map_err(|error| ApiError::Bincode(error.to_string()))?;
    Ok(BASE64.encode(bytes))
}

pub fn decode_bincode_base64<T: serde::de::DeserializeOwned>(value: &str) -> Result<T, ApiError> {
    let bytes = BASE64
        .decode(value.as_bytes())
        .map_err(|error| ApiError::Base64(error.to_string()))?;

    bincode::deserialize(&bytes).map_err(|error| ApiError::Bincode(error.to_string()))
}

pub fn encode_json_base64<T: serde::Serialize>(value: &T) -> Result<String, ApiError> {
    let bytes = serde_json::to_vec(value).map_err(|error| ApiError::JsonWire(error.to_string()))?;
    Ok(BASE64.encode(bytes))
}

pub fn decode_json_base64<T: serde::de::DeserializeOwned>(value: &str) -> Result<T, ApiError> {
    let bytes = BASE64
        .decode(value.as_bytes())
        .map_err(|error| ApiError::Base64(error.to_string()))?;

    serde_json::from_slice(&bytes).map_err(|error| ApiError::JsonWire(error.to_string()))
}

fn validate_registered_crs_params(crs: &ServerCrs) -> Result<(), ApiError> {
    let expected = inspire_packing_params_fast();
    let got = &crs.params;

    if got.ring_dim != expected.ring_dim {
        return Err(ApiError::InvalidClientCrs(format!(
            "ring_dim mismatch: expected {}, got {}",
            expected.ring_dim, got.ring_dim
        )));
    }

    if got.q != expected.q {
        return Err(ApiError::InvalidClientCrs("q mismatch".to_string()));
    }

    if got.crt_moduli != expected.crt_moduli {
        return Err(ApiError::InvalidClientCrs(
            "crt_moduli mismatch".to_string(),
        ));
    }

    if got.p != expected.p {
        return Err(ApiError::InvalidClientCrs("p mismatch".to_string()));
    }

    if got.sigma.to_bits() != expected.sigma.to_bits() {
        return Err(ApiError::InvalidClientCrs(format!(
            "sigma mismatch: expected {}, got {}",
            expected.sigma, got.sigma
        )));
    }

    if got.gadget_base != expected.gadget_base {
        return Err(ApiError::InvalidClientCrs(
            "gadget_base mismatch".to_string(),
        ));
    }

    if got.gadget_len != expected.gadget_len {
        return Err(ApiError::InvalidClientCrs(
            "gadget_len mismatch".to_string(),
        ));
    }

    if format!("{:?}", got.security_level) != format!("{:?}", expected.security_level) {
        return Err(ApiError::InvalidClientCrs(
            "security_level mismatch".to_string(),
        ));
    }

    Ok(())
}

fn validate_crs_hash_format(crs_hash: &str) -> Result<(), ApiError> {
    if crs_hash.len() != 64 || !crs_hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ApiError::InvalidCrsHash);
    }

    Ok(())
}

fn validate_level_query_shape(
    levels: &[LevelQueryDto],
    layout: &TreePirLayout<SERVER_DEPTH>,
) -> Result<(), ApiError> {
    if levels.len() != SERVER_DEPTH {
        return Err(ApiError::InvalidPathRequest(format!(
            "expected {} level PIR queries, got {}",
            SERVER_DEPTH,
            levels.len()
        )));
    }

    for (expected_level, level) in levels.iter().enumerate() {
        if level.level != expected_level {
            return Err(ApiError::InvalidPathRequest(format!(
                "path request levels must be canonical 0..{}; at position {expected_level}, got level {}",
                SERVER_DEPTH.saturating_sub(1),
                level.level
            )));
        }

        if layout.level_lens[expected_level] == 0 {
            return Err(ApiError::InvalidLayout(format!(
                "empty level database at level {expected_level}"
            )));
        }
    }

    Ok(())
}

#[cfg(debug_assertions)]
fn debug_assert_core_response_shape(response: &TreePirPathResponse<SERVER_DEPTH>) {
    debug_assert_eq!(
        response.levels().len(),
        SERVER_DEPTH,
        "core returned non-canonical response level count"
    );

    for (expected_level, level) in response.levels().iter().enumerate() {
        debug_assert_eq!(
            level.level(),
            expected_level,
            "core returned non-canonical response level order"
        );

        let expected_level_len = response.layout().level_lens[expected_level];

        debug_assert_eq!(
            level.level_len(),
            expected_level_len,
            "core returned mismatched response level_len"
        );
    }
}

#[cfg(not(debug_assertions))]
fn debug_assert_core_response_shape(_response: &TreePirPathResponse<SERVER_DEPTH>) {}

fn is_stale_treepir_error(error: &InspirePirError) -> bool {
    matches!(error, InspirePirError::Inspire(message) if message.contains("stale layout"))
}

fn missing_registration_error(crs_hash: &str) -> ApiError {
    InspirePirError::MissingClientRegistration {
        crs_hash: crs_hash.to_string(),
    }
    .into()
}

fn hex_encode(bytes: &[u8; 32]) -> String {
    let mut out = String::with_capacity(64);

    for byte in bytes {
        use std::fmt::Write;
        write!(&mut out, "{byte:02x}").expect("write to string");
    }

    out
}

fn hex_decode_32(value: &str) -> Result<[u8; 32], ApiError> {
    if value.len() != 64 {
        return Err(ApiError::InvalidLayout(format!(
            "expected 64 hex chars for root, got {}",
            value.len()
        )));
    }

    let mut out = [0u8; 32];

    for i in 0..32 {
        let part = &value[i * 2..i * 2 + 2];
        out[i] = u8::from_str_radix(part, 16)
            .map_err(|error| ApiError::InvalidLayout(error.to_string()))?;
    }

    Ok(out)
}
