use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    response::Response,
    Router,
};
use serde::Serialize;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tower::ServiceExt;
use treepir_core::{
    crs_hash_hex, setup_inspire_client_material_default, InspireClientMaterial, LevelMerkleTree,
    NodeId, TreePirClient, TreePirServer as CoreTreePirServer,
};

use stellar_mixer_treepir_server::{
    app as server_app, encode_bincode_base64, encode_json_base64, LayoutDto, LevelQueryDto,
    ParamsResponse, PathQueryRequest, PathQueryResponse, RegisterClientRequest,
    RegisterClientResponse, SERVER_DEPTH,
};

struct Timings {
    test_name: &'static str,
    started: Instant,
    rows: Vec<(String, Duration)>,
    notes: Vec<String>,
}

impl Timings {
    fn new(test_name: &'static str) -> Self {
        Self {
            test_name,
            started: Instant::now(),
            rows: Vec::new(),
            notes: Vec::new(),
        }
    }

    fn measure<T>(&mut self, label: impl Into<String>, f: impl FnOnce() -> T) -> T {
        let label = label.into();
        let started = Instant::now();
        let value = f();
        self.rows.push((label, started.elapsed()));
        value
    }

    async fn measure_async<T, F>(&mut self, label: impl Into<String>, future: F) -> T
    where
        F: Future<Output = T>,
    {
        let label = label.into();
        let started = Instant::now();
        let value = future.await;
        self.rows.push((label, started.elapsed()));
        value
    }

    fn note(&mut self, note: impl Into<String>) {
        self.notes.push(note.into());
    }

    fn print(&self) {
        use std::fmt::Write as _;

        let wall = self.started.elapsed();
        let measured_sum = self
            .rows
            .iter()
            .fold(Duration::from_micros(0), |acc, (_, duration)| {
                acc + *duration
            });

        let mut out = String::new();

        let _ = writeln!(out);
        let _ = writeln!(out, "==== timings: {} ====", self.test_name);

        for (label, duration) in &self.rows {
            let _ = writeln!(out, "{:>12}  {}", fmt_duration(*duration), label);
        }

        let _ = writeln!(
            out,
            "{:>12}  {}",
            fmt_duration(wall),
            "total test wall time"
        );
        let _ = writeln!(
            out,
            "{:>12}  {}",
            fmt_duration(measured_sum),
            "sum measured spans, nested"
        );

        if !self.notes.is_empty() {
            let _ = writeln!(out);
            let _ = writeln!(out, "---- notes ----");

            for note in &self.notes {
                let _ = writeln!(out, "{note}");
            }
        }

        let _ = writeln!(out, "==============================");

        print!("{out}");
    }
}

impl Drop for Timings {
    fn drop(&mut self) {
        self.print();
    }
}

fn make_test_server() -> CoreTreePirServer<SERVER_DEPTH> {
    make_test_server_with_leaf_count(16)
}

fn make_test_server_with_leaf_count(leaf_count: usize) -> CoreTreePirServer<SERVER_DEPTH> {
    let mut tree = LevelMerkleTree::<SERVER_DEPTH>::new().unwrap();

    for i in 0..leaf_count {
        tree.append_data(format!("leaf-{i}").as_bytes()).unwrap();
    }

    CoreTreePirServer::new(tree)
}

fn new_client_material() -> (InspireClientMaterial, String) {
    let material = setup_inspire_client_material_default().unwrap();
    let crs_hash = crs_hash_hex(material.crs()).unwrap();
    (material, crs_hash)
}

#[tokio::test]
async fn full_registered_client_http_workflow_does_not_expose_private_indices() {
    let mut timings =
        Timings::new("full_registered_client_http_workflow_does_not_expose_private_indices");

    let server = timings.measure("make test server with 16 leaves", make_test_server);
    let original_tree = timings.measure("clone original tree", || server.tree().clone());

    let state = Arc::new(RwLock::new(server));
    let app = timings.measure("build axum app", || server_app(state.clone()));

    let params_response = timings
        .measure_async(
            "GET /v1/pir-params",
            app.clone().oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/pir-params")
                    .body(Body::empty())
                    .unwrap(),
            ),
        )
        .await
        .unwrap();

    assert_eq!(params_response.status(), StatusCode::OK);

    let params: ParamsResponse = timings
        .measure_async(
            "read/decode /v1/pir-params json",
            read_json(params_response),
        )
        .await;
    assert_eq!(params.entry_size, treepir_core::INSPIRE_ENTRY_SIZE);

    let (material, crs_hash) =
        timings.measure("generate client material + CRS hash", new_client_material);

    timings.note(format!("crs_hash={crs_hash}"));

    let layout_before_registration = timings
        .measure_async(
            "GET /v1/layout before registration",
            app.clone().oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/layout?crs_hash={crs_hash}"))
                    .body(Body::empty())
                    .unwrap(),
            ),
        )
        .await
        .unwrap();

    timings
        .measure_async(
            "assert /v1/layout before registration is 401",
            assert_status(
                layout_before_registration.status(),
                StatusCode::UNAUTHORIZED,
                layout_before_registration,
            ),
        )
        .await;

    let register_response =
        register_material_timed(&mut timings, app.clone(), &material, "register client").await;
    assert_eq!(register_response.status(), StatusCode::OK);

    let register_bytes = timings
        .measure_async(
            "read /v1/clients/register body",
            body_bytes(register_response),
        )
        .await;
    timings.note(format!("register response bytes={}", register_bytes.len()));

    let register_text = String::from_utf8_lossy(&register_bytes);

    assert!(
        !register_text.contains("registered_client_count"),
        "registration response leaked server-internal client count: {register_text}"
    );

    let registered: RegisterClientResponse = timings
        .measure("decode RegisterClientResponse", || {
            serde_json::from_slice(&register_bytes).unwrap()
        });

    assert!(registered.registered);
    assert_eq!(registered.crs_hash, crs_hash);

    {
        let server = timings
            .measure_async("state read after registration", state.read())
            .await;
        assert_eq!(server.registered_client_count(), 1);
        assert!(!server.has_prepared_setup());
        assert_eq!(server.setup_generation_count(), 0);
    }

    let layout = get_layout_timed(
        &mut timings,
        app.clone(),
        &crs_hash,
        "initial registered layout",
    )
    .await
    .to_treepir()
    .unwrap();

    timings.note(format!(
        "layout leaf_count={} depth={} level_lens={:?}",
        layout.leaf_count, SERVER_DEPTH, layout.level_lens
    ));

    let client = timings.measure("TreePirClient::for_layout_with_material", || {
        TreePirClient::for_layout_with_material(layout.clone(), material).unwrap()
    });

    assert_eq!(client.crs_hash(), crs_hash);
    assert_eq!(client.layout(), &layout);

    {
        let server = timings
            .measure_async("state read before first path", state.read())
            .await;
        assert!(!server.has_prepared_setup());
        assert_eq!(server.setup_generation_count(), 0);
    }

    for leaf_index in [0usize, 3, 6, 15] {
        let fresh_layout = get_layout_timed(
            &mut timings,
            app.clone(),
            &crs_hash,
            format!("leaf={leaf_index} fresh layout"),
        )
        .await
        .to_treepir()
        .unwrap();

        assert_eq!(&fresh_layout, client.layout());

        let path_query = timings.measure(format!("leaf={leaf_index} client query_path"), || {
            client.query_path(leaf_index).unwrap()
        });

        let request_body = timings.measure(
            format!("leaf={leaf_index} build PathQueryRequest + encode level queries"),
            || PathQueryRequest {
                crs_hash: path_query.crs_hash().to_string(),
                layout: LayoutDto::from_treepir(path_query.layout()),
                levels: path_query
                    .levels()
                    .iter()
                    .map(|level| LevelQueryDto {
                        level: level.level(),
                        query_json_base64: encode_json_base64(level.inspire_query().query())
                            .unwrap(),
                    })
                    .collect(),
            },
        );

        timings.note(format!(
            "leaf={leaf_index} path request levels={}",
            request_body.levels.len()
        ));

        let request_json = timings.measure(
            format!("leaf={leaf_index} serialize /v1/path request json"),
            || serde_json::to_vec(&request_body).unwrap(),
        );

        timings.note(format!(
            "leaf={leaf_index} /v1/path request_json bytes={}",
            request_json.len()
        ));

        let request_text = String::from_utf8_lossy(&request_json);

        timings.measure(
            format!("leaf={leaf_index} assert request has no private fields"),
            || assert_no_private_wire_fields(&request_text),
        );

        let response = timings
            .measure_async(
                format!("leaf={leaf_index} POST /v1/path server+router"),
                app.clone().oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/path")
                        .header("content-type", "application/json")
                        .body(Body::from(request_json))
                        .unwrap(),
                ),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let response_bytes = timings
            .measure_async(
                format!("leaf={leaf_index} read /v1/path response body"),
                body_bytes(response),
            )
            .await;

        timings.note(format!(
            "leaf={leaf_index} /v1/path response_json bytes={}",
            response_bytes.len()
        ));

        let response_text = String::from_utf8_lossy(&response_bytes);

        timings.measure(
            format!("leaf={leaf_index} assert response has no private fields"),
            || assert_no_private_wire_fields(&response_text),
        );

        let path_response_dto: PathQueryResponse = timings.measure(
            format!("leaf={leaf_index} decode PathQueryResponse json"),
            || serde_json::from_slice(&response_bytes).unwrap(),
        );

        let path_response = timings.measure(
            format!("leaf={leaf_index} PathQueryResponse::to_treepir"),
            || path_response_dto.to_treepir().unwrap(),
        );

        let extracted = timings.measure(format!("leaf={leaf_index} client extract_path"), || {
            client.extract_path(&path_query, &path_response).unwrap()
        });

        let leaf = timings.measure(
            format!("leaf={leaf_index} original_tree.hash_for_node"),
            || {
                original_tree
                    .hash_for_node(NodeId {
                        level: 0,
                        index: leaf_index,
                    })
                    .unwrap()
            },
        );

        timings.measure(format!("leaf={leaf_index} verify extracted path"), || {
            assert_eq!(extracted.leaf_index(), leaf_index);
            assert_eq!(extracted.root(), layout.root);
            assert!(extracted.verify(leaf));
        });
    }

    let server = timings
        .measure_async("state read after all paths", state.read())
        .await;
    assert_eq!(server.registered_client_count(), 1);
    assert!(server.has_prepared_setup());
    assert_eq!(server.setup_generation_count(), 1);
}

#[tokio::test]
async fn path_query_without_registered_crs_hash_is_rejected_before_query_payload_decode() {
    let mut timings = Timings::new(
        "path_query_without_registered_crs_hash_is_rejected_before_query_payload_decode",
    );

    let server = timings.measure("make test server", make_test_server);
    let layout = timings.measure("LayoutDto::from_treepir current_layout", || {
        LayoutDto::from_treepir(server.current_layout())
    });

    let state = Arc::new(RwLock::new(server));
    let app = timings.measure("build axum app", || server_app(state.clone()));

    let (material, crs_hash) =
        timings.measure("generate client material + CRS hash", new_client_material);
    let client = timings.measure("TreePirClient::for_layout_with_material", || {
        TreePirClient::for_layout_with_material(layout.to_treepir().unwrap(), material).unwrap()
    });

    let path_query = timings.measure("client query_path(0)", || client.query_path(0).unwrap());

    let request_body = timings.measure("build bad /v1/path request", || PathQueryRequest {
        crs_hash,
        layout: LayoutDto::from_treepir(path_query.layout()),
        levels: path_query
            .levels()
            .iter()
            .map(|level| LevelQueryDto {
                level: level.level(),
                query_json_base64: "definitely-not-base64".to_string(),
            })
            .collect(),
    });

    timings.note(format!(
        "bad unauthorized request levels={}",
        request_body.levels.len()
    ));

    let response = timings
        .measure_async(
            "POST /v1/path unauthorized CRS with bad payload",
            post_json(app, "/v1/path", &request_body),
        )
        .await;

    timings
        .measure_async(
            "assert /v1/path unauthorized CRS is 401",
            assert_status(response.status(), StatusCode::UNAUTHORIZED, response),
        )
        .await;

    let server = timings
        .measure_async("state read after unauthorized path", state.read())
        .await;
    assert!(!server.has_prepared_setup());
    assert_eq!(server.setup_generation_count(), 0);
}

#[tokio::test]
async fn stale_layout_is_rejected_before_query_payload_decode_and_before_setup() {
    let mut timings =
        Timings::new("stale_layout_is_rejected_before_query_payload_decode_and_before_setup");

    let server = timings.measure("make test server with 8 leaves", || {
        make_test_server_with_leaf_count(8)
    });

    let state = Arc::new(RwLock::new(server));
    let app = timings.measure("build axum app", || server_app(state.clone()));

    let (material, crs_hash) =
        timings.measure("generate client material + CRS hash", new_client_material);

    let register_response =
        register_material_timed(&mut timings, app.clone(), &material, "register client").await;
    assert_eq!(register_response.status(), StatusCode::OK);

    let old_layout = get_layout_timed(&mut timings, app.clone(), &crs_hash, "old layout")
        .await
        .to_treepir()
        .unwrap();

    let client = timings.measure("TreePirClient::for_layout_with_material", || {
        TreePirClient::for_layout_with_material(old_layout.clone(), material).unwrap()
    });

    let path_query = timings.measure("client query_path(0)", || client.query_path(0).unwrap());

    {
        let mut server = timings
            .measure_async("state write lock before append stale leaf", state.write())
            .await;

        timings.measure("server.append_data new blockchain leaf", || {
            server.append_data(b"new-blockchain-event-leaf").unwrap()
        });

        assert_ne!(server.current_layout(), &old_layout);
        assert!(!server.has_prepared_setup());
        assert_eq!(server.setup_generation_count(), 0);
    }

    let stale_request = timings.measure("build stale /v1/path request", || PathQueryRequest {
        crs_hash,
        layout: LayoutDto::from_treepir(path_query.layout()),
        levels: path_query
            .levels()
            .iter()
            .map(|level| LevelQueryDto {
                level: level.level(),
                query_json_base64: "definitely-not-base64".to_string(),
            })
            .collect(),
    });

    timings.note(format!(
        "stale request levels={}",
        stale_request.levels.len()
    ));

    let response = timings
        .measure_async(
            "POST /v1/path stale layout",
            post_json(app, "/v1/path", &stale_request),
        )
        .await;

    timings
        .measure_async(
            "assert stale layout is 409",
            assert_status(response.status(), StatusCode::CONFLICT, response),
        )
        .await;

    let server = timings
        .measure_async("state read after stale layout rejection", state.read())
        .await;
    assert!(!server.has_prepared_setup());
    assert_eq!(server.setup_generation_count(), 0);
}

#[tokio::test]
async fn non_canonical_level_shape_is_rejected_before_query_payload_decode_and_before_setup() {
    let mut timings = Timings::new(
        "non_canonical_level_shape_is_rejected_before_query_payload_decode_and_before_setup",
    );

    let server = timings.measure("make test server", make_test_server);

    let state = Arc::new(RwLock::new(server));
    let app = timings.measure("build axum app", || server_app(state.clone()));

    let (material, crs_hash) =
        timings.measure("generate client material + CRS hash", new_client_material);

    let register_response =
        register_material_timed(&mut timings, app.clone(), &material, "register client").await;
    assert_eq!(register_response.status(), StatusCode::OK);

    let layout = get_layout_timed(&mut timings, app.clone(), &crs_hash, "registered layout")
        .await
        .to_treepir()
        .unwrap();

    let request_body = timings.measure("build non-canonical /v1/path request", || {
        PathQueryRequest {
            crs_hash,
            layout: LayoutDto::from_treepir(&layout),
            levels: vec![
                LevelQueryDto {
                    level: 0,
                    query_json_base64: "definitely-not-base64".to_string(),
                },
                LevelQueryDto {
                    level: 0,
                    query_json_base64: "definitely-not-base64".to_string(),
                },
                LevelQueryDto {
                    level: 2,
                    query_json_base64: "definitely-not-base64".to_string(),
                },
                LevelQueryDto {
                    level: 3,
                    query_json_base64: "definitely-not-base64".to_string(),
                },
            ],
        }
    });

    timings.note(format!(
        "non-canonical request levels={}",
        request_body.levels.len()
    ));

    let response = timings
        .measure_async(
            "POST /v1/path non-canonical levels",
            post_json(app, "/v1/path", &request_body),
        )
        .await;

    timings
        .measure_async(
            "assert non-canonical levels is 400",
            assert_status(response.status(), StatusCode::BAD_REQUEST, response),
        )
        .await;

    let server = timings
        .measure_async("state read after non-canonical rejection", state.read())
        .await;
    assert!(!server.has_prepared_setup());
    assert_eq!(server.setup_generation_count(), 0);
}

#[tokio::test]
async fn registration_rejects_crs_with_unexpected_pir_params() {
    let mut timings = Timings::new("registration_rejects_crs_with_unexpected_pir_params");

    let server = timings.measure("make test server", make_test_server);

    let state = Arc::new(RwLock::new(server));
    let app = timings.measure("build axum app", || server_app(state.clone()));

    let (material, _crs_hash) =
        timings.measure("generate client material + CRS hash", new_client_material);

    let mut bad_crs = timings.measure("clone CRS", || material.crs().clone());
    timings.measure("mutate bad CRS params", || {
        bad_crs.params.ring_dim += 1;
    });

    let register_body = timings.measure("build bad register body encode CRS", || {
        RegisterClientRequest {
            crs_bincode_base64: encode_bincode_base64(&bad_crs).unwrap(),
        }
    });

    timings.note(format!(
        "bad register crs_bincode_base64 chars={}",
        register_body.crs_bincode_base64.len()
    ));

    let response = timings
        .measure_async(
            "POST /v1/clients/register bad CRS",
            post_json(app, "/v1/clients/register", &register_body),
        )
        .await;

    timings
        .measure_async(
            "assert bad CRS registration is 400",
            assert_status(response.status(), StatusCode::BAD_REQUEST, response),
        )
        .await;

    let server = timings
        .measure_async("state read after bad registration", state.read())
        .await;
    assert_eq!(server.registered_client_count(), 0);
    assert!(!server.has_prepared_setup());
    assert_eq!(server.setup_generation_count(), 0);
}

#[tokio::test]
async fn layout_requires_registered_crs_hash_and_does_not_prepare_encoded_db() {
    let mut timings =
        Timings::new("layout_requires_registered_crs_hash_and_does_not_prepare_encoded_db");

    let server = timings.measure("make test server", make_test_server);

    let state = Arc::new(RwLock::new(server));
    let app = timings.measure("build axum app", || server_app(state.clone()));

    let (material, crs_hash) =
        timings.measure("generate client material + CRS hash", new_client_material);

    let missing = timings
        .measure_async(
            "GET /v1/layout without crs_hash",
            app.clone().oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/layout")
                    .body(Body::empty())
                    .unwrap(),
            ),
        )
        .await
        .unwrap();

    timings
        .measure_async(
            "assert missing crs_hash layout is 401",
            assert_status(missing.status(), StatusCode::UNAUTHORIZED, missing),
        )
        .await;

    let unknown = timings
        .measure_async(
            "GET /v1/layout unknown crs_hash",
            app.clone().oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/layout?crs_hash={crs_hash}"))
                    .body(Body::empty())
                    .unwrap(),
            ),
        )
        .await
        .unwrap();

    timings
        .measure_async(
            "assert unknown crs_hash layout is 401",
            assert_status(unknown.status(), StatusCode::UNAUTHORIZED, unknown),
        )
        .await;

    let register_response =
        register_material_timed(&mut timings, app.clone(), &material, "register client").await;
    assert_eq!(register_response.status(), StatusCode::OK);

    let layout = get_layout_timed(&mut timings, app, &crs_hash, "registered layout").await;
    assert_eq!(layout.level_lens.len(), SERVER_DEPTH);

    timings.note(format!(
        "registered layout leaf_count={} level_lens={:?}",
        layout.leaf_count, layout.level_lens
    ));

    let server = timings
        .measure_async("state read after layout", state.read())
        .await;
    assert!(!server.has_prepared_setup());
    assert_eq!(server.setup_generation_count(), 0);
}

async fn register_material_timed(
    timings: &mut Timings,
    app: Router,
    material: &InspireClientMaterial,
    label: impl Into<String>,
) -> Response {
    let label = label.into();

    let register_body = timings.measure(format!("{label}: build register body encode CRS"), || {
        RegisterClientRequest {
            crs_bincode_base64: encode_bincode_base64(material.crs()).unwrap(),
        }
    });

    timings.note(format!(
        "{label}: crs_bincode_base64 chars={}",
        register_body.crs_bincode_base64.len()
    ));

    timings
        .measure_async(
            format!("{label}: POST /v1/clients/register"),
            post_json(app, "/v1/clients/register", &register_body),
        )
        .await
}

async fn get_layout_timed(
    timings: &mut Timings,
    app: Router,
    crs_hash: &str,
    label: impl Into<String>,
) -> LayoutDto {
    let label = label.into();

    let response = timings
        .measure_async(
            format!("{label}: GET /v1/layout"),
            app.oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/layout?crs_hash={crs_hash}"))
                    .body(Body::empty())
                    .unwrap(),
            ),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    timings
        .measure_async(
            format!("{label}: read/decode layout json"),
            read_json(response),
        )
        .await
}

async fn post_json<T: Serialize>(app: Router, uri: &str, body: &T) -> Response {
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(body).unwrap()))
            .unwrap(),
    )
    .await
    .unwrap()
}

async fn read_json<T: serde::de::DeserializeOwned>(response: Response) -> T {
    let bytes = body_bytes(response).await;
    serde_json::from_slice(&bytes).unwrap()
}

async fn body_bytes(response: Response) -> Vec<u8> {
    to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec()
}

async fn assert_status(actual: StatusCode, expected: StatusCode, response: Response) {
    if actual != expected {
        let bytes = body_bytes(response).await;
        panic!(
            "expected status {expected}, got {actual}; body={}",
            String::from_utf8_lossy(&bytes)
        );
    }
}

fn assert_no_private_wire_fields(json: &str) {
    for forbidden_key in ["\"leaf_index\"", "\"sibling_index\"", "\"level_len\""] {
        assert!(
            !json.contains(forbidden_key),
            "wire JSON leaked private/redundant field {forbidden_key}: {json}"
        );
    }
}

fn fmt_duration(duration: Duration) -> String {
    let micros = duration.as_micros();

    if micros < 1_000 {
        format!("{micros}µs")
    } else if micros < 1_000_000 {
        format!("{:.3}ms", micros as f64 / 1_000.0)
    } else {
        format!("{:.3}s", micros as f64 / 1_000_000.0)
    }
}
