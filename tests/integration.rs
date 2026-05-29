use std::{
    process::{Child, Command, Stdio},
    thread::sleep,
    time::Duration,
};

use async_nats::jetstream;

struct NatsServer {
    child: Child,
    port: u16,
}

impl Drop for NatsServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_nats_server() -> Option<NatsServer> {
    let port = pick_port();
    let child = Command::new("nats-server")
        .args([
            "-p",
            &port.to_string(),
            "-js",
            "-sd",
            &std::env::temp_dir().to_string_lossy(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    sleep(Duration::from_millis(400));
    Some(NatsServer { child, port })
}

fn pick_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let port = listener.local_addr().expect("local_addr").port();
    drop(listener);
    port
}

#[tokio::test]
async fn list_streams_returns_seeded_stream() {
    let Some(server) = spawn_nats_server() else {
        eprintln!("skipping: nats-server binary not found on PATH");
        return;
    };
    let url = format!("nats://127.0.0.1:{}", server.port);

    let client = async_nats::connect(&url).await.expect("connect");
    let js = jetstream::new(client.clone());
    js.create_stream(async_nats::jetstream::stream::Config {
        name: "TEST".into(),
        subjects: vec!["test.>".into()],
        ..Default::default()
    })
    .await
    .expect("create stream");

    js.publish("test.one", "hello".into())
        .await
        .expect("publish")
        .await
        .expect("ack");

    let cfg = hyerix_mcp_test_config(&url);
    let conn = hyerix_mcp_connect(&cfg).await;
    let streams = hyerix_mcp_lib::nats::streams::list_streams(&conn, None)
        .await
        .expect("list streams");
    assert!(streams.iter().any(|s| s.name == "TEST"));
    let test = streams.iter().find(|s| s.name == "TEST").unwrap();
    assert_eq!(test.subjects, vec!["test.>".to_string()]);
    assert!(test.messages >= 1);
}

#[tokio::test]
async fn browse_messages_returns_payload() {
    let Some(server) = spawn_nats_server() else {
        eprintln!("skipping: nats-server binary not found on PATH");
        return;
    };
    let url = format!("nats://127.0.0.1:{}", server.port);
    let client = async_nats::connect(&url).await.expect("connect");
    let js = jetstream::new(client.clone());
    js.create_stream(async_nats::jetstream::stream::Config {
        name: "ORDERS".into(),
        subjects: vec!["orders.>".into()],
        ..Default::default()
    })
    .await
    .expect("create stream");
    for i in 0..3u32 {
        js.publish("orders.eu.created", format!("payload-{i}").into())
            .await
            .expect("publish")
            .await
            .expect("ack");
    }

    let cfg = hyerix_mcp_test_config(&url);
    let conn = hyerix_mcp_connect(&cfg).await;
    let browsed = hyerix_mcp_lib::nats::messages::browse_messages(&conn, "ORDERS", 10, None)
        .await
        .expect("browse");
    assert_eq!(browsed.stream_name, "ORDERS");
    assert_eq!(browsed.total_messages, 3);
    assert!(browsed.returned >= 1);
    assert!(browsed
        .messages
        .iter()
        .all(|m| m.payload.starts_with("payload-")));
}

fn hyerix_mcp_test_config(url: &str) -> hyerix_mcp_lib::config::Config {
    hyerix_mcp_lib::config::Config {
        nats_url: url.to_string(),
        creds: None,
        user: None,
        pass: None,
        token: None,
        nkey: None,
        allow_publish: false,
    }
}

async fn hyerix_mcp_connect(cfg: &hyerix_mcp_lib::config::Config) -> async_nats::Client {
    hyerix_mcp_lib::nats::connection::connect(cfg)
        .await
        .expect("connect")
}

#[tokio::test]
async fn server_capabilities_declare_tools() {
    use rmcp::ServerHandler;
    let cfg = hyerix_mcp_test_config("nats://127.0.0.1:1");
    let lazy = hyerix_mcp_lib::nats::connection::LazyClient::new(cfg);
    let server = hyerix_mcp_lib::tools::HyerixMcp::new(lazy, false);
    let info = server.get_info();
    assert!(info.capabilities.tools.is_some());
}

#[test]
fn stdio_binary_declares_all_fourteen_tools() {
    use std::io::{BufRead, BufReader, Write};
    use std::process::{Command, Stdio};

    let bin = env!("CARGO_BIN_EXE_hyerix-mcp");
    let mut child = Command::new(bin)
        .args(["--nats-url", "nats://127.0.0.1:1"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn hyerix-mcp");

    let stdin = child.stdin.as_mut().expect("stdin");
    let initialize = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"smoke","version":"0"}}}"#;
    let initialized = r#"{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}"#;
    let list = r#"{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}"#;
    writeln!(stdin, "{initialize}").unwrap();
    writeln!(stdin, "{initialized}").unwrap();
    writeln!(stdin, "{list}").unwrap();
    stdin.flush().unwrap();

    let stdout = child.stdout.take().expect("stdout");
    let mut reader = BufReader::new(stdout);
    let mut tools_payload: Option<serde_json::Value> = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    while std::time::Instant::now() < deadline {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let v: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if v.get("id").and_then(|x| x.as_i64()) == Some(2) {
            tools_payload = Some(v);
            break;
        }
    }
    let _ = child.kill();
    let _ = child.wait();

    let payload = tools_payload.expect("tools/list response");
    let tools = payload["result"]["tools"].as_array().expect("tools array");
    let names: std::collections::BTreeSet<String> = tools
        .iter()
        .filter_map(|t| t["name"].as_str().map(|s| s.to_string()))
        .collect();
    let expected: [&str; 14] = [
        "browse_messages",
        "get_cluster_health",
        "get_consumer",
        "get_stream",
        "kv_get",
        "kv_list_keys",
        "list_consumers",
        "list_streams",
        "obj_get",
        "obj_list",
        "obj_list_buckets",
        "publish_message",
        "request_reply",
        "subscribe",
    ];
    for name in expected {
        assert!(
            names.contains(name),
            "expected tool '{name}' to be declared. Got: {names:?}"
        );
    }
    assert_eq!(
        tools.len(),
        14,
        "expected exactly 14 tools, got {}: {names:?}",
        tools.len()
    );

    for t in tools {
        let schema = t.get("inputSchema").expect("inputSchema present");
        assert!(
            schema.get("type").is_some() || schema.get("properties").is_some(),
            "tool '{}' missing JSONSchema body: {schema}",
            t["name"]
        );
    }
}

#[tokio::test]
async fn subscribe_returns_after_max_messages() {
    let Some(server) = spawn_nats_server() else {
        eprintln!("skipping: nats-server binary not found on PATH");
        return;
    };
    let url = format!("nats://127.0.0.1:{}", server.port);
    let client = async_nats::connect(&url).await.expect("connect");

    let publisher = client.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        for i in 0..3u32 {
            publisher
                .publish("evt.sub", format!("msg-{i}").into())
                .await
                .ok();
        }
        publisher.flush().await.ok();
    });

    let cfg = hyerix_mcp_test_config(&url);
    let conn = hyerix_mcp_connect(&cfg).await;
    let result = hyerix_mcp_lib::nats::messages::subscribe_collect(&conn, "evt.sub", 5, 3, None)
        .await
        .expect("subscribe");
    assert_eq!(result.received, 3);
    assert!(!result.timed_out);
    assert!(result.messages.iter().all(|m| m.subject == "evt.sub"));
}

#[tokio::test]
async fn subscribe_clamps_oversized_limits() {
    let Some(server) = spawn_nats_server() else {
        eprintln!("skipping: nats-server binary not found on PATH");
        return;
    };
    let url = format!("nats://127.0.0.1:{}", server.port);

    let cfg = hyerix_mcp_test_config(&url);
    let conn = hyerix_mcp_connect(&cfg).await;
    let result =
        hyerix_mcp_lib::nats::messages::subscribe_collect(&conn, "no.traffic.here", 1, 10000, None)
            .await
            .expect("subscribe");
    assert_eq!(result.max_messages, 100);
    assert!(result.timed_out);
    assert_eq!(result.received, 0);
}

#[tokio::test]
async fn request_reply_returns_reply() {
    let Some(server) = spawn_nats_server() else {
        eprintln!("skipping: nats-server binary not found on PATH");
        return;
    };
    let url = format!("nats://127.0.0.1:{}", server.port);
    let client = async_nats::connect(&url).await.expect("connect");

    let responder = client.clone();
    tokio::spawn(async move {
        let mut sub = responder
            .subscribe("svc.echo".to_string())
            .await
            .expect("sub");
        use futures::StreamExt;
        while let Some(msg) = sub.next().await {
            if let Some(reply) = msg.reply {
                responder.publish(reply, msg.payload.clone()).await.ok();
                responder.flush().await.ok();
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    let cfg = hyerix_mcp_test_config(&url);
    let conn = hyerix_mcp_connect(&cfg).await;
    let reply =
        hyerix_mcp_lib::nats::messages::request_reply(&conn, "svc.echo", b"ping".to_vec(), None, 3)
            .await
            .expect("request");
    assert_eq!(reply.payload, "ping");
}

#[tokio::test]
async fn request_reply_times_out_on_no_responder() {
    let Some(server) = spawn_nats_server() else {
        eprintln!("skipping: nats-server binary not found on PATH");
        return;
    };
    let url = format!("nats://127.0.0.1:{}", server.port);
    let cfg = hyerix_mcp_test_config(&url);
    let conn = hyerix_mcp_connect(&cfg).await;
    let err = hyerix_mcp_lib::nats::messages::request_reply(
        &conn,
        "nobody.here",
        b"hi".to_vec(),
        None,
        1,
    )
    .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn kv_list_keys_returns_keys() {
    let Some(server) = spawn_nats_server() else {
        eprintln!("skipping: nats-server binary not found on PATH");
        return;
    };
    let url = format!("nats://127.0.0.1:{}", server.port);
    let client = async_nats::connect(&url).await.expect("connect");
    let js = jetstream::new(client.clone());
    let store = js
        .create_key_value(async_nats::jetstream::kv::Config {
            bucket: "CFG".into(),
            ..Default::default()
        })
        .await
        .expect("create kv");
    store.put("alpha", "1".into()).await.expect("put");
    store.put("beta", "2".into()).await.expect("put");
    store.put("gamma", "3".into()).await.expect("put");

    let cfg = hyerix_mcp_test_config(&url);
    let conn = hyerix_mcp_connect(&cfg).await;
    let listing = hyerix_mcp_lib::nats::kv::list_keys(&conn, "CFG", None, 100)
        .await
        .expect("list_keys");
    assert_eq!(listing.bucket, "CFG");
    assert!(listing.returned >= 3);

    let filtered = hyerix_mcp_lib::nats::kv::list_keys(&conn, "CFG", Some("alph"), 100)
        .await
        .expect("filter");
    assert!(filtered.keys.iter().any(|k| k.key == "alpha"));
    assert!(filtered.keys.iter().all(|k| k.key.contains("alph")));
}

#[tokio::test]
async fn kv_list_keys_clamps_limit() {
    let Some(server) = spawn_nats_server() else {
        eprintln!("skipping: nats-server binary not found on PATH");
        return;
    };
    let url = format!("nats://127.0.0.1:{}", server.port);
    let client = async_nats::connect(&url).await.expect("connect");
    let js = jetstream::new(client.clone());
    js.create_key_value(async_nats::jetstream::kv::Config {
        bucket: "CFG2".into(),
        ..Default::default()
    })
    .await
    .expect("create kv");

    let cfg = hyerix_mcp_test_config(&url);
    let conn = hyerix_mcp_connect(&cfg).await;
    let res = hyerix_mcp_lib::nats::kv::list_keys(&conn, "CFG2", None, 50_000)
        .await
        .expect("list_keys");
    assert_eq!(res.bucket, "CFG2");
}

#[tokio::test]
async fn obj_buckets_and_objects_roundtrip() {
    let Some(server) = spawn_nats_server() else {
        eprintln!("skipping: nats-server binary not found on PATH");
        return;
    };
    let url = format!("nats://127.0.0.1:{}", server.port);
    let client = async_nats::connect(&url).await.expect("connect");
    let js = jetstream::new(client.clone());
    let store = js
        .create_object_store(async_nats::jetstream::object_store::Config {
            bucket: "ASSETS".into(),
            description: Some("test bucket".into()),
            ..Default::default()
        })
        .await
        .expect("create object store");
    store
        .put("note.txt", &mut std::io::Cursor::new(b"hello-obj".to_vec()))
        .await
        .expect("put object");

    let cfg = hyerix_mcp_test_config(&url);
    let conn = hyerix_mcp_connect(&cfg).await;

    let buckets = hyerix_mcp_lib::nats::object_store::list_buckets(&conn)
        .await
        .expect("list buckets");
    assert!(buckets.iter().any(|b| b.bucket_name == "ASSETS"));

    let objects = hyerix_mcp_lib::nats::object_store::list_objects(&conn, "ASSETS", 100)
        .await
        .expect("list objects");
    assert!(objects.iter().any(|o| o.name == "note.txt"));

    let fetched = hyerix_mcp_lib::nats::object_store::get_object(&conn, "ASSETS", "note.txt")
        .await
        .expect("get object");
    assert_eq!(fetched.payload, "hello-obj");
    assert_eq!(fetched.payload_encoding, "utf8");
}

#[tokio::test]
async fn obj_get_rejects_oversized() {
    let Some(server) = spawn_nats_server() else {
        eprintln!("skipping: nats-server binary not found on PATH");
        return;
    };
    let url = format!("nats://127.0.0.1:{}", server.port);
    let client = async_nats::connect(&url).await.expect("connect");
    let js = jetstream::new(client.clone());
    let store = js
        .create_object_store(async_nats::jetstream::object_store::Config {
            bucket: "BIG".into(),
            ..Default::default()
        })
        .await
        .expect("create object store");

    let big = vec![0u8; (1024 * 1024) + 64];
    store
        .put("blob.bin", &mut std::io::Cursor::new(big))
        .await
        .expect("put big object");

    let cfg = hyerix_mcp_test_config(&url);
    let conn = hyerix_mcp_connect(&cfg).await;
    let err = hyerix_mcp_lib::nats::object_store::get_object(&conn, "BIG", "blob.bin").await;
    assert!(err.is_err(), "expected too-large error");
}
