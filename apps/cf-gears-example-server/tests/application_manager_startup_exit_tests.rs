#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Process-exit smoke test for a failed `application-manager` start (DESIGN §3.2 "Fault
//! handling", §4.8 "Startup", §4.12 platform dependency 5).
//!
//! The host-level part (the start phase returns `RegistryError::Start` and the run
//! returns it) is tested in the gear crate, `infra/startup_host_tests.rs`. This test
//! closes the loop at the process boundary: a failed `start` must end the server
//! process with a non-zero exit status. If it does not, platform dependency 5 must be
//! escalated to the ToolKit runtime owners (the rollout still fails because the replica
//! never becomes ready, but a restart policy that restarts only after a failure exit
//! depends on this).
//!
//! The example server cannot run the gear yet: the `application-manager` example-server
//! feature, its entry in `registered_gears.rs` and its YAML wiring are planned for P9.
//! Run it there with
//! `cargo test -p cf-gears-example-server --features application-manager -- --ignored`.
//!
//! Setup: the gear is enabled with a valid configuration and the process has NO audit
//! plugin registered (the P9 test plugin crate is not enabled), so `start` fails with
//! the `no_audit_plugin` reason (DESIGN §4.6).

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

/// Kills the child on drop, so a failed assertion or a hung server never leaks a process.
struct KillOnDrop(std::process::Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

// The gear config is valid. No database block: the registry arrives with P4, and the
// example-server test config for P9 adds what that phase needs. `home_dir` is a temp
// directory, so the test never writes under the real home.
fn config_text(home: &std::path::Path) -> String {
    format!(
        r#"
server:
  home_dir: "{home}"
logging:
  default:
    console_level: info
gears:
  api-gateway:
    config:
      bind_addr: "127.0.0.1:0"
      auth_disabled: true
  application-manager:
    config:
      plugin_call_timeout: 500ms
      plugin_max_in_flight: 8
      max_request_tasks: 16
      max_envelope_text_length: 255
      pdp_call_timeout: 2s
      registry_operation_timeout: 3s
"#,
        home = home.display()
    )
}

fn cf_gears_binary() -> String {
    std::env::var("CARGO_BIN_EXE_cf-gears-example-server")
        .or_else(|_| std::env::var("CARGO_BIN_EXE_CF_GEARS_EXAMPLE_SERVER"))
        .expect("CARGO_BIN_EXE_cf-gears-example-server must be set for tests")
}

#[test]
#[ignore = "needs example-server feature (P9); platform dependency 5"]
fn failed_application_manager_start_makes_the_server_exit_non_zero() {
    let dir = TempDir::new().expect("temp dir");
    let home = dir.path().join("home");
    std::fs::create_dir_all(&home).expect("home dir");
    let config_path = dir.path().join("application-manager-no-audit.yaml");
    std::fs::write(&config_path, config_text(&home)).expect("write config");
    // Output goes to files, never to pipes nobody drains.
    let stdout_path = dir.path().join("stdout.txt");
    let stderr_path = dir.path().join("stderr.txt");

    let child = Command::new(cf_gears_binary())
        .args(["--config", config_path.to_str().unwrap(), "run"])
        .stdout(Stdio::from(std::fs::File::create(&stdout_path).unwrap()))
        .stderr(Stdio::from(std::fs::File::create(&stderr_path).unwrap()))
        .spawn()
        .expect("spawn the example server");
    let mut child = KillOnDrop(child);

    // A failed start ends the process by itself; a server that keeps running is the bug.
    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(status) = child.0.try_wait().expect("poll the server") {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "the server kept running after a failed start (platform dependency 5)"
        );
        std::thread::sleep(Duration::from_millis(100));
    };
    let text = format!(
        "{}{}",
        std::fs::read_to_string(&stdout_path).unwrap_or_default(),
        std::fs::read_to_string(&stderr_path).unwrap_or_default()
    );

    // An exit code, not a signal death: the process ended through `main` returning Err.
    let code = status
        .code()
        .unwrap_or_else(|| panic!("the server died by a signal, not by an exit: {status}: {text}"));
    assert_ne!(code, 0, "a failed start must exit non-zero: {text}");
    assert!(
        text.contains("no_audit_plugin"),
        "the startup reason must reach the operator: {text}"
    );
}
