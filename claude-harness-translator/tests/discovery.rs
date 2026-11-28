//! The translator's discovery under a transcript root: a session is a
//! `<uuid>.jsonl` directly in a project directory, reported once it appears.

use std::collections::BTreeSet;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use claude_harness_process::{Discover_Data, Discovered_Data};
use claude_harness_translator::discover;
use harness::{FilePath, SessionId};
use transport::Framed;

const AT_START: &str = "0f3c2a91-6b7d-4e58-a1c2-9d8e7f6a5b40";
const SUBAGENT_PARENT: &str = "4b1e8d2c-0a9f-4c37-b6e5-3f2a1d0c9e87";
const NEW_PROJECT: &str = "7a6c5b4d-3e2f-4a1b-9c8d-0e1f2a3b4c5d";
const OLD_PROJECT: &str = "c9d8e7f6-a5b4-4c3d-8e2f-1a0b9c8d7e6f";

fn root() -> PathBuf {
    let root = std::env::temp_dir().join(format!("claude-harness-discovery-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("the root");
    root
}

fn write(path: &Path) {
    std::fs::create_dir_all(path.parent().expect("a parent")).expect("the parent");
    std::fs::write(path, "{}\n").expect("the file");
}

fn discovered(path: &Path, session: &str) -> Discovered_Data {
    Discovered_Data { session_id: SessionId(session.into()), file_path: FilePath(path.to_string_lossy().into_owned()) }
}

#[test]
fn a_session_that_appears_is_discovered_and_nothing_else_is() {
    let root = root();
    let old_project = root.join("-home-li-primary");
    write(&old_project.join(format!("{AT_START}.jsonl")));

    let (nexus, translator) = UnixStream::pair().expect("a pair");
    let discovering = {
        let root = FilePath(root.to_string_lossy().into_owned());
        std::thread::spawn(move || discover(translator, Discover_Data { file_path: root }))
    };
    std::thread::sleep(Duration::from_millis(300));

    let new_project = root.join("-home-li-Code-claude-harness-nexus");
    let subagents = new_project.join(SUBAGENT_PARENT).join("subagents");
    write(&subagents.join("agent-a1b2c3d4e5f6.jsonl"));
    write(&subagents.join("agent-a1b2c3d4e5f6.meta.json"));
    write(&new_project.join(SUBAGENT_PARENT).join("tool-results").join("toolu_01abc.txt"));
    write(&new_project.join("notes.jsonl"));
    write(&new_project.join(format!("{NEW_PROJECT}.jsonl")));
    write(&old_project.join(format!("{OLD_PROJECT}.jsonl")));

    let mut reading = nexus.try_clone().expect("a reader");
    reading.set_read_timeout(Some(Duration::from_millis(1500))).expect("a timeout");
    let mut reported = BTreeSet::new();
    while let Ok(Some(discovered)) = Discovered_Data::read_from(&mut reading) {
        assert!(reported.insert((discovered.session_id.0.clone(), discovered.file_path.0.clone())), "reported twice: {discovered:?}");
    }
    let expected: BTreeSet<(String, String)> = [
        discovered(&new_project.join(format!("{NEW_PROJECT}.jsonl")), NEW_PROJECT),
        discovered(&old_project.join(format!("{OLD_PROJECT}.jsonl")), OLD_PROJECT),
    ]
    .into_iter()
    .map(|discovered| (discovered.session_id.0, discovered.file_path.0))
    .collect();
    assert_eq!(reported, expected);

    drop(reading);
    drop(nexus);
    assert!(discovering.join().expect("the discovery").is_ok());
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_root_not_yet_made_is_waited_for() {
    let base = std::env::temp_dir().join(format!("claude-harness-discovery-waited-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).expect("the base");
    let root = base.join(".claude").join("projects");

    let (nexus, translator) = UnixStream::pair().expect("a pair");
    let discovering = {
        let root = FilePath(root.to_string_lossy().into_owned());
        std::thread::spawn(move || discover(translator, Discover_Data { file_path: root }))
    };
    std::thread::sleep(Duration::from_millis(300));

    let session = root.join("-tmp").join(format!("{NEW_PROJECT}.jsonl"));
    write(&session);

    let mut reading = nexus.try_clone().expect("a reader");
    reading.set_read_timeout(Some(Duration::from_millis(1500))).expect("a timeout");
    assert_eq!(Discovered_Data::read_from(&mut reading).expect("a discovery"), Some(discovered(&session, NEW_PROJECT)));

    drop(reading);
    drop(nexus);
    assert!(discovering.join().expect("the discovery").is_ok());
    let _ = std::fs::remove_dir_all(&base);
}
