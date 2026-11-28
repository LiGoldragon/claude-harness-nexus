//! Every follow in one loop: two hundred sessions, each held by a process of
//! its own, are followed over one inotify instance, each from Working to
//! Rested to its end, where one instance a follow would pass a small budget.

use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;

use claude_harness_process::{Follow_Data, Following_Data, Offset, PendingAgentCount, ReadFrom, Row, RowCount, SessionRoot};
use claude_harness_translator::Watching;
use harness::{EndReason, FilePath};
use transport::Framed;

const SESSIONS: usize = 200;

/// More than this many inotify instances in the test's process is one a follow.
const BUDGET: usize = 8;

/// A session's id: a canonical UUID numbered for it.
fn session(number: usize) -> String {
    format!("5e7d1c3a-2b4f-4a6e-8d9c-{number:012x}")
}

fn start_of(process_id: u32) -> String {
    let stat = std::fs::read_to_string(format!("/proc/{process_id}/stat")).expect("its stat");
    stat.rsplit_once(") ").expect("its fields").1.split_whitespace().nth(19).expect("its start").to_owned()
}

/// The inotify instances the test's process holds open.
fn instances() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .expect("the descriptors")
        .flatten()
        .filter(|entry| std::fs::read_link(entry.path()).is_ok_and(|target| target.to_string_lossy() == "anon_inode:inotify"))
        .count()
}

/// The next outcome of a follow.
fn next(nexus: &mut UnixStream) -> Following_Data {
    Following_Data::read_from(nexus).expect("an outcome").expect("not closed")
}

struct Followed {
    transcript: PathBuf,
    process: Child,
    nexus: UnixStream,
}

#[test]
fn two_hundred_sessions_are_followed_in_one_loop_from_working_to_rested_to_their_end() {
    let base = std::env::temp_dir().join(format!("claude-harness-many-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let sessions = base.join("sessions");
    std::fs::create_dir_all(&sessions).expect("the sessions directory");
    let watching = Watching::start().expect("the loop");
    let session_root = SessionRoot(FilePath(sessions.to_string_lossy().into_owned()));

    let mut followed: Vec<Followed> = (0..SESSIONS)
        .map(|number| {
            let project = base.join("projects").join(format!("-tmp-{}", number % 10));
            std::fs::create_dir_all(&project).expect("the project");
            let transcript = project.join(format!("{}.jsonl", session(number)));
            std::fs::write(&transcript, "").expect("the transcript");
            let process = Command::new("sleep").arg("120").spawn().expect("a sleep");
            let record = format!(r#"{{"pid":{},"sessionId":"{}","procStart":"{}"}}"#, process.id(), session(number), start_of(process.id()));
            std::fs::write(sessions.join(format!("{}.json", process.id())), record).expect("the record");
            let (nexus, translator) = UnixStream::pair().expect("a pair");
            nexus.set_read_timeout(Some(Duration::from_secs(10))).expect("a timeout");
            watching.follow(
                translator,
                Follow_Data { file_path: FilePath(transcript.to_string_lossy().into_owned()), read_from: ReadFrom::Offset(Offset(0)), row_count: RowCount(64), session_root_option: Some(session_root.clone()) },
            );
            Followed { transcript, process, nexus }
        })
        .collect();
    std::thread::sleep(Duration::from_millis(500));
    assert!(instances() <= BUDGET, "{} inotify instances for {SESSIONS} follows", instances());

    let append = |transcript: &PathBuf, row: &str| {
        let mut file = std::fs::OpenOptions::new().append(true).open(transcript).expect("the transcript");
        writeln!(file, "{row}").expect("the row");
    };
    for session in &followed {
        append(&session.transcript, r#"{"type":"user","isSidechain":false,"turnPosition":0,"message":{"role":"user","content":"work"}}"#);
    }
    for session in &mut followed {
        let working = next(&mut session.nexus);
        assert_eq!((working.row_vector, working.end_reason_option), (vec![Row::TurnStarted], None), "Working");
    }
    for session in &followed {
        append(&session.transcript, r#"{"type":"system","subtype":"turn_duration","durationMs":1200}"#);
    }
    for session in &mut followed {
        let rested = next(&mut session.nexus);
        assert_eq!((rested.row_vector, rested.end_reason_option), (vec![Row::TurnEnded(PendingAgentCount(0))], None), "Rested");
    }
    for session in &mut followed {
        session.process.kill().expect("the kill");
        session.process.wait().expect("the reaping");
    }
    for session in &mut followed {
        assert_eq!(next(&mut session.nexus).end_reason_option, Some(EndReason::Died));
    }
    assert!(instances() <= BUDGET);
    drop(followed);
    let _ = std::fs::remove_dir_all(&base);
}
