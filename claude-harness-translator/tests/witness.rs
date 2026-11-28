//! The follow's witness of a session's processes: Claude's sessions directory
//! names each live process and its session; the session ends when no process
//! of the start recorded holds it: Cleared when its last process's record now
//! names another session, Died when its last process is gone.

use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::thread::JoinHandle;
use std::time::Duration;

use claude_harness_process::{Follow_Data, Following_Data, Offset, PendingAgentCount, ReadFrom, Row, RowCount, SessionRoot};
use claude_harness_translator::follow;
use harness::{EndReason, FilePath};
use transport::Framed;

const SESSION: &str = "5e7d1c3a-2b4f-4a6e-8d9c-0f1e2a3b4c5d";
const OTHER: &str = "9a8b7c6d-5e4f-4321-8765-43210fedcba9";

/// A fake Claude home: a sessions directory and a project holding the session's transcript.
struct Home {
    base: PathBuf,
}

impl Home {
    fn new(name: &str) -> Home {
        let base = std::env::temp_dir().join(format!("claude-harness-witness-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(base.join("sessions")).expect("the sessions directory");
        std::fs::create_dir_all(base.join("projects").join("-tmp")).expect("the project");
        std::fs::write(base.join("projects").join("-tmp").join(format!("{SESSION}.jsonl")), "").expect("the transcript");
        Home { base }
    }

    fn sessions(&self) -> PathBuf {
        self.base.join("sessions")
    }

    fn transcript(&self) -> PathBuf {
        self.transcript_of(SESSION)
    }

    fn transcript_of(&self, session: &str) -> PathBuf {
        self.base.join("projects").join("-tmp").join(format!("{session}.jsonl"))
    }

    /// A session's transcript made, as a /clear makes the new one, a tick after what was written before it.
    fn begin(&self, session: &str) {
        tick();
        std::fs::write(self.transcript_of(session), "").expect("the transcript");
    }

    /// Claude's record of a process, as it writes it: `<pid>.json`, a tick
    /// after what was written before it, so the two are not written at the
    /// same moment of the file system's clock.
    fn record(&self, process_id: u32, session: &str, start: &str) {
        tick();
        let record = format!(r#"{{"pid":{process_id},"sessionId":"{session}","cwd":"/tmp","procStart":"{start}","kind":"interactive","status":"busy"}}"#);
        std::fs::write(self.sessions().join(format!("{process_id}.json")), record).expect("the record");
    }

    fn append(&self, row: &str) {
        let mut transcript = std::fs::OpenOptions::new().append(true).open(self.transcript()).expect("the transcript");
        writeln!(transcript, "{row}").expect("the row");
    }

    /// The follow of the transcript, witnessed under the sessions directory.
    fn follow(&self) -> (UnixStream, JoinHandle<std::io::Result<()>>) {
        self.follow_of(SESSION)
    }

    fn follow_of(&self, session: &str) -> (UnixStream, JoinHandle<std::io::Result<()>>) {
        let (nexus, translator) = UnixStream::pair().expect("a pair");
        let emission = Follow_Data {
            file_path: FilePath(self.transcript_of(session).to_string_lossy().into_owned()),
            read_from: ReadFrom::Offset(Offset(0)),
            row_count: RowCount(64),
            session_root_option: Some(SessionRoot(FilePath(self.sessions().to_string_lossy().into_owned()))),
        };
        let following = std::thread::spawn(move || follow(translator, emission));
        nexus.set_read_timeout(Some(Duration::from_millis(1500))).expect("a timeout");
        (nexus, following)
    }
}

impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.base);
    }
}

/// Long enough that two files written either side of it have different modification times.
fn tick() {
    std::thread::sleep(Duration::from_millis(20));
}

/// A real process to stand as Claude's.
fn claude() -> Child {
    Command::new("sleep").arg("60").spawn().expect("a sleep")
}

/// The start the kernel gave a process: the twenty-second field of its stat.
fn start_of(process_id: u32) -> String {
    let stat = std::fs::read_to_string(format!("/proc/{process_id}/stat")).expect("its stat");
    stat.rsplit_once(") ").expect("its fields").1.split_whitespace().nth(19).expect("its start").to_owned()
}

/// The next outcome of the follow, None when nothing comes within the timeout.
fn next(nexus: &mut UnixStream) -> Option<Following_Data> {
    Following_Data::read_from(nexus).ok().flatten()
}

fn end(home: Home, nexus: UnixStream, following: JoinHandle<std::io::Result<()>>) {
    drop(nexus);
    assert!(following.join().expect("the follow").is_ok());
    drop(home);
}

#[test]
fn a_session_held_by_a_process_ends_died_when_it_is_killed() {
    let home = Home::new("killed");
    let mut process = claude();
    home.record(process.id(), SESSION, &start_of(process.id()));
    let (mut nexus, following) = home.follow();
    assert_eq!(next(&mut nexus), None, "a held session is not ended");

    process.kill().expect("the kill");
    process.wait().expect("the reaping");
    let ended = next(&mut nexus).expect("the end");
    assert_eq!(ended.end_reason_option, Some(EndReason::Died));
    assert!(ended.row_vector.is_empty());
    end(home, nexus, following);
}

#[test]
fn the_rows_a_process_wrote_come_before_its_end() {
    let home = Home::new("rows");
    let mut process = claude();
    home.record(process.id(), SESSION, &start_of(process.id()));
    let (mut nexus, following) = home.follow();
    assert_eq!(next(&mut nexus), None);

    home.append(r#"{"type":"system","subtype":"turn_duration","durationMs":1200}"#);
    process.kill().expect("the kill");
    process.wait().expect("the reaping");
    let mut rows = Vec::new();
    let ended = loop {
        let following = next(&mut nexus).expect("an outcome");
        rows.extend(following.row_vector);
        if let Some(end_reason) = following.end_reason_option {
            break end_reason;
        }
    };
    assert_eq!((rows, ended), (vec![Row::TurnEnded(PendingAgentCount(0))], EndReason::Died));
    end(home, nexus, following);
}

#[test]
fn a_record_whose_start_is_not_the_process_s_is_not_taken() {
    let home = Home::new("reused");
    let mut process = claude();
    home.record(process.id(), SESSION, "1");
    let (mut nexus, following) = home.follow();
    assert_eq!(next(&mut nexus).map(|following| following.end_reason_option), Some(Some(EndReason::Died)));
    let _ = process.kill();
    let _ = process.wait();
    end(home, nexus, following);
}

#[test]
fn a_session_no_process_holds_is_ended_at_once_and_once() {
    let home = Home::new("unheld");
    let mut other = claude();
    home.record(other.id(), OTHER, &start_of(other.id()));
    let (mut nexus, following) = home.follow();
    assert_eq!(next(&mut nexus).map(|following| following.end_reason_option), Some(Some(EndReason::Died)));

    home.append(r#"{"type":"system","subtype":"turn_duration","durationMs":1200}"#);
    home.record(other.id(), OTHER, &start_of(other.id()));
    assert_eq!(next(&mut nexus), Some(Following_Data { row_vector: vec![Row::TurnEnded(PendingAgentCount(0))], offset: Offset(62), end_reason_option: None }));
    assert_eq!(next(&mut nexus), None, "an end is told once");
    let _ = other.kill();
    let _ = other.wait();
    end(home, nexus, following);
}

#[test]
fn a_resumed_session_is_held_again_and_ends_again() {
    let home = Home::new("resumed");
    let (mut nexus, following) = home.follow();
    assert_eq!(next(&mut nexus).map(|following| following.end_reason_option), Some(Some(EndReason::Died)));

    let mut process = claude();
    home.record(process.id(), SESSION, &start_of(process.id()));
    std::thread::sleep(Duration::from_millis(200));
    home.append(r#"{"type":"user","isSidechain":false,"turnPosition":0,"message":{"role":"user","content":"again"}}"#);
    let resumed = next(&mut nexus).expect("the turn's start");
    assert_eq!((resumed.row_vector, resumed.end_reason_option), (vec![Row::TurnStarted], None));

    process.kill().expect("the kill");
    process.wait().expect("the reaping");
    assert_eq!(next(&mut nexus).map(|following| following.end_reason_option), Some(Some(EndReason::Died)));
    end(home, nexus, following);
}

#[test]
fn a_process_that_moves_to_another_session_has_cleared_this_one() {
    let home = Home::new("moved");
    let mut process = claude();
    let start = start_of(process.id());
    home.record(process.id(), SESSION, &start);
    let (mut nexus, following) = home.follow();
    assert_eq!(next(&mut nexus), None);

    home.begin(OTHER);
    home.record(process.id(), OTHER, &start);
    assert_eq!(next(&mut nexus).map(|following| following.end_reason_option), Some(Some(EndReason::Cleared)));
    let _ = process.kill();
    let _ = process.wait();
    end(home, nexus, following);
}

/// A /clear that writes the new transcript before the record names it: the new
/// session is not told Died while the record still names the old one; once
/// it is rewritten the old session is Cleared, the new one held, and Died
/// only when the process is gone.
#[test]
fn a_new_transcript_before_its_record_is_rewritten_is_not_died() {
    let home = Home::new("cleared");
    let mut process = claude();
    let start = start_of(process.id());
    home.record(process.id(), SESSION, &start);
    let (mut old, old_following) = home.follow();
    assert_eq!(next(&mut old), None);

    home.begin(OTHER);
    let (mut new, new_following) = home.follow_of(OTHER);
    assert_eq!(next(&mut new), None, "the record not yet rewritten tells no end of the new session");

    home.record(process.id(), OTHER, &start);
    assert_eq!(next(&mut old).map(|following| following.end_reason_option), Some(Some(EndReason::Cleared)));
    assert_eq!(next(&mut new), None, "the new session is held");

    process.kill().expect("the kill");
    process.wait().expect("the reaping");
    assert_eq!(next(&mut new).map(|following| following.end_reason_option), Some(Some(EndReason::Died)));
    assert_eq!(next(&mut old), None, "the old session's end is told once");
    drop(old);
    assert!(old_following.join().expect("the follow").is_ok());
    end(home, new, new_following);
}

/// The record rewritten before the new transcript is followed: the new
/// session is held at once, the old one Cleared.
#[test]
fn a_record_rewritten_before_the_new_transcript_holds_it() {
    let home = Home::new("rewritten");
    let mut process = claude();
    let start = start_of(process.id());
    home.record(process.id(), SESSION, &start);
    let (mut old, old_following) = home.follow();
    assert_eq!(next(&mut old), None);

    home.record(process.id(), OTHER, &start);
    home.begin(OTHER);
    assert_eq!(next(&mut old).map(|following| following.end_reason_option), Some(Some(EndReason::Cleared)));
    let (mut new, new_following) = home.follow_of(OTHER);
    assert_eq!(next(&mut new), None, "the new session is held");

    process.kill().expect("the kill");
    process.wait().expect("the reaping");
    assert_eq!(next(&mut new).map(|following| following.end_reason_option), Some(Some(EndReason::Died)));
    drop(old);
    assert!(old_following.join().expect("the follow").is_ok());
    end(home, new, new_following);
}

/// A session no process has held, awaiting a process whose record might yet
/// name it: that process gone, its record left behind, the session is Died.
#[test]
fn an_awaited_process_gone_tells_the_unheld_session_died() {
    let home = Home::new("awaited");
    let mut process = claude();
    home.record(process.id(), OTHER, &start_of(process.id()));
    home.begin(SESSION);
    let (mut nexus, following) = home.follow();
    assert_eq!(next(&mut nexus), None, "a process whose record is older than the transcript is awaited");

    process.kill().expect("the kill");
    process.wait().expect("the reaping");
    assert_eq!(next(&mut nexus).map(|following| following.end_reason_option), Some(Some(EndReason::Died)));
    end(home, nexus, following);
}

#[test]
fn a_sessions_directory_deleted_and_made_again_is_read_again() {
    let home = Home::new("remade");
    let mut first = claude();
    home.record(first.id(), SESSION, &start_of(first.id()));
    let (mut nexus, following) = home.follow();
    assert_eq!(next(&mut nexus), None, "a held session is not ended");

    std::fs::remove_dir_all(home.sessions()).expect("the sessions directory deleted");
    std::thread::sleep(Duration::from_millis(300));
    std::fs::create_dir_all(home.sessions()).expect("the sessions directory made again");
    let mut second = claude();
    home.record(second.id(), SESSION, &start_of(second.id()));
    std::thread::sleep(Duration::from_millis(300));

    first.kill().expect("the kill");
    first.wait().expect("the reaping");
    assert_eq!(next(&mut nexus), None, "the second process, recorded in the directory made again, holds the session");
    second.kill().expect("the kill");
    second.wait().expect("the reaping");
    assert_eq!(next(&mut nexus).map(|following| following.end_reason_option), Some(Some(EndReason::Died)));
    end(home, nexus, following);
}

#[test]
fn an_unreadable_record_is_skipped_and_holds_no_end_back() {
    let home = Home::new("unreadable");
    let mut process = claude();
    let mut other = claude();
    home.record(process.id(), SESSION, &start_of(process.id()));
    std::fs::write(home.sessions().join(format!("{}.json", other.id())), "{").expect("an unreadable record of a live process");
    let (mut nexus, following) = home.follow();
    assert_eq!(next(&mut nexus), None, "a held session is not ended");

    process.kill().expect("the kill");
    process.wait().expect("the reaping");
    assert_eq!(next(&mut nexus).map(|following| following.end_reason_option), Some(Some(EndReason::Died)));
    let _ = other.kill();
    let _ = other.wait();
    end(home, nexus, following);
}
