//! The meta datoms the CLI is typed, as the translator reads them.

use claude_harness_process::{EndedRetention, SessionRoot, Set, meta};
use harness::FilePath;

#[test]
fn the_sessions_directory_is_configured_beside_the_transcript_root() {
    let read = |text: &str| transport::actualize::<meta::Input>(text).expect("a meta input");
    assert_eq!(read("Configure.SessionRoot.«/home/li/.claude/sessions»"), meta::Input::Configure(Set::SessionRoot(SessionRoot(FilePath("/home/li/.claude/sessions".into())))));
    assert_eq!(read("Configure.TranscriptRoot.«/home/li/.claude/projects»"), meta::Input::Configure(Set::TranscriptRoot(FilePath("/home/li/.claude/projects".into()))));
}

#[test]
fn the_retention_of_an_ended_session_is_configured_in_minutes() {
    let read = |text: &str| transport::actualize::<meta::Input>(text).expect("a meta input");
    assert_eq!(read("Configure.EndedRetention.30"), meta::Input::Configure(Set::EndedRetention(EndedRetention(30))));
}
