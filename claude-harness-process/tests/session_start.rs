//! What a session's start tells the state, by how it started.

use claude_harness_process::*;
use harness::{Event, Flow, FilePath, Identifiable, Observable, Observed, SessionId, State, Tokens, Transitionable};

fn start(source: Source) -> Hook {
    Hook::SessionStart(SessionStart {
        session_id: SessionId("15c0ce0b-1d0c-4c58-9a57-3f1c8f1e15c0".into()),
        file_path: FilePath("/tmp/transcript.jsonl".into()),
        source,
    })
}

fn observed() -> Observed {
    Observed { flow_id: flow::FlowId("15c0ce".into()), state: State::Dry, background_work_vector: vec![], tokens: Tokens(0) }
}

#[test]
fn a_session_that_started_gives_started() {
    assert_eq!(start(Source::Startup).events(), vec![Event::Started]);
}

#[test]
fn a_session_resumed_gives_resumed_and_then_its_context_size() {
    assert_eq!(start(Source::Resume(Some(Tokens(181204)))).events(), vec![Event::Resumed, Event::ContextSize(Tokens(181204))]);
    assert_eq!(start(Source::Resume(None)).events(), vec![Event::Resumed]);
}

#[test]
fn a_session_compacted_gives_no_event_from_the_hook() {
    assert_eq!(start(Source::Compact).events(), vec![]);
}

#[test]
fn a_resumed_session_is_rested_and_keeps_its_context_size() {
    let mut flow = observed();
    flow.after(Event::PromptSubmitted);
    for event in start(Source::Resume(Some(Tokens(181204)))).events() {
        flow.after(event);
    }
    assert_eq!(flow.state, State::Flowing(Flow::Rested));
    assert_eq!(flow.tokens, Tokens(181204));
}

#[test]
fn a_hook_identifies_its_flow_by_the_first_six_characters_of_its_session() {
    assert_eq!(start(Source::Startup).identify(), flow::FlowId("15c0ce".into()));
}
