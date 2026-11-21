//! What the transcript's interrupted and compacted rows tell the state.

use claude_harness_process::*;
use harness::{BackgroundWork, Compaction, Description, Event, Flow, Observable, Observed, Prompt, PromptId, Sha256, SessionId, State, Tokens, Transitionable, Trigger, WorkId, WorkKind};

fn observed() -> Observed {
    Observed { session_id: SessionId("15c0ce0b-1d0c-4c58-9a57-3f1c8f1e15c0".into()), state: State::Dry, background_work_vector: vec![], tokens: Tokens(0), prompt_option: None }
}

#[test]
fn an_interrupted_row_reads_as_interrupted_and_rests_the_flow() {
    assert_eq!(Row::Interrupted.events(), vec![Event::Interrupted]);
    let mut flow = observed();
    flow.after(Event::PromptSubmitted(Prompt { prompt_id: PromptId("p1".into()), sha256: Sha256("digest".into()) }));
    for event in Row::Interrupted.events() {
        flow.after(event);
    }
    assert_eq!(flow.state, State::Flowing(Flow::Rested));
}

#[test]
fn an_interrupted_row_with_work_in_flight_leaves_the_flow_waiting() {
    let work = BackgroundWork { work_id: WorkId("b1".into()), work_kind: WorkKind::Shell, description: Description("sleep".into()) };
    let mut flow = observed();
    flow.after(Event::PromptSubmitted(Prompt { prompt_id: PromptId("p1".into()), sha256: Sha256("digest".into()) }));
    flow.after(Event::InFlight(vec![work]));
    for event in Row::Interrupted.events() {
        flow.after(event);
    }
    assert_eq!(flow.state, State::Flowing(Flow::Waiting));
}

#[test]
fn a_compacted_row_reports_the_boundary_then_updates_the_context_size() {
    let compaction = Compaction { trigger: Trigger::Automatic, tokens_option: Some(Tokens(18336)) };
    let row = Row::Compacted(Compacted_Data {
        compaction: compaction.clone(),
        tokens_option: Some(Tokens(6002)),
    });
    let events = row.events();
    assert_eq!(events, vec![Event::Compacted(compaction), Event::ContextSize(Tokens(6002))]);
    let mut flow = observed();
    flow.after(Event::PromptSubmitted(Prompt { prompt_id: PromptId("p1".into()), sha256: Sha256("digest".into()) }));
    let before = flow.state.clone();
    for event in events {
        flow.after(event);
    }
    assert_eq!(flow.state, before);
    assert_eq!(flow.tokens, Tokens(6002));
}

#[test]
fn a_compacted_row_without_post_tokens_gives_only_its_boundary() {
    let compaction = Compaction { trigger: Trigger::Manual, tokens_option: None };
    let row = Row::Compacted(Compacted_Data {
        compaction: compaction.clone(),
        tokens_option: None,
    });
    assert_eq!(row.events(), vec![Event::Compacted(compaction)]);
}
