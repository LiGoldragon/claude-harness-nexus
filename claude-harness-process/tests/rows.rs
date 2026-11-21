//! What the transcript's interrupted and compacted rows tell the state.

use claude_harness_process::*;
use harness::{BackgroundWork, Compaction, Description, Event, Flow, Observable, Observed, State, Tokens, Transitionable, Trigger, WorkId, WorkKind};

fn observed() -> Observed {
    Observed { flow_id: flow::FlowId("15c0ce".into()), state: State::Dry, background_work_vector: vec![], tokens: Tokens(0) }
}

#[test]
fn an_interrupted_row_reads_as_interrupted_and_rests_the_flow() {
    assert_eq!(Row::Interrupted.events(), vec![Event::Interrupted]);
    let mut flow = observed();
    flow.after(Event::PromptSubmitted);
    for event in Row::Interrupted.events() {
        flow.after(event);
    }
    assert_eq!(flow.state, State::Flowing(Flow::Rested));
}

#[test]
fn an_interrupted_row_with_work_in_flight_leaves_the_flow_waiting() {
    let work = BackgroundWork { work_id: WorkId("b1".into()), work_kind: WorkKind::Shell, description: Description("sleep".into()) };
    let mut flow = observed();
    flow.after(Event::PromptSubmitted);
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
    flow.after(Event::PromptSubmitted);
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
