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
fn a_compacted_row_reads_as_compacted_and_leaves_the_state_as_it_was() {
    let compaction = Compaction { trigger: Trigger::Automatic, tokens_option: Some(Tokens(228425)) };
    assert_eq!(Row::Compacted(compaction.clone()).events(), vec![Event::Compacted(compaction)]);
    let mut flow = observed();
    flow.after(Event::PromptSubmitted);
    let before = flow.clone();
    for event in Row::Compacted(Compaction { trigger: Trigger::Manual, tokens_option: None }).events() {
        flow.after(event);
    }
    assert_eq!(flow, before);
}
