//! A session's end, as its Watch is told it by the follow: after the rows its
//! last process wrote, and with the sessions directory the Watch was born with.

use claude_harness_process::*;
use flow::FlowId;
use harness::{EndReason, Event, FilePath};
use process::{Gave, Given, Identity, Living, ProcessManager, Roots, Trace, Traced, Tracing};

#[derive(Default)]
struct World(Vec<Traced<Process>>);

impl Tracing<Process> for World {
    fn trace(&mut self, trace: Traced<Process>) {
        self.0.push(trace);
    }
}

fn session_root() -> SessionRoot {
    SessionRoot(FilePath("/home/li/.claude/sessions".into()))
}

fn watch() -> Process {
    Process::Watch(WatchProcess {
        flow_id: FlowId("5e7d1c".into()),
        file_path: FilePath("/home/li/.claude/projects/-tmp/5e7d1c3a-2b4f-4a6e-8d9c-0f1e2a3b4c5d.jsonl".into()),
        read_from: ReadFrom::Offset(Offset(0)),
        row_count: RowCount(64),
        session_root_option: Some(session_root()),
    })
}

#[test]
fn a_watch_follows_with_its_sessions_directory_and_observes_the_end_after_the_rows() {
    let identity: Identity = Roots::default().draw();
    let mut manager = ProcessManager::default();
    let mut world = World::default();
    manager.bear(Living { identity: identity.clone(), process: watch() }, &mut world);

    let ended = Taken::Following(Ok(Following_Data { row_vector: vec![Row::TurnEnded(PendingAgentCount(0))], offset: Offset(62), end_reason_option: Some(EndReason::Died) }));
    manager.hand(Given { identity: identity.clone(), taken: ended.clone() }, &mut world);

    let gave = |gift| Trace::Gave(Gave { identity: identity.clone(), gift });
    assert_eq!(
        world.0,
        vec![
            Trace::Born(Living { identity: identity.clone(), process: watch() }),
            gave(Gift::Follow(Follow_Data {
                file_path: FilePath("/home/li/.claude/projects/-tmp/5e7d1c3a-2b4f-4a6e-8d9c-0f1e2a3b4c5d.jsonl".into()),
                read_from: ReadFrom::Offset(Offset(0)),
                row_count: RowCount(64),
                session_root_option: Some(session_root()),
            })),
            Trace::Took(Given { identity: identity.clone(), taken: ended }),
            gave(Gift::Observe(Observe { flow_id: FlowId("5e7d1c".into()), event_vector: vec![Event::InFlight(vec![]), Event::TurnEnded, Event::Ended(EndReason::Died)], offset_option: Some(Offset(62)) })),
        ]
    );
}

#[test]
fn a_watch_goes_on_past_the_end() {
    let identity: Identity = Roots::default().draw();
    let mut manager = ProcessManager::default();
    let mut world = World::default();
    manager.bear(Living { identity: identity.clone(), process: watch() }, &mut world);
    let ended = Taken::Following(Ok(Following_Data { row_vector: vec![], offset: Offset(0), end_reason_option: Some(EndReason::Died) }));
    manager.hand(Given { identity: identity.clone(), taken: ended }, &mut world);
    let resumed = Taken::Following(Ok(Following_Data { row_vector: vec![Row::TurnStarted], offset: Offset(90), end_reason_option: None }));
    manager.hand(Given { identity: identity.clone(), taken: resumed }, &mut world);
    assert_eq!(
        world.0.last(),
        Some(&Trace::Gave(Gave { identity, gift: Gift::Observe(Observe { flow_id: FlowId("5e7d1c".into()), event_vector: vec![Event::PromptSubmitted], offset_option: Some(Offset(90)) }) }))
    );
}
