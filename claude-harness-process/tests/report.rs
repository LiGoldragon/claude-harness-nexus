//! The whole life of one report, and of a watch ended with its owner, as the
//! manager traces them.

use claude_harness_process::*;
use flow::FlowId;
use harness::{Event, Flow, SessionId, State};
use process::{Answer, Arrival, Caller, Gave, Given, Identity, Living, Path, ProcessId, ProcessManager, Root, Roots, Trace, Traced, Tracing, UserId};

#[derive(Default)]
struct World(Vec<Traced<Process>>);

impl Tracing<Process> for World {
    fn trace(&mut self, trace: Traced<Process>) {
        self.0.push(trace);
    }
}

fn identity(root: i64, path: &[i64]) -> Identity {
    Identity { root: Root(root), path: Path(path.to_vec()) }
}

fn caller() -> Caller {
    Caller { process_id: ProcessId(48211), user_id: UserId(1000) }
}

#[test]
fn a_report_gives_its_answer_and_its_observation_and_ends_observed() {
    let session = SessionId("15c0ce0b-1d0c-4c58-9a57-3f1c8f1e15c0".into());
    let mut manager = ProcessManager::default();
    let mut world = World::default();

    let report = Process::Report(ReportProcess { caller: caller(), hook: Hook::PostToolUse(session) });
    manager.bear(Living { identity: Roots::default().draw(), process: report.clone() }, &mut world);

    let changed = Changed { flow_id: FlowId("15c0ce".into()), event: Event::ToolUsed, state: State::Flowing(Flow::Working) };
    let taken = Taken::Observation(Ok(Observation(vec![changed.clone()])));
    manager.hand(Given { identity: identity(1, &[]), taken: taken.clone() }, &mut world);

    let gave = |gift| Trace::Gave(Gave { identity: identity(1, &[]), gift });
    assert_eq!(
        world.0,
        vec![
            Trace::Born(Living { identity: identity(1, &[]), process: report }),
            gave(Gift::Reported),
            gave(Gift::Observe(Observe { flow_id: FlowId("15c0ce".into()), event_vector: vec![Event::ToolUsed], offset_option: None })),
            Trace::Took(Given { identity: identity(1, &[]), taken }),
            gave(Gift::Changed(changed)),
            Trace::Ended(process::Ended { identity: identity(1, &[]), outcome: Outcome::Report(Reporting::Observed) }),
        ]
    );
}

#[test]
fn what_comes_for_an_ended_process_is_let_go() {
    let mut manager = ProcessManager::<Process>::default();
    let mut world = World::default();
    manager.hand(Given { identity: identity(9, &[]), taken: Taken::Answer(Answer::Succeeded) }, &mut world);
    assert!(world.0.is_empty());
}

#[test]
fn an_owned_process_ends_before_its_owner_the_last_born_first() {
    let watch = |name: &str| {
        Process::Watch(WatchProcess { flow_id: FlowId(name.into()), file_path: harness::FilePath(format!("/t/{name}.jsonl")), read_from: ReadFrom::Offset(Offset(0)), row_count: RowCount(64), session_root_option: None })
    };
    let mut manager = ProcessManager::default();
    let mut world = World::default();
    let queue = vec![
        Arrival::Born(Living { identity: identity(1, &[]), process: watch("owner") }),
        Arrival::Born(Living { identity: identity(1, &[1]), process: watch("first") }),
        Arrival::Born(Living { identity: identity(1, &[2]), process: watch("second") }),
        Arrival::Born(Living { identity: identity(1, &[1, 1]), process: watch("grandchild") }),
    ];
    manager.run(queue, &mut world);

    let ended: Vec<Identity> = world
        .0
        .into_iter()
        .filter_map(|trace| match trace {
            Trace::Ended(ended) => Some(ended.identity),
            _ => None,
        })
        .collect();
    assert_eq!(ended, vec![identity(1, &[2]), identity(1, &[1, 1]), identity(1, &[1]), identity(1, &[])]);
}

#[test]
fn an_end_from_outside_takes_a_living_process_out_and_lets_go_of_a_gone_one() {
    let watch = Process::Watch(WatchProcess { flow_id: FlowId("0ea2f9".into()), file_path: harness::FilePath("/t/0ea2f9.jsonl".into()), read_from: ReadFrom::Offset(Offset(0)), row_count: RowCount(64), session_root_option: None });
    let forgotten = || Outcome::Watch(Watching::Forgotten);
    let mut manager = ProcessManager::default();
    let mut world = World::default();
    let queue = vec![
        Arrival::Born(Living { identity: identity(1, &[]), process: watch }),
        Arrival::Ended(process::Ended { identity: identity(1, &[]), outcome: forgotten() }),
        Arrival::Ended(process::Ended { identity: identity(1, &[]), outcome: forgotten() }),
    ];
    manager.run(queue, &mut world);
    let ended: Vec<_> = world.0.into_iter().filter(|trace| matches!(trace, Trace::Ended(_))).collect();
    assert_eq!(ended, vec![Trace::Ended(process::Ended { identity: identity(1, &[]), outcome: forgotten() })]);
}

#[test]
fn a_birth_under_an_ended_owner_is_let_go() {
    let watch = Process::Watch(WatchProcess { flow_id: FlowId("0ea2f9".into()), file_path: harness::FilePath("/t/0ea2f9.jsonl".into()), read_from: ReadFrom::Offset(Offset(0)), row_count: RowCount(64), session_root_option: None });
    let mut manager = ProcessManager::default();
    let mut world = World::default();
    manager.run(vec![Arrival::Born(Living { identity: identity(3, &[1]), process: watch })], &mut world);
    assert!(world.0.is_empty());
}

#[test]
fn at_the_end_what_the_signal_process_owns_ends_before_what_the_memory_process_owns() {
    let watch = Process::Watch(WatchProcess { flow_id: FlowId("0ea2f9".into()), file_path: harness::FilePath("/t/0ea2f9.jsonl".into()), read_from: ReadFrom::Offset(Offset(0)), row_count: RowCount(64), session_root_option: None });
    let subscribe = Process::Subscribe(SubscribeProcess { caller: caller() });
    let roots = Roots::default();
    let (first, second) = (roots.draw(), roots.draw());
    let mut manager = ProcessManager::default();
    let mut world = World::default();
    let queue = vec![Arrival::Born(Living { identity: first.clone(), process: subscribe }), Arrival::Born(Living { identity: second.clone(), process: watch })];
    manager.run(queue, &mut world);
    let ended: Vec<Identity> = world
        .0
        .into_iter()
        .filter_map(|trace| match trace {
            Trace::Ended(ended) => Some(ended.identity),
            _ => None,
        })
        .collect();
    assert_eq!(ended, vec![first, second]);
}
