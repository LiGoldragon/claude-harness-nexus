//! The life of a discovery, as the manager traces it: each session discovered
//! becomes a Watch of its transcript, read from its first row.

use claude_harness_process::*;
use flow::FlowId;
use harness::{FilePath, SessionId};
use process::{Answer, Gave, Given, Identity, Living, Owner, Ownable, ProcessManager, Roots, Trace, Traced, Tracing, Unobserved};

#[derive(Default)]
struct World(Vec<Traced<Process>>);

impl Tracing<Process> for World {
    fn trace(&mut self, trace: Traced<Process>) {
        self.0.push(trace);
    }
}

fn discovery() -> Process {
    Process::Discover(DiscoverProcess { file_path: FilePath("/home/li/.claude/projects".into()), session_root_option: Some(SessionRoot(FilePath("/home/li/.claude/sessions".into()))) })
}

#[test]
fn a_discovered_session_is_watched_from_its_first_row() {
    let identity: Identity = Roots::default().draw();
    let mut manager = ProcessManager::default();
    let mut world = World::default();
    manager.bear(Living { identity: identity.clone(), process: discovery() }, &mut world);

    let transcript = FilePath("/home/li/.claude/projects/-home-li-primary/15c0ce0b-1d0c-4c58-9a57-3f1c8f1e15c0.jsonl".into());
    let found = Taken::Discovered(Ok(Discovered_Data { session_id: SessionId("15c0ce0b-1d0c-4c58-9a57-3f1c8f1e15c0".into()), file_path: transcript.clone() }));
    manager.hand(Given { identity: identity.clone(), taken: found.clone() }, &mut world);
    manager.hand(Given { identity: identity.clone(), taken: Taken::Answer(Answer::Succeeded) }, &mut world);

    let gave = |gift| Trace::Gave(Gave { identity: identity.clone(), gift });
    assert_eq!(
        world.0,
        vec![
            Trace::Born(Living { identity: identity.clone(), process: discovery() }),
            gave(Gift::Discover(Discover_Data { file_path: FilePath("/home/li/.claude/projects".into()), session_root_option: Some(SessionRoot(FilePath("/home/li/.claude/sessions".into()))) })),
            Trace::Took(Given { identity: identity.clone(), taken: found }),
            gave(Gift::Watch(Watch { flow_id: FlowId("15c0ce".into()), file_path: transcript, read_from: ReadFrom::Offset(Offset(0)) })),
            Trace::Took(Given { identity: identity.clone(), taken: Taken::Answer(Answer::Succeeded) }),
        ]
    );
}

#[test]
fn a_discovery_whose_emission_fails_ends_undiscovered() {
    let identity: Identity = Roots::default().draw();
    let mut manager = ProcessManager::default();
    let mut world = World::default();
    manager.bear(Living { identity: identity.clone(), process: discovery() }, &mut world);
    manager.hand(Given { identity: identity.clone(), taken: Taken::Discovered(Err(Unobserved::Unanswered)) }, &mut world);
    assert_eq!(world.0.last(), Some(&Trace::Ended(process::Ended { identity, outcome: Outcome::Discover(Discovering::Undiscovered) })));
}

#[test]
fn a_discovery_is_owned_by_the_memory() {
    assert_eq!(discovery().owner(), Owner::Memory);
}
