//! The processes of the Claude Harness Nexus: a hook's report and a
//! transcript's watch. Built with no operating system under it.

// The text side, where datom is spoken, is built with std; the Nexus's build is not.
#![cfg_attr(not(feature = "datom"), no_std)]
extern crate alloc;

mod generated;

pub use generated::*;

/// The version of these types: the memory and the trace are named for it.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;

use flow::FlowId;
use harness::{Event, Observable, Session};
use process::{Giving, Processable};

pub trait Identifiable {
    fn identify(&self) -> FlowId;
}

impl Identifiable for Session {
    fn identify(&self) -> FlowId {
        FlowId(self.0.chars().take(6).collect::<String>())
    }
}

impl Identifiable for Hook {
    fn identify(&self) -> FlowId {
        match self {
            Hook::SessionStart(start) => start.session.identify(),
            Hook::UserPromptSubmit(session) | Hook::PermissionRequest(session) | Hook::PostToolUse(session) => session.identify(),
            Hook::Stop(stop) => stop.session.identify(),
            Hook::SessionEnd(end) => end.session.identify(),
        }
    }
}

impl Observable for Hook {
    fn events(&self) -> Vec<Event> {
        match self {
            // A resume is Resumed, followed by its context's size; Resumed waits on the harness library.
            Hook::SessionStart(start) => match &start.source {
                Source::Startup => vec![Event::Started],
                Source::Resume(tokens) => tokens.iter().copied().map(Event::ContextSize).collect(),
                Source::Compact => vec![],
            },
            Hook::UserPromptSubmit(_) => vec![Event::PromptSubmitted],
            Hook::PermissionRequest(_) => vec![Event::PermissionAsked],
            Hook::PostToolUse(_) => vec![Event::ToolUsed],
            Hook::Stop(stop) => {
                let mut events: Vec<Event> = stop.response_option.iter().cloned().map(Event::Responded).collect();
                events.push(Event::InFlight(stop.background_work_vector.clone()));
                events.push(Event::TurnEnded);
                events
            }
            Hook::SessionEnd(end) => vec![Event::Ended(end.end_reason.clone())],
        }
    }
}

impl Observable for Row {
    fn events(&self) -> Vec<Event> {
        match self {
            Row::Assistant(tokens) => vec![Event::ContextSize(*tokens)],
            Row::TurnStarted => vec![Event::PromptSubmitted],
            Row::TurnEnded => vec![Event::TurnEnded],
            Row::Interrupted => vec![Event::Interrupted],
            Row::Compacted(compaction) => vec![Event::Compacted(compaction.clone())],
            Row::Other => vec![],
        }
    }
}

impl Processable for ReportProcess {
    type Taken = ReportTaken;
    type Gift = Gift;
    type Outcome = Reporting;

    fn begin<N: Giving<Gift>>(&mut self, mut manager: N) -> Option<Reporting> {
        manager.give(Reported);
        if let Hook::SessionStart(start) = &self.hook {
            // A session that started is read from its first row; one resumed or compacted from the file's end.
            let read_from = match start.source {
                Source::Startup => ReadFrom::Offset(Offset(0)),
                Source::Resume(_) | Source::Compact => ReadFrom::FileEnd,
            };
            manager.give(Watch { flow_id: start.session.identify(), file_path: start.file_path.clone(), read_from });
        }
        manager.give(Observe { flow_id: self.hook.identify(), event_vector: self.hook.events(), offset_option: None });
        None
    }

    fn take<N: Giving<Gift>>(&mut self, taken: ReportTaken, mut manager: N) -> Option<Reporting> {
        match taken {
            ReportTaken::Answer(Answer::Succeeded) => None,
            ReportTaken::Answer(Answer::Failed) => Some(Reporting::Unremembered),
            ReportTaken::Observation(Err(Unremembered)) => Some(Reporting::Unremembered),
            ReportTaken::Observation(Ok(observation)) => {
                for changed in observation.0 {
                    manager.give(changed)
                }
                Some(Reporting::Observed)
            }
        }
    }
}

impl Processable for WatchProcess {
    type Taken = WatchTaken;
    type Gift = Gift;
    type Outcome = Watching;

    fn begin<N: Giving<Gift>>(&mut self, mut manager: N) -> Option<Watching> {
        manager.give(Follow { file_path: self.file_path.clone(), read_from: self.read_from.clone(), row_count: self.row_count.clone() });
        None
    }

    fn take<N: Giving<Gift>>(&mut self, taken: WatchTaken, mut manager: N) -> Option<Watching> {
        match taken {
            WatchTaken::Following(Ok(following)) => {
                self.read_from = ReadFrom::Offset(following.offset.clone());
                let event_vector = following.row_vector.iter().flat_map(Observable::events).collect();
                manager.give(Observe { flow_id: self.flow_id.clone(), event_vector, offset_option: Some(following.offset) });
                None
            }
            WatchTaken::Following(Err(_)) => Some(Watching::Unfollowed),
            // A Watch goes on when an Observe was not kept: its next carries the later offset.
            WatchTaken::Observation(Err(Unremembered)) => None,
            WatchTaken::Observation(Ok(observation)) => {
                for changed in observation.0 {
                    manager.give(changed)
                }
                None
            }
        }
    }
}

impl Processable for SubscribeProcess {
    type Taken = Result<Recollection, Unremembered>;
    type Gift = Gift;
    /// It ends one way, when its connection closes, and so declares no outcome.
    type Outcome = ();

    fn begin<N: Giving<Gift>>(&mut self, mut manager: N) -> Option<()> {
        manager.give(Recall);
        None
    }

    /// It answers with every session as it is now, then rests while its connection is open.
    fn take<N: Giving<Gift>>(&mut self, recollection: Result<Recollection, Unremembered>, mut manager: N) -> Option<()> {
        if let Ok(recollection) = recollection {
            manager.give(Subscribed(recollection.0));
        }
        None
    }
}

impl Processable for ConfigureProcess {
    type Taken = Answer;
    type Gift = Gift;
    type Outcome = Configuring;

    fn begin<N: Giving<Gift>>(&mut self, mut manager: N) -> Option<Configuring> {
        manager.give(Keep(self.set.clone()));
        None
    }

    fn take<N: Giving<Gift>>(&mut self, answer: Answer, mut manager: N) -> Option<Configuring> {
        match answer {
            Answer::Succeeded => {
                manager.give(Configured(self.set.clone()));
                Some(Configuring::Configured)
            }
            Answer::Failed => Some(Configuring::Unremembered),
        }
    }
}

/// Born of a Prepare, holding the hook client and the hooks as configured then:
/// it gives Install, and answers with the path the translator wrote, or None,
/// the reason for it being its outcome in the trace.
impl Processable for PrepareProcess {
    type Taken = Result<Installation, Unobserved>;
    type Gift = Gift;
    type Outcome = Preparing;

    fn begin<N: Giving<Gift>>(&mut self, mut manager: N) -> Option<Preparing> {
        match &self.hook_client_option {
            Some(hook_client) => {
                manager.give(Install { hook_client: hook_client.clone(), hooks: self.hooks.clone() });
                None
            }
            None => {
                manager.give(Prepared(None));
                Some(Preparing::Unconfigured)
            }
        }
    }

    fn take<N: Giving<Gift>>(&mut self, installation: Result<Installation, Unobserved>, mut manager: N) -> Option<Preparing> {
        let (prepared, preparing) = match installation {
            Ok(Installation::Installed(file_path)) => (Some(file_path), Preparing::Installed),
            Ok(Installation::Uninstalled(_)) => (None, Preparing::Uninstalled),
            Err(_) => (None, Preparing::Unobserved),
        };
        manager.give(Prepared(prepared));
        Some(preparing)
    }
}
