//! The processes of the Claude Harness Nexus: a hook's report and a
//! transcript's watch. Built with no operating system under it.

#![no_std]
extern crate alloc;

mod generated;

pub use generated::*;

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
            Hook::SessionStart(_) => vec![Event::Started],
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
            manager.give(Watch { flow_id: start.session.identify(), file_path: start.file_path.clone(), offset: Offset(0) });
        }
        manager.give(Observe { flow_id: self.hook.identify(), event_vector: self.hook.events(), offset_option: None });
        None
    }

    fn take<N: Giving<Gift>>(&mut self, taken: ReportTaken, mut manager: N) -> Option<Reporting> {
        match taken {
            ReportTaken::Answer(Answer::Succeeded) => None,
            ReportTaken::Answer(Answer::Failed) => Some(Reporting::Unremembered),
            ReportTaken::Observation(observation) => {
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
        manager.give(Follow { file_path: self.file_path.clone(), offset: self.offset.clone(), count: self.count.clone() });
        None
    }

    fn take<N: Giving<Gift>>(&mut self, taken: WatchTaken, mut manager: N) -> Option<Watching> {
        match taken {
            WatchTaken::Following(Ok(Following::Rows(rows))) => {
                self.offset = rows.offset;
                let event_vector = rows.row_vector.iter().flat_map(Observable::events).collect();
                manager.give(Observe { flow_id: self.flow_id.clone(), event_vector, offset_option: Some(self.offset.clone()) });
                None
            }
            WatchTaken::Following(Ok(Following::Missing)) => Some(Watching::Missing),
            WatchTaken::Following(Err(_)) => Some(Watching::Unfollowed),
            WatchTaken::Observation(observation) => {
                for changed in observation.0 {
                    manager.give(changed)
                }
                None
            }
        }
    }
}
