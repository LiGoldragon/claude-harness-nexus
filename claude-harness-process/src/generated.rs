//! What the generator would write from the Signal, Memory, Emission and Process
//! ethos of the Claude Harness Nexus. Written by hand while ethos-zero knows no
//! Process root; every path names core or alloc, so it builds with no_std.

use alloc::vec::Vec;
use core::option::Option;
use core::result::Result;

use flow::FlowId;
use harness::{BackgroundWork, EndReason, Event, FilePath, Observed, Response, Session, State, Tokens};
use process::{Givable, Giving, Processable, Unwindable};

macro_rules! generated {
    ($($item:item)*) => { $(
        #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
        #[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
        $item
    )* };
}

generated! {
    // Signal
    pub struct ProcessId(pub i64);
    pub struct UserId(pub i64);
    pub struct Caller { pub process_id: ProcessId, pub user_id: UserId }
    pub struct SessionStart { pub session: Session, pub file_path: FilePath }
    pub struct Stop { pub session: Session, pub response_option: Option<Response>, pub background_work_vector: Vec<BackgroundWork> }
    pub struct SessionEnd { pub session: Session, pub end_reason: EndReason }
    pub enum Hook {
        SessionStart(SessionStart),
        UserPromptSubmit(Session),
        PermissionRequest(Session),
        PostToolUse(Session),
        Stop(Stop),
        SessionEnd(SessionEnd),
    }
    pub struct Reported;
    pub struct Changed { pub flow_id: FlowId, pub event: Event, pub state: State }
    pub enum Input { Report(Hook), Subscribe }
    pub struct Call { pub caller: Caller, pub input: Input }
    pub enum Output { Reported(Reported), Subscribed(Vec<Observed>), Changed(Changed) }

    // Memory
    pub struct Offset(pub i64);
    pub struct Count(pub i64);
    pub struct Watch { pub flow_id: FlowId, pub file_path: FilePath, pub offset: Offset }
    pub struct Observe { pub flow_id: FlowId, pub event_vector: Vec<Event>, pub offset_option: Option<Offset> }
    pub struct Observation(pub Vec<Changed>);
    pub enum Answer { Succeeded, Failed }

    // Emission
    pub struct Follow { pub file_path: FilePath, pub offset: Offset, pub count: Count }
    pub struct Rows { pub row_vector: Vec<Row>, pub offset: Offset }
    pub enum Following { Rows(Rows), Missing }
    pub enum Row { Assistant(Tokens), Other }
    pub enum Unobserved { Unsubmitted, Unanswered }

    // Process
    pub struct ReportProcess { pub caller: Caller, pub hook: Hook }
    pub struct WatchProcess { pub flow_id: FlowId, pub file_path: FilePath, pub offset: Offset, pub count: Count }
    pub enum ReportTaken { Answer(Answer), Observation(Observation) }
    pub enum WatchTaken { Following(Result<Following, Unobserved>), Observation(Observation) }
    pub enum Reporting { Observed, Unremembered }
    pub enum Watching { Missing, Unfollowed, Forgotten }

    // The enums over the Nexus's processes
    pub enum Process { Report(ReportProcess), Watch(WatchProcess) }
    pub enum Gift { Reported(Reported), Changed(Changed), Watch(Watch), Observe(Observe), Follow(Follow) }
    pub enum Taken { Answer(Answer), Observation(Observation), Following(Result<Following, Unobserved>) }
    pub enum Outcome { Reporting(Reporting), Watching(Watching), Unwound }
}

macro_rules! givable {
    ($($gift:ident => $returned:ty),* $(,)?) => { $(
        impl Givable for $gift { type Returned = $returned; }
        impl From<$gift> for Gift { fn from(gift: $gift) -> Gift { Gift::$gift(gift) } }
    )* };
}

givable! {
    Reported => (),
    Changed => (),
    Watch => Answer,
    Observe => Observation,
    Follow => Result<Following, Unobserved>,
}

impl Unwindable for Outcome {
    fn unwound() -> Self {
        Outcome::Unwound
    }
}

impl Processable for Process {
    type Taken = Taken;
    type Gift = Gift;
    type Outcome = Outcome;

    fn begin<N: Giving<Gift>>(&mut self, giving: N) -> Option<Outcome> {
        match self {
            Process::Report(report) => report.begin(giving).map(Outcome::Reporting),
            Process::Watch(watch) => watch.begin(giving).map(Outcome::Watching),
        }
    }

    fn take<N: Giving<Gift>>(&mut self, taken: Taken, giving: N) -> Option<Outcome> {
        match (self, taken) {
            (Process::Report(report), Taken::Answer(answer)) => report.take(ReportTaken::Answer(answer), giving).map(Outcome::Reporting),
            (Process::Report(report), Taken::Observation(observation)) => report.take(ReportTaken::Observation(observation), giving).map(Outcome::Reporting),
            (Process::Watch(watch), Taken::Following(following)) => watch.take(WatchTaken::Following(following), giving).map(Outcome::Watching),
            (Process::Watch(watch), Taken::Observation(observation)) => watch.take(WatchTaken::Observation(observation), giving).map(Outcome::Watching),
            _ => None,
        }
    }
}
