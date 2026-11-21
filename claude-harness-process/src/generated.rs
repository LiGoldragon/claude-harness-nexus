//! What the generator would write from the Signal, Memory, Emission and Process
//! ethos of the Claude Harness Nexus. Written by hand while ethos-zero knows no
//! Process root; every path names core or alloc, so it builds with no_std.

use alloc::string::String;
use alloc::vec::Vec;
use core::option::Option;
use core::result::Result;

use flow::FlowId;
use harness::{BackgroundWork, Compaction, EndReason, Event, FilePath, Observed, Response, Session, State, Tokens};
use process::{Givable, Giving, Ownable, Owner, Processable, Unwindable};

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
    pub enum Source { Startup, Resume(Option<Tokens>), Compact }
    pub struct SessionStart { pub session: Session, pub file_path: FilePath, pub source: Source }
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
    // Listening is the library's, one input every Nexus has: its translator has begun to listen.
    // It is answered at once by Acknowledged. Subscribe is signal-harness's, Prepare signal-claude-prepare's.
    pub enum Input { Report(Hook), Listening }
    pub struct Call { pub caller: Caller, pub input: Input }
    pub enum Output { Reported, Acknowledged }

    // Of the contracts the Nexus serves beside its own, as its processes hold them
    pub struct Changed { pub flow_id: FlowId, pub event: Event, pub state: State }
    pub struct Subscribed(pub Vec<Observed>);
    pub struct Prepared(pub Option<FilePath>);

    // Meta signal
    pub struct HookName(pub String);
    pub struct Hooks(pub Vec<HookName>);
    pub struct HookClient(pub String);
    pub enum Set { Hooks(Hooks), RemoveHook(HookName), HookClient(HookClient), RowCount(RowCount) }
    pub enum MetaInput { Configure(Set) }
    pub enum MetaOutput { Configured(Set) }
    pub struct MetaCall { pub caller: Caller, pub meta_input: MetaInput }

    // Library: the changes and their types, until the Memory page places them
    pub struct Offset(pub i64);
    // Where a transcript is read from: its first row, the file's end, or an offset read so far.
    pub enum ReadFrom { FileEnd, Offset(Offset) }
    // What returns for a change when it was not kept.
    pub struct Unremembered;
    pub struct RowCount(pub i64);

    // Memory
    pub struct Watch { pub flow_id: FlowId, pub file_path: FilePath, pub read_from: ReadFrom }
    pub struct Observe { pub flow_id: FlowId, pub event_vector: Vec<Event>, pub offset_option: Option<Offset> }
    pub struct Observation(pub Vec<Changed>);
    pub enum Answer { Succeeded, Failed }
    pub struct Recall;
    pub struct Recollection(pub Vec<Observed>);
    pub struct Keep(pub Set);
    pub struct Remembered { pub observed_vector: Vec<Observed>, pub watch_vector: Vec<Watch>, pub hooks: Hooks, pub hook_client_option: Option<HookClient>, pub row_count_option: Option<RowCount> }

    // Emission
    pub struct Follow { pub file_path: FilePath, pub read_from: ReadFrom, pub row_count: RowCount }
    pub struct Following { pub row_vector: Vec<Row>, pub offset: Offset }
    pub enum Row { Assistant(Tokens), TurnStarted, TurnEnded, Interrupted, Compacted(Compaction), Other }
    pub struct Install { pub hook_client: HookClient, pub hooks: Hooks }
    pub enum Installation { Installed(FilePath), Uninstalled(i64) }
    pub enum Emission { Follow(Follow), Install(Install) }
    pub enum Unobserved { Unsubmitted, Unanswered }

    // Process
    pub struct ReportProcess { pub caller: Caller, pub hook: Hook }
    pub struct WatchProcess { pub flow_id: FlowId, pub file_path: FilePath, pub read_from: ReadFrom, pub row_count: RowCount }
    pub enum ReportTaken { Answer(Answer), Observation(Result<Observation, Unremembered>) }
    pub enum WatchTaken { Following(Result<Following, Unobserved>), Observation(Result<Observation, Unremembered>) }
    pub enum Reporting { Observed, Unremembered }
    pub enum Watching { Forgotten, Unfollowed }
    pub struct SubscribeProcess { pub caller: Caller }
    pub struct ConfigureProcess { pub caller: Caller, pub set: Set }
    pub struct Configured(pub Set);
    pub enum Configuring { Configured, Unremembered }
    // The hook client and the hooks, moved in at birth; no hook client configured is a None.
    pub struct PrepareProcess { pub hook_client_option: Option<HookClient>, pub hooks: Hooks }
    pub enum Preparing { Installed, Uninstalled, Unobserved, Unconfigured }

    // The enums over the Nexus's processes
    pub enum Process { Report(ReportProcess), Watch(WatchProcess), Subscribe(SubscribeProcess), Configure(ConfigureProcess), Prepare(PrepareProcess) }
    pub enum Gift { Reported, Subscribed(Subscribed), Changed(Changed), Watch(Watch), Observe(Observe), Recall(Recall), Keep(Keep), Configured(Configured), Follow(Follow), Install(Install), Prepared(Prepared) }
    pub enum Taken { Answer(Answer), Observation(Result<Observation, Unremembered>), Recollection(Result<Recollection, Unremembered>), Following(Result<Following, Unobserved>), Installation(Result<Installation, Unobserved>) }
    // One variant for each process, named for it; a process that ends one way carries nothing.
    pub enum Outcome { Report(Reporting), Watch(Watching), Subscribe, Configure(Configuring), Prepare(Preparing), Unwound }
}

macro_rules! givable {
    ($($gift:ident => $returned:ty),* $(,)?) => { $(
        impl Givable for $gift { type Returned = $returned; }
        impl From<$gift> for Gift { fn from(gift: $gift) -> Gift { Gift::$gift(gift) } }
    )* };
}

// An output that carries nothing is a variant of the Gift that carries nothing.
impl Givable for Reported { type Returned = (); }
impl From<Reported> for Gift { fn from(Reported: Reported) -> Gift { Gift::Reported } }

givable! {
    Subscribed => (),
    Recall => Result<Recollection, Unremembered>,
    Keep => Answer,
    Configured => (),
    Changed => (),
    Watch => Answer,
    Observe => Result<Observation, Unremembered>,
    Follow => Result<Following, Unobserved>,
    Install => Result<Installation, Unobserved>,
    Prepared => (),
}

impl Unwindable for Outcome {
    fn unwound() -> Self {
        Outcome::Unwound
    }
}

impl Ownable for Process {
    fn owner(&self) -> Owner {
        match self {
            Process::Watch(_) => Owner::Memory,
            Process::Report(_) | Process::Subscribe(_) | Process::Configure(_) | Process::Prepare(_) => Owner::Signal,
        }
    }
}

impl Processable for Process {
    type Taken = Taken;
    type Gift = Gift;
    type Outcome = Outcome;

    fn begin<N: Giving<Gift>>(&mut self, giving: N) -> Option<Outcome> {
        match self {
            Process::Report(report) => report.begin(giving).map(Outcome::Report),
            Process::Watch(watch) => watch.begin(giving).map(Outcome::Watch),
            Process::Subscribe(subscribe) => subscribe.begin(giving).map(|()| Outcome::Subscribe),
            Process::Configure(configure) => configure.begin(giving).map(Outcome::Configure),
            Process::Prepare(prepare) => prepare.begin(giving).map(Outcome::Prepare),
        }
    }

    fn take<N: Giving<Gift>>(&mut self, taken: Taken, giving: N) -> Option<Outcome> {
        match (self, taken) {
            (Process::Report(report), Taken::Answer(answer)) => report.take(ReportTaken::Answer(answer), giving).map(Outcome::Report),
            (Process::Report(report), Taken::Observation(observation)) => report.take(ReportTaken::Observation(observation), giving).map(Outcome::Report),
            (Process::Watch(watch), Taken::Following(following)) => watch.take(WatchTaken::Following(following), giving).map(Outcome::Watch),
            (Process::Watch(watch), Taken::Observation(observation)) => watch.take(WatchTaken::Observation(observation), giving).map(Outcome::Watch),
            (Process::Subscribe(subscribe), Taken::Recollection(recollection)) => subscribe.take(recollection, giving).map(|()| Outcome::Subscribe),
            (Process::Configure(configure), Taken::Answer(answer)) => configure.take(answer, giving).map(Outcome::Configure),
            (Process::Prepare(prepare), Taken::Installation(installation)) => prepare.take(installation, giving).map(Outcome::Prepare),
            _ => None,
        }
    }
}
