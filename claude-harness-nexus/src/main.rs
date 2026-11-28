//! The Claude Harness Nexus: the process manager on its queue, and the world it
//! traces into. The signal process holds the sockets, the memory process the
//! disk, and an emission is carried to the translator on a connection of its own.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::BufReader;
use std::os::unix::net::{UnixListener, UnixStream};
use std::process::ExitCode;
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

use claude_harness_process::*;
use flow::FlowId;
use harness::{Event, FilePath, Observed, State, Tokens, Transitionable};
use process::{Answer, Arrival, Caller, Ended, Gave, Given, Identity, Living, Moment, Ownable, Owner, ProcessId, ProcessManager, Roots, Trace, Traced, Tracing, Unobserved, Unremembered, UserId};
use transport::{Framed, Place, Placed};

/// What the manager's queue carries: an arrival for the manager; the library's
/// announcement that the translator has begun to listen; a Prepare and its
/// caller, whose process is born with the configuration Memory holds; or the
/// news that the Watches born with an earlier sessions directory have ended,
/// to be born again with the one configured now.
enum Arrived {
    Arrival(Arrival<Process>),
    Listening,
    Preparing(Identity, Caller),
    Reconfigured,
}

type Queue = Sender<Arrived>;

/// What the memory holds, archived whole as its file: each session observed,
/// each Watch, and the configuration its Keeps have come to.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct Records {
    observed_vector: Vec<Observed>,
    watch_vector: Vec<Watch>,
    hooks: Hooks,
    hook_client_option: Option<HookClient>,
    row_count_option: Option<RowCount>,
    transcript_root_option: Option<FilePath>,
    session_root_option: Option<SessionRoot>,
}

/// The memory process: one change at a time, each kept before it is answered.
struct MemoryProcess {
    records: Records,
    file: std::path::PathBuf,
}

impl MemoryProcess {
    /// A memory file that is there and cannot be read is not overwritten: the program does not start.
    fn open(place: &Place) -> std::io::Result<MemoryProcess> {
        let file = place.path(Placed::Memory(VERSION));
        let empty = || Records { observed_vector: Vec::new(), watch_vector: Vec::new(), hooks: Hooks(Vec::new()), hook_client_option: None, row_count_option: None, transcript_root_option: None, session_root_option: None };
        let records = match File::open(&file) {
            Ok(opened) => Records::read_from(&mut BufReader::new(opened))?.unwrap_or_else(empty),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => empty(),
            Err(error) => return Err(error),
        };
        Ok(MemoryProcess { records, file })
    }

    fn keep(&self) -> Answer {
        let temporary = self.file.with_extension("next");
        let written = File::create(&temporary)
            .and_then(|mut file| self.records.write_to(&mut file))
            .and_then(|()| std::fs::rename(&temporary, &self.file));
        match written {
            Ok(()) => Answer::Succeeded,
            Err(_) => Answer::Failed,
        }
    }

    fn holds_watch(&self, flow_id: &FlowId) -> bool {
        self.records.watch_vector.iter().any(|watch| watch.flow_id == *flow_id)
    }

    /// A Watch's process, when the row count it is born with is configured,
    /// born with the sessions directory configured, if one is.
    fn process_of(&self, watch: &Watch) -> Option<Process> {
        let row_count = self.records.row_count_option?;
        let session_root_option = self.records.session_root_option.clone();
        Some(Process::Watch(WatchProcess { flow_id: watch.flow_id.clone(), file_path: watch.file_path.clone(), read_from: watch.read_from.clone(), row_count, session_root_option }))
    }

    /// The discovery's process, when the transcript root it is born with is
    /// configured, with the sessions directory configured, if one is.
    fn discover_process(&self) -> Option<DiscoverProcess> {
        let file_path = self.records.transcript_root_option.clone()?;
        Some(DiscoverProcess { file_path, session_root_option: self.records.session_root_option.clone() })
    }

    /// A Prepare's process, with its caller, and the hook client and the hooks as configured now.
    fn prepare_process(&self, caller: Caller) -> Process {
        Process::Prepare(PrepareProcess { caller, hook_client_option: self.records.hook_client_option.clone(), hooks: self.records.hooks.clone() })
    }

    /// A Watch of the same flow and file is kept as it is, with the offset read so
    /// far: a session discovered and named again by its SessionStart is one Watch.
    fn watch(&mut self, watch: Watch) -> Answer {
        if self.records.watch_vector.iter().any(|held| held.flow_id == watch.flow_id && held.file_path == watch.file_path) {
            return Answer::Succeeded;
        }
        self.records.watch_vector.retain(|held| held.flow_id != watch.flow_id);
        self.records.watch_vector.push(watch);
        self.keep()
    }

    fn configure(&mut self, Keep(set): Keep) -> Answer {
        match set {
            Set::Hooks(hooks) => self.records.hooks = hooks,
            Set::RemoveHook(hook_name) => self.records.hooks.0.retain(|held| *held != hook_name),
            Set::HookClient(hook_client) => self.records.hook_client_option = Some(hook_client),
            Set::RowCount(row_count) => self.records.row_count_option = Some(row_count),
            Set::TranscriptRoot(file_path) => self.records.transcript_root_option = Some(file_path),
            Set::SessionRoot(session_root) => self.records.session_root_option = Some(session_root),
        }
        self.keep()
    }

    /// Applies each event and keeps the offset. What a Watch observes applies
    /// only while its Watch is kept. A session that has ended keeps its
    /// Observed, Ended, and its Watch, since a resume writes on in the same
    /// transcript; a second end is no change, as a second TurnEnded is not.
    fn observe(&mut self, observe: Observe) -> Result<Observation, Unremembered> {
        if observe.offset_option.is_some() && !self.holds_watch(&observe.flow_id) {
            return Ok(Observation(Vec::new()));
        }
        let position = match self.records.observed_vector.iter().position(|observed| observed.flow_id == observe.flow_id) {
            Some(position) => position,
            None => {
                self.records.observed_vector.push(Observed { flow_id: observe.flow_id.clone(), state: State::Dry, tokens: Tokens(0), background_work_vector: vec![] });
                self.records.observed_vector.len() - 1
            }
        };
        let observed = &mut self.records.observed_vector[position];
        let changed = observe
            .event_vector
            .into_iter()
            .filter_map(|event| {
                let before = observed.state.clone();
                observed.after(event.clone());
                let witnessed = matches!(event, Event::PromptSubmitted | Event::TurnEnded | Event::Ended(_)) && observed.state == before;
                (!witnessed).then(|| Changed { flow_id: observe.flow_id.clone(), event, state: observed.state.clone() })
            })
            .collect();
        if let Some(offset) = observe.offset_option {
            for watch in self.records.watch_vector.iter_mut().filter(|watch| watch.flow_id == observe.flow_id) {
                watch.read_from = ReadFrom::Offset(offset);
            }
        }
        match self.keep() {
            Answer::Succeeded => Ok(Observation(changed)),
            Answer::Failed => Err(Unremembered {}),
        }
    }
}

/// A subscriber's connection holds what changed until its Subscribed is written.
struct Subscription {
    pending: Vec<signal_harness::Output>,
    answered: bool,
}

/// The signal process: what enters the program and what leaves it.
struct SignalProcess {
    arrived: Arc<Mutex<BTreeMap<Identity, UnixStream>>>,
    connections: BTreeMap<Identity, UnixStream>,
    subscriptions: BTreeMap<Identity, Subscription>,
    queue: Queue,
}

/// What a call becomes: the process it is born as, the library's Listening, or
/// a Prepare, born where Memory is.
enum Called {
    Born(Process),
    Listening,
    Preparing(Caller),
}

/// A peer that speaks a contract directly, its caller from its credentials.
fn peer(stream: &UnixStream) -> Option<Caller> {
    let credentials = rustix::net::sockopt::socket_peercred(stream).ok()?;
    Some(Caller { process_id: ProcessId(credentials.pid.as_raw_nonzero().get().into()), user_id: UserId(credentials.uid.as_raw().into()) })
}

impl SignalProcess {
    /// Each socket's calls become births: the identity is drawn here, and the
    /// call's connection is held under it until its process is born.
    fn listen<C, F>(place: &Place, placed: Placed, roots: Arc<Roots>, arrived: Arc<Mutex<BTreeMap<Identity, UnixStream>>>, queue: Queue, called: F) -> std::io::Result<()>
    where
        C: Framed + Send + 'static,
        F: Fn(C, Caller) -> Called + Send + 'static,
    {
        let path = place.path(placed);
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path)?;
        thread::spawn(move || {
            for mut stream in listener.incoming().flatten() {
                let (Ok(Some(call)), Some(caller)) = (C::read_from(&mut stream), peer(&stream)) else { continue };
                let arrival = match called(call, caller) {
                    Called::Listening => {
                        let _ = Output::Acknowledged.write_to(&mut stream);
                        Arrived::Listening
                    }
                    Called::Born(process) => {
                        let identity = roots.draw();
                        arrived.lock().expect("the arrived connections").insert(identity.clone(), stream);
                        Arrived::Arrival(Arrival::Born(Living { identity, process }))
                    }
                    Called::Preparing(caller) => {
                        let identity = roots.draw();
                        arrived.lock().expect("the arrived connections").insert(identity.clone(), stream);
                        Arrived::Preparing(identity, caller)
                    }
                };
                if queue.send(arrival).is_err() {
                    return;
                }
            }
        });
        Ok(())
    }

    fn start(place: &Place, roots: Arc<Roots>, queue: Queue) -> std::io::Result<SignalProcess> {
        let arrived = Arc::new(Mutex::new(BTreeMap::new()));
        SignalProcess::listen(place, Placed::NexusSocket, roots.clone(), arrived.clone(), queue.clone(), |call: Call, _| match call.input {
            Input::Report(hook) => Called::Born(Process::Report(ReportProcess { caller: call.caller, hook })),
            Input::Listening => Called::Listening,
        })?;
        SignalProcess::listen(place, Placed::SubscriptionSocket, roots.clone(), arrived.clone(), queue.clone(), |input: signal_harness::Input, caller| match input {
            signal_harness::Input::Subscribe => Called::Born(Process::Subscribe(SubscribeProcess { caller })),
        })?;
        SignalProcess::listen(place, Placed::PrepareSocket, roots.clone(), arrived.clone(), queue.clone(), |input: signal_claude_prepare::Input, caller| match input {
            signal_claude_prepare::Input::Prepare => Called::Preparing(caller),
        })?;
        SignalProcess::listen(place, Placed::MetaSocket, roots, arrived.clone(), queue.clone(), |call: MetaCall, _| match call.meta_input {
            meta::Input::Configure(set) => Called::Born(Process::Configure(ConfigureProcess { caller: call.caller, set })),
        })?;
        Ok(SignalProcess { arrived, connections: BTreeMap::new(), subscriptions: BTreeMap::new(), queue })
    }

    /// A call's process takes the connection held under its identity. A
    /// subscriber's process ends when its connection closes.
    fn bind(&mut self, living: &Living<Process>) {
        if living.process.owner() != Owner::Signal {
            return;
        }
        let Some(stream) = self.arrived.lock().expect("the arrived connections").remove(&living.identity) else { return };
        if let (Process::Subscribe(_), Ok(mut closing)) = (&living.process, stream.try_clone()) {
            let identity = living.identity.clone();
            let queue = self.queue.clone();
            thread::spawn(move || {
                let _ = std::io::copy(&mut closing, &mut std::io::sink());
                let _ = queue.send(Arrived::Arrival(Arrival::Ended(Ended { identity, outcome: Outcome::Subscribe })));
            });
        }
        self.connections.insert(living.identity.clone(), stream);
    }

    fn answer<T: Framed>(&mut self, identity: &Identity, output: &T) {
        if let Some(stream) = self.connections.get_mut(identity) {
            let _ = output.write_to(stream);
        }
    }

    /// From the moment its recollection is taken, a subscriber misses nothing.
    fn subscribe(&mut self, identity: &Identity) {
        self.subscriptions.insert(identity.clone(), Subscription { pending: Vec::new(), answered: false });
    }

    fn subscribed(&mut self, identity: &Identity, output: signal_harness::Output) {
        self.answer(identity, &output);
        let pending = match self.subscriptions.get_mut(identity) {
            Some(subscription) => {
                subscription.answered = true;
                std::mem::take(&mut subscription.pending)
            }
            None => Vec::new(),
        };
        for output in pending {
            self.answer(identity, &output);
        }
    }

    fn publish(&mut self, output: signal_harness::Output) {
        for (identity, subscription) in self.subscriptions.iter_mut() {
            match (subscription.answered, self.connections.get_mut(identity)) {
                (true, Some(stream)) => {
                    let _ = output.write_to(stream);
                }
                _ => subscription.pending.push(output.clone()),
            }
        }
    }

    fn close(&mut self, identity: &Identity) {
        self.subscriptions.remove(identity);
        if let Some(stream) = self.connections.remove(identity) {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
    }
}

/// The emissions on their way to the translator, each on its own connection.
struct EmissionSocket {
    place: Place,
    lasting: BTreeMap<Identity, UnixStream>,
}

impl EmissionSocket {
    /// One emission, one connection, one outcome; read beside the manager, so it waits for nothing.
    fn install(&mut self, identity: Identity, install: Install_Data, queue: Queue) {
        let given = move |identity: Identity, taken| Arrived::Arrival(Arrival::Given(Given { identity, taken: Taken::Installation(taken) }));
        let Ok(mut stream) = UnixStream::connect(self.place.path(Placed::EmissionSocket)) else {
            let _ = queue.send(given(identity, Err(Unobserved::Unsubmitted)));
            return;
        };
        if Emission::Install(install).write_to(&mut stream).is_err() {
            let _ = queue.send(given(identity, Err(Unobserved::Unsubmitted)));
            return;
        }
        thread::spawn(move || {
            let installation = match Installation_Data::read_from(&mut stream) {
                Ok(Some(installation)) => Ok(installation),
                _ => Err(Unobserved::Unanswered),
            };
            let _ = queue.send(given(identity, installation));
        });
    }

    /// A lasting emission: each outcome read is given to its process as it
    /// comes, until the connection ends, which is given as Unanswered.
    fn last<O: Framed + Send + 'static>(&mut self, identity: Identity, emission: Emission, queue: Queue, taken: fn(Result<O, Unobserved>) -> Taken) {
        let given = move |identity: &Identity, outcome| Arrived::Arrival(Arrival::Given(Given { identity: identity.clone(), taken: taken(outcome) }));
        let Ok(mut stream) = UnixStream::connect(self.place.path(Placed::EmissionSocket)) else {
            let _ = queue.send(given(&identity, Err(Unobserved::Unsubmitted)));
            return;
        };
        if emission.write_to(&mut stream).is_err() {
            let _ = queue.send(given(&identity, Err(Unobserved::Unsubmitted)));
            return;
        }
        let Ok(mut reading) = stream.try_clone() else {
            let _ = queue.send(given(&identity, Err(Unobserved::Unanswered)));
            return;
        };
        self.lasting.insert(identity.clone(), stream);
        thread::spawn(move || {
            loop {
                match O::read_from(&mut reading) {
                    Ok(Some(outcome)) => {
                        if queue.send(given(&identity, Ok(outcome))).is_err() {
                            return;
                        }
                    }
                    _ => {
                        let _ = queue.send(given(&identity, Err(Unobserved::Unanswered)));
                        return;
                    }
                }
            }
        });
    }

    fn close(&mut self, identity: &Identity) {
        if let Some(stream) = self.lasting.remove(identity) {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
    }
}

/// Everything the manager does passes here: it is archived, and carried.
struct World {
    signal: SignalProcess,
    memory: MemoryProcess,
    emission: EmissionSocket,
    archive: File,
    queue: Queue,
    roots: Arc<Roots>,
    /// Each Watch that has a living process, or one on its way to birth, by its flow.
    watching: BTreeMap<FlowId, Identity>,
    /// The one discovery that has a living process, or one on its way to birth, and its root.
    discovering: Option<(DiscoverProcess, Identity)>,
}

impl World {
    fn give_back(&self, identity: Identity, taken: Taken) {
        let _ = self.queue.send(Arrived::Arrival(Arrival::Given(Given { identity, taken })));
    }

    fn end(&self, identity: Identity, outcome: Outcome) {
        let _ = self.queue.send(Arrived::Arrival(Arrival::Ended(Ended { identity, outcome })));
    }

    /// Every Watch kept and without a living process is given its process, once
    /// its row count is configured: at the program's start, when the translator
    /// begins to listen, and when a Watch or the configuration is kept.
    fn bear_watches(&mut self) {
        let unborn: Vec<Process> = self
            .memory
            .records
            .watch_vector
            .iter()
            .filter(|watch| !self.watching.contains_key(&watch.flow_id))
            .filter_map(|watch| self.memory.process_of(watch))
            .collect();
        for process in unborn {
            let identity = self.roots.draw();
            if let Process::Watch(watch) = &process {
                self.watching.insert(watch.flow_id.clone(), identity.clone());
            }
            let _ = self.queue.send(Arrived::Arrival(Arrival::Born(Living { identity, process })));
        }
    }

    /// The discovery of the transcript root configured is given its process
    /// where it has none: at the program's start, when the translator begins to
    /// listen, and when the configuration is kept. A root or sessions directory
    /// replaced ends the discovery born with the old one.
    fn bear_discovery(&mut self) {
        let configured = self.memory.discover_process();
        if let Some((discover, identity)) = &self.discovering {
            if Some(discover) == configured.as_ref() {
                return;
            }
            self.end(identity.clone(), Outcome::Discover(Discovering::Forgotten));
            self.discovering = None;
        }
        let Some(discover) = configured else { return };
        let identity = self.roots.draw();
        self.discovering = Some((discover.clone(), identity.clone()));
        let _ = self.queue.send(Arrived::Arrival(Arrival::Born(Living { identity, process: Process::Discover(discover) })));
    }

    /// A Watch forgotten ends its process.
    fn forget(&mut self) {
        let forgotten: Vec<Identity> = self.watching.iter().filter(|(flow_id, _)| !self.memory.holds_watch(flow_id)).map(|(_, identity)| identity.clone()).collect();
        for identity in forgotten {
            self.end(identity, Outcome::Watch(Watching::Forgotten));
        }
    }

    /// A sessions directory configured anew ends each living Watch, and the
    /// queue, past their ends, bears them again with it.
    fn reconfigure(&mut self) {
        for identity in self.watching.values() {
            self.end(identity.clone(), Outcome::Watch(Watching::Reconfigured));
        }
        let _ = self.queue.send(Arrived::Reconfigured);
    }

    fn born(&mut self, living: &Living<Process>) {
        self.signal.bind(living);
        if let Process::Watch(watch) = &living.process
            && !self.memory.holds_watch(&watch.flow_id)
        {
            self.end(living.identity.clone(), Outcome::Watch(Watching::Forgotten));
        }
    }

    fn carry(&mut self, Gave { identity, gift }: Gave<Gift>) {
        match gift {
            Gift::Reported => self.signal.answer(&identity, &Output::Reported),
            Gift::Subscribed(Subscribed(observed_vector)) => self.signal.subscribed(&identity, signal_harness::Output::Subscribed(observed_vector)),
            Gift::Changed(Changed { flow_id, event, state }) => self.signal.publish(signal_harness::Output::Changed(signal_harness::Changed { flow_id, event, state })),
            Gift::Prepared(Prepared(file_path_option)) => self.signal.answer(&identity, &signal_claude_prepare::Output::Prepared(file_path_option)),
            Gift::Install(install) => self.emission.install(identity, install, self.queue.clone()),
            Gift::Configured(Configured(set)) => self.signal.answer(&identity, &meta::Output::Configured(set)),
            Gift::Recall(Recall {}) => {
                self.signal.subscribe(&identity);
                let recollection = Recollection(self.memory.records.observed_vector.clone());
                self.give_back(identity, Taken::Recollection(Ok(recollection)));
            }
            Gift::Watch(watch) => {
                let answer = self.memory.watch(watch);
                self.give_back(identity, Taken::Answer(answer));
                self.bear_watches();
            }
            Gift::Keep(keep) => {
                let session_root = self.memory.records.session_root_option.clone();
                let answer = self.memory.configure(keep);
                if self.memory.records.session_root_option != session_root {
                    self.reconfigure();
                }
                self.give_back(identity, Taken::Answer(answer));
                self.bear_watches();
                self.bear_discovery();
            }
            Gift::Observe(observe) => {
                let observation = self.memory.observe(observe);
                self.give_back(identity, Taken::Observation(observation));
                self.forget();
            }
            Gift::Follow(follow) => self.emission.last(identity, Emission::Follow(follow), self.queue.clone(), Taken::Following),
            Gift::Discover(discover) => self.emission.last(identity, Emission::Discover(discover), self.queue.clone(), Taken::Discovered),
        }
    }
}

impl Tracing<Process> for World {
    fn trace(&mut self, trace: Traced<Process>) {
        let _ = trace.write_to(&mut self.archive);
        match trace {
            Trace::Started(_) => {}
            Trace::Born(living) => self.born(&living),
            Trace::Gave(gave) => self.carry(gave),
            Trace::Took(_) => {}
            Trace::Ended(ended) => {
                self.watching.retain(|_, identity| *identity != ended.identity);
                if self.discovering.as_ref().is_some_and(|(_, identity)| *identity == ended.identity) {
                    self.discovering = None;
                }
                self.signal.close(&ended.identity);
                self.emission.close(&ended.identity);
            }
        }
    }
}

fn main() -> ExitCode {
    let place = match Place::from_arguments() {
        Ok(place) => place,
        Err(unplaced) => {
            eprintln!("{unplaced:?}");
            return ExitCode::from(2);
        }
    };
    let (queue, arrivals) = mpsc::channel();
    let roots = Arc::new(Roots::default());
    let started = place.prepare().and_then(|()| {
        let archive = OpenOptions::new().create(true).append(true).open(place.path(Placed::Trace(VERSION)))?;
        let signal = SignalProcess::start(&place, roots.clone(), queue.clone())?;
        Ok((archive, signal))
    });
    let (archive, signal) = match started {
        Ok(started) => started,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    let memory = match MemoryProcess::open(&place) {
        Ok(memory) => memory,
        Err(error) => {
            eprintln!("the memory cannot be read: {error}");
            return ExitCode::FAILURE;
        }
    };
    let emission = EmissionSocket { place: place.clone(), lasting: BTreeMap::new() };
    let mut world = World { signal, memory, emission, archive, queue, roots, watching: BTreeMap::new(), discovering: None };
    world.trace(Trace::Started(now()));
    world.bear_watches();
    world.bear_discovery();
    let mut manager = ProcessManager::default();
    for arrived in arrivals {
        match arrived {
            Arrived::Arrival(arrival) => manager.arrive(arrival, &mut world),
            Arrived::Listening => {
                world.bear_watches();
                world.bear_discovery();
            }
            Arrived::Reconfigured => world.bear_watches(),
            Arrived::Preparing(identity, caller) => {
                let process = world.memory.prepare_process(caller);
                manager.arrive(Arrival::Born(Living { identity, process }), &mut world)
            }
        }
    }
    manager.end_all(&mut world);
    ExitCode::SUCCESS
}

fn now() -> Moment {
    let since = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    Moment(i64::try_from(since.as_nanos()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness::{EndReason, Flow};

    const FLOW: &str = "5e7d1c";

    fn memory(name: &str) -> MemoryProcess {
        let directory = std::env::temp_dir().join(format!("claude-harness-memory-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&directory).expect("the directory");
        let records = Records { observed_vector: Vec::new(), watch_vector: Vec::new(), hooks: Hooks(Vec::new()), hook_client_option: None, row_count_option: Some(RowCount(64)), transcript_root_option: None, session_root_option: None };
        let mut memory = MemoryProcess { records, file: directory.join("memory.signal") };
        let watch = Watch { flow_id: FlowId(FLOW.into()), file_path: FilePath(format!("/t/{FLOW}.jsonl")), read_from: ReadFrom::Offset(Offset(0)) };
        assert_eq!(memory.watch(watch), Answer::Succeeded);
        memory
    }

    fn observe(memory: &mut MemoryProcess, events: Vec<Event>, offset: i64) -> Vec<Changed> {
        memory.observe(Observe { flow_id: FlowId(FLOW.into()), event_vector: events, offset_option: Some(Offset(offset)) }).expect("an observation").0
    }

    fn changed(event: Event, state: State) -> Changed {
        Changed { flow_id: FlowId(FLOW.into()), event, state }
    }

    #[test]
    fn an_ended_session_keeps_its_watch_and_a_second_end_is_no_change() {
        let mut memory = memory("ended");
        assert_eq!(observe(&mut memory, vec![Event::PromptSubmitted, Event::TurnEnded, Event::Ended(EndReason::Died)], 120), vec![
            changed(Event::PromptSubmitted, State::Flowing(Flow::Working)),
            changed(Event::TurnEnded, State::Flowing(Flow::Rested)),
            changed(Event::Ended(EndReason::Died), State::Ended),
        ]);
        assert!(memory.holds_watch(&FlowId(FLOW.into())));
        assert_eq!(memory.records.watch_vector[0].read_from, ReadFrom::Offset(Offset(120)));
        assert_eq!(memory.records.observed_vector[0].state, State::Ended);
        let hooked = memory.observe(Observe { flow_id: FlowId(FLOW.into()), event_vector: vec![Event::Ended(EndReason::Exited)], offset_option: None }).expect("an observation").0;
        assert!(hooked.is_empty());
    }

    #[test]
    fn a_resumed_session_works_again() {
        let mut memory = memory("resumed");
        observe(&mut memory, vec![Event::Ended(EndReason::Died)], 0);
        assert_eq!(observe(&mut memory, vec![Event::PromptSubmitted], 90), vec![changed(Event::PromptSubmitted, State::Flowing(Flow::Working))]);
    }

    #[test]
    fn a_watch_is_born_with_the_sessions_directory_configured() {
        let mut memory = memory("configured");
        let session_root = SessionRoot(FilePath("/home/li/.claude/sessions".into()));
        assert_eq!(memory.configure(Keep(Set::SessionRoot(session_root.clone()))), Answer::Succeeded);
        let Some(Process::Watch(watch)) = memory.process_of(&memory.records.watch_vector[0].clone()) else { panic!("a Watch's process") };
        assert_eq!(watch.session_root_option, Some(session_root));
    }
}
