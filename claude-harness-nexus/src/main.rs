//! The Claude Harness Nexus: the process manager on its queue, and the world it
//! traces into. The signal process holds the sockets, the memory process the
//! disk, and an emission is carried to the translator on a connection of its own.

use std::collections::{BTreeMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::BufReader;
use std::os::unix::net::{UnixListener, UnixStream};
use std::process::ExitCode;
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

use claude_harness_process::*;
use flow::FlowId;
use harness::{Observed, State, Tokens, Transitionable};
use process::{Arrival, Ended, Gave, Given, Identity, Living, ProcessManager, Trace, Traced, Tracing};
use transport::{Framed, Place, Placed};

type Queue = Sender<Arrival<Process>>;

/// The memory process: one change at a time, each kept before it is answered.
struct MemoryProcess {
    remembered: Remembered,
    file: std::path::PathBuf,
}

impl MemoryProcess {
    fn open(place: &Place) -> MemoryProcess {
        let file = place.path(Placed::Memory(VERSION));
        let remembered = File::open(&file)
            .ok()
            .and_then(|file| Remembered::read_from(&mut BufReader::new(file)).ok().flatten())
            .unwrap_or(Remembered { observed_vector: Vec::new(), watch_vector: Vec::new(), count_option: None });
        let mut memory = MemoryProcess { remembered, file };
        memory.forget_ended();
        memory
    }

    fn keep(&self) -> Answer {
        let temporary = self.file.with_extension("next");
        let written = File::create(&temporary)
            .and_then(|mut file| self.remembered.write_to(&mut file))
            .and_then(|()| std::fs::rename(&temporary, &self.file));
        match written {
            Ok(()) => Answer::Succeeded,
            Err(_) => Answer::Failed,
        }
    }

    /// A Watch outlives no session: one whose session has ended is forgotten.
    fn forget_ended(&mut self) {
        let observed = &self.remembered.observed_vector;
        self.remembered
            .watch_vector
            .retain(|watch| !observed.iter().any(|observed| observed.flow_id == watch.flow_id && observed.state == State::Ended));
    }

    fn holds_watch(&self, flow_id: &FlowId) -> bool {
        self.remembered.watch_vector.iter().any(|watch| watch.flow_id == *flow_id)
    }

    /// A Watch's process, when the count it is born with is configured.
    fn process_of(&self, watch: &Watch) -> Option<Process> {
        let count = self.remembered.count_option.clone()?;
        Some(Process::Watch(WatchProcess { flow_id: watch.flow_id.clone(), file_path: watch.file_path.clone(), offset: watch.offset.clone(), count }))
    }

    fn watch(&mut self, watch: Watch) -> Answer {
        if self.remembered.watch_vector.contains(&watch) {
            return Answer::Succeeded;
        }
        self.remembered.watch_vector.retain(|held| held.flow_id != watch.flow_id);
        self.remembered.watch_vector.push(watch);
        self.keep()
    }

    fn configure(&mut self, Configure(set): Configure) -> Answer {
        match set {
            Set::Count(count) => self.remembered.count_option = Some(count),
        }
        self.keep()
    }

    /// Applies each event and keeps the offset; a session that has ended forgets its Watch.
    fn observe(&mut self, observe: Observe) -> Observation {
        let position = match self.remembered.observed_vector.iter().position(|observed| observed.flow_id == observe.flow_id) {
            Some(position) => position,
            None => {
                self.remembered.observed_vector.push(Observed { flow_id: observe.flow_id.clone(), state: State::Dry, tokens: Tokens(0), background_work_vector: vec![] });
                self.remembered.observed_vector.len() - 1
            }
        };
        let observed = &mut self.remembered.observed_vector[position];
        let changed = observe
            .event_vector
            .into_iter()
            .map(|event| {
                observed.after(event.clone());
                Changed { flow_id: observe.flow_id.clone(), event, state: observed.state.clone() }
            })
            .collect();
        if let Some(offset) = observe.offset_option {
            for watch in self.remembered.watch_vector.iter_mut().filter(|watch| watch.flow_id == observe.flow_id) {
                watch.offset = offset.clone();
            }
        }
        self.forget_ended();
        self.keep();
        Observation(changed)
    }
}

/// A subscriber's connection holds what changed until its Subscribed is written.
struct Subscription {
    pending: Vec<Output>,
    answered: bool,
}

/// The signal process: what enters the program and what leaves it.
struct SignalProcess {
    arrived: Arc<Mutex<VecDeque<UnixStream>>>,
    connections: BTreeMap<Identity, UnixStream>,
    subscriptions: BTreeMap<Identity, Subscription>,
    queue: Queue,
}

impl SignalProcess {
    /// Each socket's calls become births; a call's connection waits, in the
    /// order of the births, for the process it is bound to.
    fn listen<C, F>(place: &Place, placed: Placed, arrived: Arc<Mutex<VecDeque<UnixStream>>>, queue: Queue, born: F) -> std::io::Result<()>
    where
        C: Framed + Send + 'static,
        F: Fn(C) -> Process + Send + 'static,
    {
        let path = place.path(placed);
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path)?;
        thread::spawn(move || {
            for mut stream in listener.incoming().flatten() {
                let Ok(Some(call)) = C::read_from(&mut stream) else { continue };
                let mut waiting = arrived.lock().expect("the arrived connections");
                waiting.push_back(stream);
                if queue.send(Arrival::Born { owner: None, process: born(call) }).is_err() {
                    return;
                }
            }
        });
        Ok(())
    }

    fn start(place: &Place, queue: Queue) -> std::io::Result<SignalProcess> {
        let arrived = Arc::new(Mutex::new(VecDeque::new()));
        SignalProcess::listen(place, Placed::NexusSocket, arrived.clone(), queue.clone(), |call: Call| match call.input {
            Input::Report(hook) => Process::Report(ReportProcess { caller: call.caller, hook }),
            Input::Subscribe => Process::Subscribe(SubscribeProcess { caller: call.caller }),
        })?;
        SignalProcess::listen(place, Placed::MetaSocket, arrived.clone(), queue.clone(), |call: MetaCall| match call.meta_input {
            MetaInput::Configure(set) => Process::Configure(ConfigureProcess { caller: call.caller, set }),
        })?;
        Ok(SignalProcess { arrived, connections: BTreeMap::new(), subscriptions: BTreeMap::new(), queue })
    }

    /// A call's process takes the connection it came on. A subscriber's
    /// process ends when its connection closes.
    fn bind(&mut self, living: &Living<Process>) {
        if let Process::Watch(_) = living.process {
            return;
        }
        let Some(stream) = self.arrived.lock().expect("the arrived connections").pop_front() else { return };
        if let (Process::Subscribe(_), Ok(mut closing)) = (&living.process, stream.try_clone()) {
            let identity = living.identity.clone();
            let queue = self.queue.clone();
            thread::spawn(move || {
                let _ = std::io::copy(&mut closing, &mut std::io::sink());
                let _ = queue.send(Arrival::Ended(Ended { identity, outcome: Outcome::Subscribing(Subscribing::Closed) }));
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

    fn subscribed(&mut self, identity: &Identity, output: Output) {
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

    fn publish(&mut self, output: Output) {
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
    fn follow(&mut self, identity: Identity, follow: Follow, queue: Queue) {
        let given = move |identity: &Identity, taken| Arrival::Given(Given { identity: identity.clone(), taken: Taken::Following(taken) });
        let Ok(mut stream) = UnixStream::connect(self.place.path(Placed::EmissionSocket)) else {
            let _ = queue.send(given(&identity, Err(Unobserved::Unsubmitted)));
            return;
        };
        if follow.write_to(&mut stream).is_err() {
            let _ = queue.send(given(&identity, Err(Unobserved::Unsubmitted)));
            return;
        }
        let Ok(mut reading) = stream.try_clone() else { return };
        self.lasting.insert(identity.clone(), stream);
        thread::spawn(move || {
            loop {
                match Following::read_from(&mut reading) {
                    Ok(Some(following)) => {
                        if queue.send(given(&identity, Ok(following))).is_err() {
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
    /// Each Watch that has a process, or one on its way to birth, by its flow.
    watching: Vec<(FlowId, Option<Identity>)>,
}

impl World {
    fn give_back(&self, identity: Identity, taken: Taken) {
        let _ = self.queue.send(Arrival::Given(Given { identity, taken }));
    }

    fn end(&self, identity: Identity, outcome: Outcome) {
        let _ = self.queue.send(Arrival::Ended(Ended { identity, outcome }));
    }

    /// Every Watch kept and without a process is born, once its count is configured.
    fn bear_watches(&mut self) {
        let unborn: Vec<Process> = self
            .memory
            .remembered
            .watch_vector
            .iter()
            .filter(|watch| !self.watching.iter().any(|(flow_id, _)| *flow_id == watch.flow_id))
            .filter_map(|watch| self.memory.process_of(watch))
            .collect();
        for process in unborn {
            if let Process::Watch(watch) = &process {
                self.watching.push((watch.flow_id.clone(), None));
            }
            let _ = self.queue.send(Arrival::Born { owner: None, process });
        }
    }

    /// A Watch forgotten ends its process.
    fn forget(&mut self) {
        let forgotten: Vec<Identity> = self
            .watching
            .iter()
            .filter(|(flow_id, _)| !self.memory.holds_watch(flow_id))
            .filter_map(|(_, identity)| identity.clone())
            .collect();
        for identity in forgotten {
            self.end(identity, Outcome::Watching(Watching::Forgotten));
        }
    }

    fn born(&mut self, living: &Living<Process>) {
        self.signal.bind(living);
        if let Process::Watch(watch) = &living.process {
            self.watching.retain(|(flow_id, _)| *flow_id != watch.flow_id);
            self.watching.push((watch.flow_id.clone(), Some(living.identity.clone())));
            if !self.memory.holds_watch(&watch.flow_id) {
                self.end(living.identity.clone(), Outcome::Watching(Watching::Forgotten));
            }
        }
    }

    fn carry(&mut self, Gave { identity, gift }: Gave<Gift>) {
        match gift {
            Gift::Reported(reported) => self.signal.answer(&identity, &Output::Reported(reported)),
            Gift::Subscribed(subscribed) => self.signal.subscribed(&identity, Output::Subscribed(subscribed)),
            Gift::Changed(changed) => self.signal.publish(Output::Changed(changed)),
            Gift::Configured(Configured(set)) => self.signal.answer(&identity, &MetaOutput::Configured(set)),
            Gift::Recall(Recall) => {
                self.signal.subscribe(&identity);
                let recollection = Recollection(self.memory.remembered.observed_vector.clone());
                self.give_back(identity, Taken::Recollection(recollection));
            }
            Gift::Watch(watch) => {
                let answer = self.memory.watch(watch);
                self.give_back(identity, Taken::Answer(answer));
                self.bear_watches();
            }
            Gift::Configure(configure) => {
                let answer = self.memory.configure(configure);
                self.give_back(identity, Taken::Answer(answer));
                self.bear_watches();
            }
            Gift::Observe(observe) => {
                let observation = self.memory.observe(observe);
                self.give_back(identity, Taken::Observation(observation));
                self.forget();
            }
            Gift::Follow(follow) => self.emission.follow(identity, follow, self.queue.clone()),
        }
    }
}

impl Tracing<Process> for World {
    fn trace(&mut self, trace: Traced<Process>) {
        let _ = trace.write_to(&mut self.archive);
        match trace {
            Trace::Born(living) => self.born(&living),
            Trace::Gave(gave) => self.carry(gave),
            Trace::Took(_) => {}
            Trace::Ended(ended) => {
                self.watching.retain(|(_, identity)| identity.as_ref() != Some(&ended.identity));
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
    let started = place.prepare().and_then(|()| {
        let archive = OpenOptions::new().create(true).append(true).open(place.path(Placed::Trace(VERSION)))?;
        let signal = SignalProcess::start(&place, queue.clone())?;
        Ok((archive, signal))
    });
    let (archive, signal) = match started {
        Ok(started) => started,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    let memory = MemoryProcess::open(&place);
    let emission = EmissionSocket { place: place.clone(), lasting: BTreeMap::new() };
    let mut world = World { signal, memory, emission, archive, queue, watching: Vec::new() };
    world.bear_watches();
    ProcessManager::default().run(arrivals, &mut world);
    ExitCode::SUCCESS
}
