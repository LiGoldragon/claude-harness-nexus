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
use harness::{Observed, State, Tokens, Transitionable};
use process::{Arrival, Gave, Given, Identity, Living, ProcessManager, Trace, Traced, Tracing};
use transport::{Framed, Place, Placed};

type Queue = Sender<Arrival<Process>>;

/// What the memory process keeps on disk.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, Default)]
struct Remembered {
    observed_vector: Vec<Observed>,
    watch_vector: Vec<Watch>,
}

/// The memory process: one change at a time, each kept before it is answered.
struct MemoryProcess {
    remembered: Remembered,
    file: std::path::PathBuf,
    count: Count,
}

impl MemoryProcess {
    fn open(place: &Place, count: Count) -> MemoryProcess {
        let file = place.path(Placed::Memory);
        let remembered = File::open(&file)
            .ok()
            .and_then(|file| Remembered::read_from(&mut BufReader::new(file)).ok().flatten())
            .unwrap_or_default();
        MemoryProcess { remembered, file, count }
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

    fn process_of(&self, watch: &Watch) -> Process {
        Process::Watch(WatchProcess { flow_id: watch.flow_id.clone(), file_path: watch.file_path.clone(), offset: watch.offset.clone(), count: self.count.clone() })
    }

    /// A Watch first kept is born as its process; one already held is answered alone.
    fn watch(&mut self, watch: Watch) -> (Answer, Option<Process>) {
        if self.remembered.watch_vector.iter().any(|held| held.flow_id == watch.flow_id && held.file_path == watch.file_path) {
            return (Answer::Succeeded, None);
        }
        self.remembered.watch_vector.retain(|held| held.flow_id != watch.flow_id);
        self.remembered.watch_vector.push(watch.clone());
        match self.keep() {
            Answer::Succeeded => (Answer::Succeeded, Some(self.process_of(&watch))),
            Answer::Failed => (Answer::Failed, None),
        }
    }

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
        self.keep();
        Observation(changed)
    }
}

/// The signal process: what enters the program and what leaves it.
struct SignalProcess {
    arrived: Arc<Mutex<VecDeque<UnixStream>>>,
    connections: BTreeMap<Identity, UnixStream>,
}

impl SignalProcess {
    fn start(place: &Place, queue: Queue) -> std::io::Result<SignalProcess> {
        let path = place.path(Placed::NexusSocket);
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path)?;
        let arrived = Arc::new(Mutex::new(VecDeque::new()));
        let held = arrived.clone();
        thread::spawn(move || {
            for mut stream in listener.incoming().flatten() {
                let Ok(Some(call)) = Call::read_from(&mut stream) else { continue };
                match call.input {
                    Input::Report(hook) => {
                        held.lock().expect("the arrived connections").push_back(stream);
                        let process = Process::Report(ReportProcess { caller: call.caller, hook });
                        if queue.send(Arrival::Born { owner: None, process }).is_err() {
                            return;
                        }
                    }
                    Input::Subscribe => {}
                }
            }
        });
        Ok(SignalProcess { arrived, connections: BTreeMap::new() })
    }

    /// A call's process takes the connection it came on: births come in the order the calls did.
    fn bind(&mut self, living: &Living<Process>) {
        if let Process::Report(_) = living.process
            && let Some(stream) = self.arrived.lock().expect("the arrived connections").pop_front()
        {
            self.connections.insert(living.identity.clone(), stream);
        }
    }

    fn answer(&mut self, identity: &Identity, output: Output) {
        if let Some(stream) = self.connections.get_mut(identity) {
            let _ = output.write_to(stream);
        }
    }

    fn close(&mut self, identity: &Identity) {
        self.connections.remove(identity);
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

/// The trace, archived as Signal in the Nexus's memory.
struct Archive(File);

/// Everything the manager does passes here: it is archived, and carried.
struct World {
    signal: SignalProcess,
    memory: MemoryProcess,
    emission: EmissionSocket,
    archive: Archive,
    queue: Queue,
}

impl World {
    fn give_back(&self, identity: Identity, taken: Taken) {
        let _ = self.queue.send(Arrival::Given(Given { identity, taken }));
    }

    fn carry(&mut self, Gave { identity, gift }: Gave<Gift>) {
        match gift {
            Gift::Reported(reported) => self.signal.answer(&identity, Output::Reported(reported)),
            Gift::Changed(_) => {}
            Gift::Watch(watch) => {
                let (answer, born) = self.memory.watch(watch);
                self.give_back(identity, Taken::Answer(answer));
                if let Some(process) = born {
                    let _ = self.queue.send(Arrival::Born { owner: None, process });
                }
            }
            Gift::Observe(observe) => {
                let observation = self.memory.observe(observe);
                self.give_back(identity, Taken::Observation(observation));
            }
            Gift::Follow(follow) => self.emission.follow(identity, follow, self.queue.clone()),
        }
    }
}

impl Tracing<Process> for World {
    fn trace(&mut self, trace: Traced<Process>) {
        let _ = trace.write_to(&mut self.archive.0);
        match trace {
            Trace::Born(living) => self.signal.bind(&living),
            Trace::Gave(gave) => self.carry(gave),
            Trace::Took(_) => {}
            Trace::Ended(ended) => {
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
    if let Err(error) = place.prepare() {
        eprintln!("{error}");
        return ExitCode::FAILURE;
    }
    let (queue, arrivals) = mpsc::channel();
    let memory = MemoryProcess::open(&place, Count(256));
    for watch in &memory.remembered.watch_vector {
        let _ = queue.send(Arrival::Born { owner: None, process: memory.process_of(watch) });
    }
    let signal = match SignalProcess::start(&place, queue.clone()) {
        Ok(signal) => signal,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    let archive = match OpenOptions::new().create(true).append(true).open(place.path(Placed::Trace)) {
        Ok(file) => Archive(file),
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    let emission = EmissionSocket { place: place.clone(), lasting: BTreeMap::new() };
    let mut world = World { signal, memory, emission, archive, queue };
    ProcessManager::default().run(arrivals, &mut world);
    ExitCode::SUCCESS
}
