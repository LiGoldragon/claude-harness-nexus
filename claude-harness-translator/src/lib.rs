//! The Claude Harness Nexus's translator: the one place that holds Claude
//! Code's words. It reads a hook's payload into a Hook, follows a transcript
//! for its Nexus, reading each new row, and discovers each session Claude
//! starts under its transcript root. A transcript's follow witnesses which live
//! Claude processes hold its session, and tells the session's end when none does.
//! Every follow and discovery runs in one loop, over one inotify instance.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::mem::MaybeUninit;
use std::net::Shutdown;
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::{Arc, mpsc};

use claude_harness_process::*;
use process::{Caller, ProcessId, UserId};
use harness::{BackgroundWork, Compaction, Description, EndReason, FilePath, Response, SessionId, Tokens, Trigger, WorkId, WorkKind};
use rustix::event::{PollFd, PollFlags};
use rustix::fs::inotify;
use serde_json::Value;
use transport::{Framed, Place, Placed};

#[derive(Debug, PartialEq, Eq)]
pub enum Untranslated {
    Unreadable,
    UnknownEvent(String),
    MissingField(&'static str),
}

/// What reads the harness's own words into a type of the Nexus.
pub trait Translatable: Sized {
    fn translate(text: &str) -> Result<Self, Untranslated>;
}

fn text(value: &Value, field: &'static str) -> Result<String, Untranslated> {
    value.get(field).and_then(Value::as_str).map(str::to_owned).ok_or(Untranslated::MissingField(field))
}

/// A task of a kind not known is left out, and the rest of its Stop is read.
fn work(task: &Value) -> Result<BackgroundWork, Untranslated> {
    let work_kind = match text(task, "type")?.as_str() {
        "shell" => WorkKind::Shell,
        "subagent" => WorkKind::Subagent,
        "monitor" => WorkKind::Monitor,
        "workflow" => WorkKind::Workflow,
        other => return Err(Untranslated::UnknownEvent(format!("background task type {other}"))),
    };
    Ok(BackgroundWork { work_id: WorkId(text(task, "id")?), work_kind, description: Description(text(task, "description").unwrap_or_default()) })
}

impl Translatable for Hook {
    fn translate(payload: &str) -> Result<Hook, Untranslated> {
        let value: Value = serde_json::from_str(payload).map_err(|_| Untranslated::Unreadable)?;
        let session = || text(&value, "session_id").map(SessionId);
        Ok(match text(&value, "hook_event_name")?.as_str() {
            "SessionStart" => Hook::SessionStart(SessionStart_Data {
                session_id: session()?,
                file_path: FilePath(text(&value, "transcript_path")?),
                source: match text(&value, "source").ok().as_deref() {
                    Some("resume") => Source::Resume(value.get("context_tokens").and_then(Value::as_i64).map(Tokens)),
                    Some("compact") => Source::Compact,
                    _ => Source::Startup,
                },
            }),
            "UserPromptSubmit" => Hook::UserPromptSubmit(session()?),
            "PermissionRequest" => Hook::PermissionRequest(session()?),
            "PostToolUse" => Hook::PostToolUse(session()?),
            "Stop" => Hook::Stop(Stop_Data {
                session_id: session()?,
                response_option: text(&value, "last_assistant_message").ok().map(Response),
                background_work_vector: value
                    .get("background_tasks")
                    .and_then(Value::as_array)
                    .map(|tasks| {
                        tasks
                            .iter()
                            .filter_map(|task| work(task).inspect_err(|untranslated| eprintln!("{untranslated:?}")).ok())
                            .collect()
                    })
                    .unwrap_or_default(),
            }),
            "SessionEnd" => Hook::SessionEnd(SessionEnd_Data {
                session_id: session()?,
                end_reason: match text(&value, "reason")?.as_str() {
                    "clear" => EndReason::Cleared,
                    _ => EndReason::Exited,
                },
            }),
            other => return Err(Untranslated::UnknownEvent(other.to_owned())),
        })
    }
}

/// Claude writes this user row when the user presses Escape mid-turn: a text
/// item reading "[Request interrupted by user]", or "... for tool use]" when a tool was running.
fn interrupts(value: &Value) -> bool {
    value
        .get("message")
        .and_then(|message| message.get("content"))
        .and_then(Value::as_array)
        .is_some_and(|items| items.iter().any(|item| item.get("text").and_then(Value::as_str).is_some_and(|text| text.starts_with("[Request interrupted by user"))))
}

/// The compact_boundary system row: its trigger and the context's size before it.
/// A trigger that is neither manual nor auto is not a compaction this reads.
fn compaction(value: &Value) -> Option<Compaction> {
    let metadata = value.get("compactMetadata")?;
    let trigger = match metadata.get("trigger").and_then(Value::as_str)? {
        "manual" => Trigger::Manual,
        "auto" => Trigger::Automatic,
        _ => return None,
    };
    Some(Compaction { trigger, tokens_option: metadata.get("preTokens").and_then(Value::as_i64).map(Tokens) })
}

impl Translatable for Row {
    /// Of the main thread: an assistant row tells the context's size, the user
    /// row that carries turnPosition starts a turn, the turn_duration system
    /// row Claude writes as every turn ends ends it, the user row that reads
    /// as an interruption is Interrupted, and the compact_boundary row is
    /// Compacted. Every other row is Other, the stop_hook_summary row among them.
    fn translate(line: &str) -> Result<Row, Untranslated> {
        let value: Value = serde_json::from_str(line).map_err(|_| Untranslated::Unreadable)?;
        let main_thread = value.get("isSidechain").and_then(Value::as_bool) != Some(true);
        let usage = value.get("message").and_then(|message| message.get("usage"));
        let subtype = value.get("subtype").and_then(Value::as_str);
        Ok(match (value.get("type").and_then(Value::as_str), main_thread, usage) {
            (Some("user"), true, _) if interrupts(&value) => Row::Interrupted,
            (Some("user"), true, _) if value.get("turnPosition").is_some_and(|position| !position.is_null()) => Row::TurnStarted,
            (Some("system"), true, _) if subtype == Some("turn_duration") => Row::TurnEnded,
            (Some("system"), true, _) if subtype == Some("compact_boundary") => compaction(&value).map_or(Row::Other, Row::Compacted),
            (Some("assistant"), true, Some(usage)) => {
                let tokens = ["input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens"]
                    .iter()
                    .filter_map(|field| usage.get(field).and_then(Value::as_i64))
                    .sum();
                Row::Assistant(Tokens(tokens))
            }
            _ => Row::Other,
        })
    }
}

/// Reads the whole lines after the offset, at most the row count of them.
fn rows_after(path: &str, offset: &Offset, row_count: &RowCount) -> io::Result<Following_Data> {
    let mut file = BufReader::new(File::open(path)?);
    file.seek(SeekFrom::Start(offset.0.max(0) as u64))?;
    let mut rows = Following_Data { row_vector: Vec::new(), offset: *offset, end_reason_option: None };
    let mut line = Vec::new();
    while (rows.row_vector.len() as i64) < row_count.0 {
        line.clear();
        let read = file.read_until(b'\n', &mut line)?;
        if read == 0 || line.last() != Some(&b'\n') {
            break;
        }
        rows.offset.0 += read as i64;
        rows.row_vector.push(Row::translate(&String::from_utf8_lossy(&line)).unwrap_or(Row::Other));
    }
    Ok(rows)
}

/// Whom a watch reports to, by its key: a follow, a sessions directory, or a discovery.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Watcher {
    Follow(u64),
    Sessions(u64),
    Discovery(u64),
}

/// The translator's one inotify instance, and whom each of its watches reports
/// to. A path two watchers watch is one watch, its flags the union of theirs;
/// it is removed when the last of them lets it go.
struct Watches {
    inotify: OwnedFd,
    watchers: BTreeMap<i32, Vec<Watcher>>,
}

impl Watches {
    fn new() -> io::Result<Watches> {
        Ok(Watches { inotify: inotify::init(inotify::CreateFlags::CLOEXEC | inotify::CreateFlags::NONBLOCK)?, watchers: BTreeMap::new() })
    }

    fn add(&mut self, path: &Path, flags: inotify::WatchFlags, watcher: Watcher) -> io::Result<i32> {
        let watch = inotify::add_watch(&self.inotify, path, flags | inotify::WatchFlags::MASK_ADD)?;
        self.watchers.entry(watch).or_default().push(watcher);
        Ok(watch)
    }

    fn remove(&mut self, watch: i32, watcher: Watcher) {
        let Some(watchers) = self.watchers.get_mut(&watch) else { return };
        if let Some(position) = watchers.iter().position(|held| *held == watcher) {
            watchers.swap_remove(position);
        }
        if watchers.is_empty() {
            self.watchers.remove(&watch);
            let _ = inotify::remove_watch(&self.inotify, watch);
        }
    }

    /// Each watcher a watch reports to, once.
    fn of(&self, watch: i32) -> Vec<Watcher> {
        let mut watchers = self.watchers.get(&watch).cloned().unwrap_or_default();
        watchers.sort();
        watchers.dedup();
        watchers
    }

    /// Everything the watches have reported since the last read, read until
    /// none is left: each watch, what it saw and the name it saw it on.
    fn events(&self, buffer: &mut [MaybeUninit<u8>]) -> io::Result<Vec<(i32, inotify::ReadFlags, Option<OsString>)>> {
        let mut reader = inotify::Reader::new(&self.inotify, buffer);
        let mut events = Vec::new();
        loop {
            match reader.next() {
                Ok(event) => {
                    let name = event.file_name().map(|name| OsStr::from_bytes(name.to_bytes()).to_owned());
                    events.push((event.wd(), event.events(), name));
                }
                Err(rustix::io::Errno::AGAIN) => return Ok(events),
                Err(rustix::io::Errno::INTR) => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
}

/// A path watched once it exists. Until then its nearest existing ancestor is
/// watched for what appears in it, and each time something does the path is
/// tried again.
struct Appearing {
    path: PathBuf,
    ancestor: Option<(PathBuf, i32)>,
}

impl Appearing {
    fn new(path: PathBuf) -> Appearing {
        Appearing { path, ancestor: None }
    }

    /// The path's watch once it exists, None while it is waited for. A path
    /// with no ancestor at all is NotFound.
    fn watch(&mut self, watches: &mut Watches, flags: inotify::WatchFlags, watcher: Watcher) -> io::Result<Option<i32>> {
        loop {
            if let Ok(watch) = watches.add(&self.path, flags, watcher) {
                self.release(watches, watcher);
                return Ok(Some(watch));
            }
            let Some(nearest) = self.path.ancestors().skip(1).find(|ancestor| ancestor.is_dir()).map(Path::to_owned) else {
                return Err(io::Error::new(io::ErrorKind::NotFound, "no ancestor of the path exists"));
            };
            if self.ancestor.as_ref().is_some_and(|(directory, _)| *directory == nearest) {
                return Ok(None);
            }
            self.release(watches, watcher);
            let ancestor = watches.add(&nearest, inotify::WatchFlags::CREATE | inotify::WatchFlags::MOVED_TO, watcher)?;
            self.ancestor = Some((nearest, ancestor));
        }
    }

    /// The ancestor waited on is let go.
    fn release(&mut self, watches: &mut Watches, watcher: Watcher) {
        if let Some((_, ancestor)) = self.ancestor.take() {
            watches.remove(ancestor, watcher);
        }
    }
}

/// What Claude records of one of its running processes, in its sessions
/// directory as `<pid>.json`: the process, the start the kernel gave it, and
/// the session it runs now.
struct Recorded {
    process_id: i32,
    process_start: String,
    session_id: String,
}

impl Recorded {
    /// The record a file holds, None when it cannot be read as one.
    fn read(path: &Path) -> Option<Recorded> {
        let value: Value = serde_json::from_str(&std::fs::read_to_string(path).ok()?).ok()?;
        let process_id = i32::try_from(value.get("pid")?.as_u64()?).ok()?;
        let process_start = match value.get("procStart")? {
            Value::String(start) => start.clone(),
            Value::Number(start) => start.to_string(),
            _ => return None,
        };
        Some(Recorded { process_id, process_start, session_id: text(&value, "sessionId").ok()? })
    }
}

/// A process's start, in clock ticks since the boot, as /proc/<pid>/stat
/// gives it: its twenty-second field. None when the process is gone or is a zombie.
fn started(process_id: i32) -> Option<String> {
    let stat = std::fs::read_to_string(format!("/proc/{process_id}/stat")).ok()?;
    let (_, fields) = stat.rsplit_once(") ")?;
    let mut fields = fields.split_whitespace();
    if matches!(fields.next()?, "Z" | "X") {
        return None;
    }
    fields.nth(18).map(str::to_owned)
}

/// Claude's sessions directory, read once for every follow it witnesses: each
/// `<pid>.json` in it, by its file, as the last event on that file left it.
struct Sessions {
    directory: Appearing,
    watch: Option<i32>,
    /// Each record by its file, None for one that cannot be read as a record.
    records: BTreeMap<PathBuf, Option<Recorded>>,
    /// The directory itself was read.
    readable: bool,
    followers: BTreeSet<u64>,
}

impl Sessions {
    const WATCHED: inotify::WatchFlags = inotify::WatchFlags::CLOSE_WRITE
        .union(inotify::WatchFlags::MOVED_TO)
        .union(inotify::WatchFlags::DELETE)
        .union(inotify::WatchFlags::MOVED_FROM)
        .union(inotify::WatchFlags::ONLYDIR);

    fn new(path: PathBuf, key: u64, watches: &mut Watches) -> io::Result<Sessions> {
        let mut sessions = Sessions { directory: Appearing::new(path), watch: None, records: BTreeMap::new(), readable: false, followers: BTreeSet::new() };
        sessions.look(watches, key)?;
        Ok(sessions)
    }

    /// The directory is watched once it exists, and each record in it read.
    fn look(&mut self, watches: &mut Watches, key: u64) -> io::Result<()> {
        if self.watch.is_none() {
            self.watch = self.directory.watch(watches, Sessions::WATCHED, Watcher::Sessions(key))?;
        }
        if self.watch.is_none() {
            return Ok(());
        }
        self.records.clear();
        let Ok(entries) = std::fs::read_dir(&self.directory.path) else {
            self.readable = false;
            return Ok(());
        };
        self.readable = true;
        for path in entries.flatten().map(|entry| entry.path()).filter(|path| Sessions::recording(path)) {
            let recorded = Recorded::read(&path);
            self.records.insert(path, recorded);
        }
        Ok(())
    }

    fn recording(path: &Path) -> bool {
        path.extension().is_some_and(|extension| extension == "json")
    }

    /// What one event changed, true when a record may have: a record written
    /// or moved in is read again, one deleted or moved out is gone. An event
    /// off the directory's own watch, a directory waited for, looks again whole.
    fn seen(&mut self, watches: &mut Watches, key: u64, watch: i32, seen: inotify::ReadFlags, name: Option<&OsStr>) -> io::Result<bool> {
        if self.watch != Some(watch) {
            self.look(watches, key)?;
            return Ok(true);
        }
        let Some(path) = name.map(|name| self.directory.path.join(name)).filter(|path| Sessions::recording(path)) else { return Ok(false) };
        if seen.intersects(inotify::ReadFlags::DELETE | inotify::ReadFlags::MOVED_FROM) {
            self.records.remove(&path);
        } else if seen.intersects(inotify::ReadFlags::CLOSE_WRITE | inotify::ReadFlags::MOVED_TO) {
            let recorded = Recorded::read(&path);
            self.records.insert(path, recorded);
        } else {
            return Ok(false);
        }
        Ok(true)
    }

    fn release(&mut self, watches: &mut Watches, key: u64) {
        self.directory.release(watches, Watcher::Sessions(key));
        if let Some(watch) = self.watch.take() {
            watches.remove(watch, Watcher::Sessions(key));
        }
    }
}

/// Which live Claude processes hold one session, as its sessions directory
/// records them: each process whose record names the session is held by a
/// pidfd, once the start the record gives is the start the process with that
/// pid has, so a pid the kernel has given again is not taken. A process is
/// let go when its pidfd becomes readable, the process gone, or when its
/// record names another session. The session has ended when no process holds
/// it; that is told once, until a process holds it again.
struct Witness {
    session_id: String,
    root: u64,
    holders: BTreeMap<i32, OwnedFd>,
    /// A record that cannot be read whose process is alive, or a directory not read: what it names is not known, so no end is told.
    unread: bool,
    told: bool,
}

impl Witness {
    fn new(session_id: String, root: u64) -> Witness {
        Witness { session_id, root, holders: BTreeMap::new(), unread: false, told: false }
    }

    /// The directory's records, as they stand, taken in.
    fn regard(&mut self, sessions: &Sessions) {
        let alive = |path: &PathBuf| path.file_stem().and_then(|stem| stem.to_str()).and_then(|stem| stem.parse::<i32>().ok()).and_then(started).is_some();
        self.unread = !sessions.readable || sessions.records.iter().any(|(path, recorded)| recorded.is_none() && alive(path));
        for recorded in sessions.records.values().flatten() {
            if recorded.session_id == self.session_id {
                self.take(recorded);
            } else {
                self.holders.remove(&recorded.process_id);
            }
        }
    }

    /// The pidfd is opened before the start is checked: a process with that
    /// start, there after the pidfd is open, is the one the pidfd holds.
    fn take(&mut self, recorded: &Recorded) {
        if self.holders.contains_key(&recorded.process_id) {
            return;
        }
        let Some(pid) = rustix::process::Pid::from_raw(recorded.process_id) else { return };
        let Ok(pidfd) = rustix::process::pidfd_open(pid, rustix::process::PidfdFlags::empty()) else { return };
        if started(recorded.process_id).as_deref() == Some(recorded.process_start.as_str()) {
            self.holders.insert(recorded.process_id, pidfd);
        }
    }

    /// The session's end, once, when the directory is watched and read whole and no process holds the session.
    fn ending(&mut self, watched: bool) -> Option<EndReason> {
        if !self.holders.is_empty() {
            self.told = false;
            return None;
        }
        if !watched || self.unread || self.told {
            return None;
        }
        self.told = true;
        Some(EndReason::Died)
    }
}

/// One transcript followed: where its Nexus reads from, its watch, and the witness of its session.
struct Followed {
    stream: UnixStream,
    file_path: FilePath,
    row_count: RowCount,
    read_from: ReadFrom,
    transcript: Appearing,
    transcript_watch: Option<i32>,
    witness: Option<Witness>,
}

/// A session's transcript is a file directly in a project directory, named
/// for its session: a canonical lowercase UUID, then .jsonl.
fn session(path: &Path) -> Option<SessionId> {
    if path.extension()? != "jsonl" {
        return None;
    }
    let stem = path.file_stem()?.to_str()?;
    let canonical = stem.len() == 36
        && stem.char_indices().all(|(position, character)| match position {
            8 | 13 | 18 | 23 => character == '-',
            _ => matches!(character, '0'..='9' | 'a'..='f'),
        });
    canonical.then(|| SessionId(stem.to_owned()))
}

/// What one discovery holds: the root's watch, once the root exists; each
/// project directory's, by its watch; and each transcript reported.
struct Discovery {
    stream: UnixStream,
    root: Appearing,
    root_watch: Option<i32>,
    projects: BTreeMap<i32, PathBuf>,
    reported: BTreeSet<PathBuf>,
}

impl Discovery {
    const WATCHED: inotify::WatchFlags = inotify::WatchFlags::CREATE.union(inotify::WatchFlags::MOVED_TO).union(inotify::WatchFlags::ONLYDIR);

    fn new(stream: UnixStream, discover: Discover_Data, key: u64, watches: &mut Watches) -> io::Result<Discovery> {
        let mut discovery = Discovery { stream, root: Appearing::new(PathBuf::from(&discover.file_path.0)), root_watch: None, projects: BTreeMap::new(), reported: BTreeSet::new() };
        discovery.root_watch = discovery.root.watch(watches, Discovery::WATCHED, Watcher::Discovery(key))?;
        if discovery.root_watch.is_some() {
            discovery.rooted(watches, key, false)?;
        }
        Ok(discovery)
    }

    /// Each directory directly under the root, as it is now.
    fn directories(&self) -> Vec<PathBuf> {
        let Ok(entries) = std::fs::read_dir(&self.root.path) else { return Vec::new() };
        entries.flatten().filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir())).map(|entry| entry.path()).collect()
    }

    /// A project directory is watched for the transcripts that appear in it, once.
    fn watch(&mut self, watches: &mut Watches, key: u64, project: PathBuf) {
        if let Ok(watch) = watches.add(&project, Discovery::WATCHED, Watcher::Discovery(key)) {
            match self.projects.entry(watch) {
                std::collections::btree_map::Entry::Vacant(vacant) => {
                    vacant.insert(project);
                }
                std::collections::btree_map::Entry::Occupied(_) => watches.remove(watch, Watcher::Discovery(key)),
            }
        }
    }

    /// A project directory that appears after the discovery began is new
    /// whole: what is in it already appeared, and is reported.
    fn appeared(&mut self, watches: &mut Watches, key: u64, project: PathBuf) -> io::Result<()> {
        self.watch(watches, key, project.clone());
        let Ok(entries) = std::fs::read_dir(&project) else { return Ok(()) };
        for entry in entries.flatten() {
            if entry.file_type().is_ok_and(|kind| kind.is_file()) {
                self.report(entry.path())?;
            }
        }
        Ok(())
    }

    /// A session's transcript is reported once.
    fn report(&mut self, path: PathBuf) -> io::Result<()> {
        let Some(session_id) = session(&path) else { return Ok(()) };
        if !self.reported.insert(path.clone()) {
            return Ok(());
        }
        Discovered_Data { session_id, file_path: FilePath(path.to_string_lossy().into_owned()) }.write_to(&mut self.stream)
    }

    /// The root has come to exist: each project directory in it is watched, and
    /// what is in a root that appeared after the discovery began is reported.
    fn rooted(&mut self, watches: &mut Watches, key: u64, appeared: bool) -> io::Result<()> {
        for project in self.directories() {
            if appeared {
                self.appeared(watches, key, project)?;
            } else {
                self.watch(watches, key, project);
            }
        }
        Ok(())
    }

    /// One event of the discovery's watches. While the root is waited for,
    /// any event tries it again.
    fn seen(&mut self, watches: &mut Watches, key: u64, watch: i32, seen: inotify::ReadFlags, name: Option<&OsStr>) -> io::Result<()> {
        let Some(root_watch) = self.root_watch else {
            self.root_watch = self.root.watch(watches, Discovery::WATCHED, Watcher::Discovery(key))?;
            if self.root_watch.is_some() {
                self.rooted(watches, key, true)?;
            }
            return Ok(());
        };
        let Some(name) = name else { return Ok(()) };
        if watch == root_watch && seen.contains(inotify::ReadFlags::ISDIR) {
            self.appeared(watches, key, self.root.path.join(name))?;
        } else if !seen.contains(inotify::ReadFlags::ISDIR)
            && let Some(path) = self.projects.get(&watch).map(|project| project.join(name))
        {
            self.report(path)?;
        }
        Ok(())
    }

    fn release(&mut self, watches: &mut Watches, key: u64) {
        self.root.release(watches, Watcher::Discovery(key));
        if let Some(watch) = self.root_watch.take() {
            watches.remove(watch, Watcher::Discovery(key));
        }
        for watch in std::mem::take(&mut self.projects).into_keys() {
            watches.remove(watch, Watcher::Discovery(key));
        }
    }
}

/// A lasting emission given to the loop: its connection and what it asks.
enum Arrival {
    Follow(UnixStream, Follow_Data),
    Discover(UnixStream, Discover_Data),
}

/// How arrivals reach a running loop: a channel, and a socket written once
/// for each so the loop wakes.
struct Arrivals {
    receiver: mpsc::Receiver<Arrival>,
    woken: UnixStream,
}

/// What the loop waits on.
#[derive(Clone, Copy)]
enum Polled {
    Watches,
    Woken,
    Follow(u64),
    Discovery(u64),
    Holder(u64, i32),
}

/// The one loop that follows every transcript and runs every discovery: one
/// inotify instance holds every watch, of each followed transcript, each
/// project directory and root, and each sessions directory, and each event
/// is given to whom its watch reports to. The loop polls the inotify
/// instance, each connection, to see it close, and each pidfd a witness holds.
struct Watched {
    watches: Watches,
    arrivals: Option<Arrivals>,
    follows: BTreeMap<u64, Followed>,
    sessions: BTreeMap<u64, Sessions>,
    roots: BTreeMap<PathBuf, u64>,
    discoveries: BTreeMap<u64, Discovery>,
    drawn: u64,
}

impl Watched {
    fn new(arrivals: Option<Arrivals>) -> io::Result<Watched> {
        Ok(Watched { watches: Watches::new()?, arrivals, follows: BTreeMap::new(), sessions: BTreeMap::new(), roots: BTreeMap::new(), discoveries: BTreeMap::new(), drawn: 0 })
    }

    fn draw(&mut self) -> u64 {
        self.drawn += 1;
        self.drawn
    }

    /// Until no arrival can come and nothing is followed or discovered. Each
    /// turn reads every event queued, and only then takes arrivals in: a
    /// record written before its follow arrived is in the records it regards.
    fn run(&mut self) -> io::Result<()> {
        let mut buffer = vec![MaybeUninit::<u8>::uninit(); 1 << 16];
        while self.arrivals.is_some() || !self.follows.is_empty() || !self.discoveries.is_empty() {
            let mut touched = BTreeSet::new();
            let mut regarded = BTreeSet::new();
            let mut arrived = Vec::new();
            for polled in self.wait()? {
                match polled {
                    Polled::Watches => {}
                    Polled::Woken => arrived.extend(self.woken()),
                    Polled::Follow(key) => self.unfollow(key),
                    Polled::Discovery(key) => self.undiscover(key),
                    Polled::Holder(key, process_id) => {
                        if let Some(witness) = self.follows.get_mut(&key).and_then(|followed| followed.witness.as_mut()) {
                            witness.holders.remove(&process_id);
                            touched.insert(key);
                        }
                    }
                }
            }
            self.read(&mut buffer, &mut touched, &mut regarded)?;
            for root in regarded {
                let Some(sessions) = self.sessions.get(&root) else { continue };
                for follower in &sessions.followers {
                    if let Some(witness) = self.follows.get_mut(follower).and_then(|followed| followed.witness.as_mut()) {
                        witness.regard(sessions);
                        touched.insert(*follower);
                    }
                }
            }
            for key in touched {
                self.advance(key);
            }
            for arrival in arrived {
                self.admit(arrival);
            }
        }
        Ok(())
    }

    /// What is ready among all the loop waits on.
    fn wait(&self) -> io::Result<Vec<Polled>> {
        let mut polled = vec![Polled::Watches];
        let mut waiting = vec![PollFd::new(&self.watches.inotify, PollFlags::IN)];
        if let Some(arrivals) = &self.arrivals {
            polled.push(Polled::Woken);
            waiting.push(PollFd::new(&arrivals.woken, PollFlags::IN));
        }
        for (key, followed) in &self.follows {
            polled.push(Polled::Follow(*key));
            waiting.push(PollFd::new(&followed.stream, PollFlags::IN));
            for (process_id, pidfd) in followed.witness.iter().flat_map(|witness| &witness.holders) {
                polled.push(Polled::Holder(*key, *process_id));
                waiting.push(PollFd::new(pidfd, PollFlags::IN));
            }
        }
        for (key, discovery) in &self.discoveries {
            polled.push(Polled::Discovery(*key));
            waiting.push(PollFd::new(&discovery.stream, PollFlags::IN));
        }
        match rustix::event::poll(&mut waiting, None) {
            Ok(_) => {}
            Err(rustix::io::Errno::INTR) => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        }
        Ok(polled.into_iter().zip(&waiting).filter(|(_, waited)| !waited.revents().is_empty()).map(|(polled, _)| polled).collect())
    }

    /// Each arrival waiting; once every sender is gone, none is waited for.
    fn woken(&mut self) -> Vec<Arrival> {
        let Some(arrivals) = &self.arrivals else { return Vec::new() };
        let mut closed = false;
        let mut drained = [0u8; 64];
        loop {
            match (&arrivals.woken).read(&mut drained) {
                Ok(0) => {
                    closed = true;
                    break;
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
        let mut arrived = Vec::new();
        loop {
            match arrivals.receiver.try_recv() {
                Ok(arrival) => arrived.push(arrival),
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    closed = true;
                    break;
                }
            }
        }
        if closed {
            self.arrivals = None;
        }
        arrived
    }

    /// An arrival that cannot be taken in is dropped, its connection with it.
    fn admit(&mut self, arrival: Arrival) {
        let key = self.draw();
        match arrival {
            Arrival::Follow(stream, follow) => {
                if self.followed(key, stream, follow).is_ok() {
                    self.advance(key);
                }
            }
            Arrival::Discover(stream, discover) => {
                if let Ok(discovery) = Discovery::new(stream, discover, key, &mut self.watches) {
                    self.discoveries.insert(key, discovery);
                }
            }
        }
    }

    /// A follow taken in; given a sessions directory, its witness regards it at once.
    fn followed(&mut self, key: u64, stream: UnixStream, follow: Follow_Data) -> io::Result<()> {
        let path = PathBuf::from(&follow.file_path.0);
        let mut witness = match (&follow.session_root_option, session(&path)) {
            (Some(session_root), Some(session_id)) => Some(Witness::new(session_id.0, self.rooted(PathBuf::from(&session_root.0.0))?)),
            _ => None,
        };
        if let Some(witness) = witness.as_mut()
            && let Some(sessions) = self.sessions.get_mut(&witness.root)
        {
            sessions.followers.insert(key);
            witness.regard(sessions);
        }
        let followed = Followed { stream, file_path: follow.file_path, row_count: follow.row_count, read_from: follow.read_from, transcript: Appearing::new(path), transcript_watch: None, witness };
        self.follows.insert(key, followed);
        Ok(())
    }

    /// The sessions directory at a path, read once for all who follow under it.
    fn rooted(&mut self, path: PathBuf) -> io::Result<u64> {
        if let Some(root) = self.roots.get(&path) {
            return Ok(*root);
        }
        let root = self.draw();
        let sessions = Sessions::new(path.clone(), root, &mut self.watches)?;
        self.sessions.insert(root, sessions);
        self.roots.insert(path, root);
        Ok(root)
    }

    /// A follow let go, and its sessions directory with the last who follows under it.
    fn unfollow(&mut self, key: u64) {
        let Some(mut followed) = self.follows.remove(&key) else { return };
        followed.transcript.release(&mut self.watches, Watcher::Follow(key));
        if let Some(watch) = followed.transcript_watch {
            self.watches.remove(watch, Watcher::Follow(key));
        }
        let Some(witness) = followed.witness else { return };
        let Some(sessions) = self.sessions.get_mut(&witness.root) else { return };
        sessions.followers.remove(&key);
        if sessions.followers.is_empty() {
            self.unroot(witness.root);
        }
    }

    /// A sessions directory let go, and every follow under it.
    fn unroot(&mut self, root: u64) {
        let followers = self.sessions.get(&root).map(|sessions| sessions.followers.clone()).unwrap_or_default();
        for follower in followers {
            self.unfollow(follower);
        }
        if let Some(mut sessions) = self.sessions.remove(&root) {
            sessions.release(&mut self.watches, root);
            self.roots.retain(|_, held| *held != root);
        }
    }

    fn undiscover(&mut self, key: u64) {
        if let Some(mut discovery) = self.discoveries.remove(&key) {
            discovery.release(&mut self.watches, key);
        }
    }

    /// The inotify instance read, each event given to whom its watch reports
    /// to. An overflowed queue touches every follow and has every sessions
    /// directory looked at again.
    fn read(&mut self, buffer: &mut [MaybeUninit<u8>], touched: &mut BTreeSet<u64>, regarded: &mut BTreeSet<u64>) -> io::Result<()> {
        for (watch, seen, name) in self.watches.events(buffer)? {
            if seen.contains(inotify::ReadFlags::QUEUE_OVERFLOW) {
                touched.extend(self.follows.keys().copied());
                let roots: Vec<u64> = self.sessions.keys().copied().collect();
                for root in roots {
                    match self.sessions.get_mut(&root).map(|sessions| sessions.look(&mut self.watches, root)) {
                        Some(Ok(())) => {
                            regarded.insert(root);
                        }
                        Some(Err(_)) => self.unroot(root),
                        None => {}
                    }
                }
                continue;
            }
            for watcher in self.watches.of(watch) {
                match watcher {
                    Watcher::Follow(key) => {
                        touched.insert(key);
                    }
                    Watcher::Sessions(root) => match self.sessions.get_mut(&root).map(|sessions| sessions.seen(&mut self.watches, root, watch, seen, name.as_deref())) {
                        Some(Ok(true)) => {
                            regarded.insert(root);
                        }
                        Some(Err(_)) => self.unroot(root),
                        _ => {}
                    },
                    Watcher::Discovery(key) => {
                        if let Some(Err(_)) = self.discoveries.get_mut(&key).map(|discovery| discovery.seen(&mut self.watches, key, watch, seen, name.as_deref())) {
                            self.undiscover(key);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// A follow that cannot go on, its path with no ancestor or its Nexus gone, is let go.
    fn advance(&mut self, key: u64) {
        if self.advanced(key).is_err() {
            self.unfollow(key);
        }
    }

    /// The rows written since the last read, as many outcomes as the row count
    /// asks, and with the last of them the session's end, when its witness tells one.
    fn advanced(&mut self, key: u64) -> io::Result<()> {
        let Some(followed) = self.follows.get_mut(&key) else { return Ok(()) };
        let watched = followed.witness.as_ref().is_some_and(|witness| self.sessions.get(&witness.root).is_some_and(|sessions| sessions.watch.is_some()));
        loop {
            if followed.transcript_watch.is_none() {
                followed.transcript_watch = followed.transcript.watch(&mut self.watches, inotify::WatchFlags::MODIFY, Watcher::Follow(key))?;
            }
            let offset = match (&followed.read_from, followed.transcript_watch) {
                (ReadFrom::Offset(offset), _) => *offset,
                (ReadFrom::FileEnd, Some(_)) => Offset(std::fs::metadata(&followed.transcript.path)?.len().try_into().unwrap_or(i64::MAX)),
                (ReadFrom::FileEnd, None) => Offset(0),
            };
            let mut rows = match followed.transcript_watch {
                Some(_) => rows_after(&followed.file_path.0, &offset, &followed.row_count)?,
                None => Following_Data { row_vector: Vec::new(), offset, end_reason_option: None },
            };
            followed.read_from = ReadFrom::Offset(rows.offset);
            let full = rows.row_vector.len() as i64 >= followed.row_count.0;
            if !full {
                rows.end_reason_option = followed.witness.as_mut().and_then(|witness| witness.ending(watched));
            }
            if !rows.row_vector.is_empty() || rows.end_reason_option.is_some() {
                rows.write_to(&mut followed.stream)?;
            }
            if !full {
                return Ok(());
            }
        }
    }
}

/// The translator's one loop, running: each lasting emission it is given is
/// followed or discovered there, beside every other, with no thread of its own.
#[derive(Clone)]
pub struct Watching {
    sender: mpsc::Sender<Arrival>,
    wake: Arc<UnixStream>,
}

impl Watching {
    /// The loop, on its own thread, for the translator's life.
    pub fn start() -> io::Result<Watching> {
        let (sender, receiver) = mpsc::channel();
        let (wake, woken) = UnixStream::pair()?;
        wake.set_nonblocking(true)?;
        woken.set_nonblocking(true)?;
        let mut watched = Watched::new(Some(Arrivals { receiver, woken }))?;
        std::thread::spawn(move || {
            if let Err(error) = watched.run() {
                eprintln!("{error}");
            }
        });
        Ok(Watching { sender, wake: Arc::new(wake) })
    }

    /// A transcript followed, as `follow` does.
    pub fn follow(&self, stream: UnixStream, follow: Follow_Data) {
        self.arrive(Arrival::Follow(stream, follow));
    }

    /// A root discovered, as `discover` does.
    pub fn discover(&self, stream: UnixStream, discover: Discover_Data) {
        self.arrive(Arrival::Discover(stream, discover));
    }

    /// A loop gone drops the arrival, and its connection ends.
    fn arrive(&self, arrival: Arrival) {
        if self.sender.send(arrival).is_ok() {
            let _ = (&*self.wake).write(&[1]);
        }
    }
}

/// A lasting emission: an outcome each time the transcript grows, until the
/// Nexus closes the connection. A transcript not yet written is waited for.
/// A path with no ancestor at all ends the connection, and the emission is
/// Unanswered. Where a sessions directory is given, the session's end is told
/// with the rows read up to it: when no live process holds the session, at
/// the start or once the last one is gone, after every row it wrote. This
/// runs the loop for the one follow, until it ends; the translator's own
/// follows share one loop, by Watching.
pub fn follow(stream: UnixStream, follow: Follow_Data) -> io::Result<()> {
    let mut watched = Watched::new(None)?;
    watched.admit(Arrival::Follow(stream, follow));
    watched.run()
}

/// A lasting emission: a Discovered for each session that appears under the
/// root, as `<project directory>/<session id>.jsonl`, until the Nexus closes
/// the connection. Every project directory directly under the root is watched,
/// those there at the start and those created later. What is deeper, a
/// subagent's transcript or a tool's result, is no session's; what is there
/// when the discovery begins is not reported. A root that does not exist yet
/// is waited for, as a transcript is. This runs the loop for the one
/// discovery, until it ends, as `follow` does.
pub fn discover(stream: UnixStream, discover: Discover_Data) -> io::Result<()> {
    let mut watched = Watched::new(None)?;
    watched.admit(Arrival::Discover(stream, discover));
    watched.run()
}

/// The translator itself as a caller: its process, and the user that owns it.
pub fn itself() -> io::Result<Caller> {
    use std::os::unix::fs::MetadataExt;
    let user_id = std::fs::metadata("/proc/self")?.uid();
    Ok(Caller { process_id: ProcessId(std::process::id().into()), user_id: UserId(user_id.into()) })
}

/// Writes the settings a session is started with, beside the sockets: the hook
/// client run at each named hook. The file is written whole, then moved into place.
pub fn install(place: &Place, install: &Install_Data) -> Installation_Data {
    let command = serde_json::json!([{ "hooks": [{ "type": "command", "command": install.hook_client.0 }] }]);
    let hooks: serde_json::Map<String, Value> = install.hooks.0.iter().map(|hook_name| (hook_name.0.clone(), command.clone())).collect();
    let settings = serde_json::json!({ "hooks": hooks });
    let path = place.path(Placed::Settings);
    let temporary = path.with_extension("next");
    let written = std::fs::write(&temporary, settings.to_string()).and_then(|()| std::fs::rename(&temporary, &path));
    match written {
        Ok(()) => Installation_Data::Installed(FilePath(path.to_string_lossy().into_owned())),
        Err(error) => Installation_Data::Uninstalled(error.raw_os_error().unwrap_or(1).into()),
    }
}

/// The hook's caller, from the connection's peer credentials.
pub fn caller(stream: &UnixStream) -> io::Result<Caller> {
    let credentials = rustix::net::sockopt::socket_peercred(stream)?;
    Ok(Caller { process_id: ProcessId(credentials.pid.as_raw_nonzero().get().into()), user_id: UserId(credentials.uid.as_raw().into()) })
}

/// The CLI, written once for the ordinary and the meta socket: one datom
/// written to the translator; each reply printed as it arrives, one datom a
/// line, until the translator ends the exchange.
pub fn call(text_socket: Placed) -> ExitCode {
    let mut arguments = std::env::args().skip(1);
    let (Some(datom), None) = (arguments.next(), arguments.next()) else {
        eprintln!("Usage");
        return ExitCode::from(2);
    };
    let place = match Place::from_configuration() {
        Ok(place) => place,
        Err(unplaced) => {
            eprintln!("{unplaced:?}");
            return ExitCode::from(2);
        }
    };
    let Ok(mut stream) = UnixStream::connect(place.path(text_socket)) else {
        eprintln!("TranslatorDown");
        return ExitCode::FAILURE;
    };
    if stream.write_all(datom.as_bytes()).is_err() || stream.shutdown(Shutdown::Write).is_err() {
        return ExitCode::FAILURE;
    }
    let mut out = std::io::stdout();
    for line in BufReader::new(stream).lines().map_while(Result::ok) {
        if writeln!(out, "{line}").and_then(|()| out.flush()).is_err() {
            break;
        }
    }
    ExitCode::SUCCESS
}
