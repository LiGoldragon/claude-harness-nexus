//! The Claude Harness Nexus's translator: the one place that holds Claude
//! Code's words. It reads a hook's payload into a Hook, follows a transcript
//! for its Nexus, reading each new row, and discovers each session Claude
//! starts under its transcript root. A transcript's follow witnesses which live
//! Claude processes hold its session, and tells the session's end when none does.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{self, BufRead, BufReader, Seek, SeekFrom};
use std::io::Write;
use std::mem::MaybeUninit;
use std::net::Shutdown;
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

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
    fn watch(&mut self, watching: &OwnedFd, flags: inotify::WatchFlags) -> io::Result<Option<i32>> {
        loop {
            if let Ok(watch) = inotify::add_watch(watching, &self.path, flags) {
                if let Some((_, ancestor)) = self.ancestor.take() {
                    inotify::remove_watch(watching, ancestor)?;
                }
                return Ok(Some(watch));
            }
            let Some(nearest) = self.path.ancestors().skip(1).find(|ancestor| ancestor.is_dir()).map(Path::to_owned) else {
                return Err(io::Error::new(io::ErrorKind::NotFound, "no ancestor of the path exists"));
            };
            if self.ancestor.as_ref().is_some_and(|(directory, _)| *directory == nearest) {
                return Ok(None);
            }
            if let Some((_, ancestor)) = self.ancestor.take() {
                inotify::remove_watch(watching, ancestor)?;
            }
            let ancestor = inotify::add_watch(watching, &nearest, inotify::WatchFlags::CREATE | inotify::WatchFlags::MOVED_TO)?;
            self.ancestor = Some((nearest, ancestor));
        }
    }
}

/// Waits for what the watches report or for the Nexus to close the connection,
/// true when it has closed.
fn closed(watching: &OwnedFd, stream: &UnixStream) -> io::Result<bool> {
    let mut waiting = [PollFd::new(watching, PollFlags::IN), PollFd::new(stream, PollFlags::IN)];
    rustix::event::poll(&mut waiting, None)?;
    Ok(!waiting[1].revents().is_empty())
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

/// Which live Claude processes hold one session, read from Claude's sessions
/// directory: each process whose record names the session is held by a pidfd,
/// once the start the record gives is the start the process with that pid has,
/// so a pid the kernel has given again is not taken. A process is let go when
/// its pidfd becomes readable, the process gone, or when its record names
/// another session. The session has ended when no process holds it; that is
/// told once, until a process holds it again.
struct Witness {
    session_id: String,
    directory: Appearing,
    watch: Option<i32>,
    holders: BTreeMap<i32, OwnedFd>,
    /// A record that cannot be read whose process is alive: what it names is not known, so no end is told.
    unread: bool,
    told: bool,
}

impl Witness {
    const WATCHED: inotify::WatchFlags = inotify::WatchFlags::CLOSE_WRITE
        .union(inotify::WatchFlags::MOVED_TO)
        .union(inotify::WatchFlags::DELETE)
        .union(inotify::WatchFlags::MOVED_FROM)
        .union(inotify::WatchFlags::ONLYDIR);

    /// The witness of the session a transcript is named for, under a sessions directory.
    fn of(transcript: &Path, session_root: &SessionRoot) -> Option<Witness> {
        let session_id = session(transcript)?.0;
        Some(Witness { session_id, directory: Appearing::new(PathBuf::from(&session_root.0.0)), watch: None, holders: BTreeMap::new(), unread: false, told: false })
    }

    /// The directory is watched once it exists, and each record in it read.
    fn look(&mut self, watching: &OwnedFd) -> io::Result<()> {
        if self.watch.is_none() {
            self.watch = self.directory.watch(watching, Witness::WATCHED)?;
        }
        if self.watch.is_none() {
            return Ok(());
        }
        let Ok(entries) = std::fs::read_dir(&self.directory.path) else {
            self.unread = true;
            return Ok(());
        };
        self.unread = false;
        for path in entries.flatten().map(|entry| entry.path()).filter(|path| path.extension().is_some_and(|extension| extension == "json")) {
            match Recorded::read(&path) {
                Some(recorded) if recorded.session_id == self.session_id => self.take(recorded),
                Some(recorded) => {
                    self.holders.remove(&recorded.process_id);
                }
                None => {
                    let alive = path.file_stem().and_then(|stem| stem.to_str()).and_then(|stem| stem.parse::<i32>().ok()).and_then(started).is_some();
                    self.unread |= alive;
                }
            }
        }
        Ok(())
    }

    /// The pidfd is opened before the start is checked: a process with that
    /// start, there after the pidfd is open, is the one the pidfd holds.
    fn take(&mut self, recorded: Recorded) {
        if self.holders.contains_key(&recorded.process_id) {
            return;
        }
        let Some(pid) = rustix::process::Pid::from_raw(recorded.process_id) else { return };
        let Ok(pidfd) = rustix::process::pidfd_open(pid, rustix::process::PidfdFlags::empty()) else { return };
        if started(recorded.process_id).as_deref() == Some(recorded.process_start.as_str()) {
            self.holders.insert(recorded.process_id, pidfd);
        }
    }

    /// The session's end, once, when the directory is read whole and no process holds the session.
    fn ending(&mut self) -> Option<EndReason> {
        if !self.holders.is_empty() {
            self.told = false;
            return None;
        }
        if self.watch.is_none() || self.unread || self.told {
            return None;
        }
        self.told = true;
        Some(EndReason::Died)
    }
}

/// What one follow reads into: the transcript's watch, and the witness of its session.
struct Followed {
    watching: OwnedFd,
    transcript: Appearing,
    transcript_watch: Option<i32>,
    witness: Option<Witness>,
}

impl Followed {
    /// Waits for a watch to report, a held process to end or the Nexus to
    /// close the connection, true when it has closed. What the watches report
    /// off the transcript is the sessions directory, or a directory waited
    /// for: the witness looks again.
    fn wait(&mut self, stream: &UnixStream, buffer: &mut [MaybeUninit<u8>]) -> io::Result<bool> {
        let (reported, gone) = {
            let mut waiting = vec![PollFd::new(&self.watching, PollFlags::IN), PollFd::new(stream, PollFlags::IN)];
            let held: Vec<i32> = self.witness.iter().flat_map(|witness| witness.holders.keys().copied()).collect();
            waiting.extend(self.witness.iter().flat_map(|witness| witness.holders.values()).map(|pidfd| PollFd::new(pidfd, PollFlags::IN)));
            rustix::event::poll(&mut waiting, None)?;
            if !waiting[1].revents().is_empty() {
                return Ok(true);
            }
            let gone: Vec<i32> = held.into_iter().zip(&waiting[2..]).filter(|(_, waited)| !waited.revents().is_empty()).map(|(process_id, _)| process_id).collect();
            (!waiting[0].revents().is_empty(), gone)
        };
        let mut looking = false;
        if reported {
            let mut reader = inotify::Reader::new(&self.watching, buffer);
            loop {
                let event = reader.next()?;
                looking |= Some(event.wd()) != self.transcript_watch;
                if reader.is_buffer_empty() {
                    break;
                }
            }
        }
        if let Some(witness) = self.witness.as_mut() {
            for process_id in gone {
                witness.holders.remove(&process_id);
            }
            if looking {
                witness.look(&self.watching)?;
            }
        }
        Ok(false)
    }
}

/// A lasting emission: an outcome each time the transcript grows, until the
/// Nexus closes the connection. A transcript not yet written is waited for.
/// A path with no ancestor at all ends the connection, and the emission is
/// Unanswered. Where a sessions directory is given, the session's end is told
/// with the rows read up to it: when no live process holds the session, at
/// the start or once the last one is gone, after every row it wrote.
pub fn follow(mut stream: UnixStream, follow: Follow_Data) -> io::Result<()> {
    let path = PathBuf::from(&follow.file_path.0);
    let witness = follow.session_root_option.as_ref().and_then(|session_root| Witness::of(&path, session_root));
    let mut following = Followed { watching: inotify::init(inotify::CreateFlags::CLOEXEC)?, transcript: Appearing::new(path), transcript_watch: None, witness };
    if let Some(witness) = following.witness.as_mut() {
        witness.look(&following.watching)?;
    }
    let mut read_from = follow.read_from.clone();
    let mut buffer = [MaybeUninit::<u8>::uninit(); 4096];
    loop {
        if following.transcript_watch.is_none() {
            following.transcript_watch = following.transcript.watch(&following.watching, inotify::WatchFlags::MODIFY)?;
        }
        let offset = match (&read_from, following.transcript_watch) {
            (ReadFrom::Offset(offset), _) => *offset,
            (ReadFrom::FileEnd, Some(_)) => Offset(std::fs::metadata(&following.transcript.path)?.len().try_into().unwrap_or(i64::MAX)),
            (ReadFrom::FileEnd, None) => Offset(0),
        };
        let mut rows = match following.transcript_watch {
            Some(_) => rows_after(&follow.file_path.0, &offset, &follow.row_count)?,
            None => Following_Data { row_vector: Vec::new(), offset, end_reason_option: None },
        };
        read_from = ReadFrom::Offset(rows.offset);
        let full = rows.row_vector.len() as i64 >= follow.row_count.0;
        if !full {
            rows.end_reason_option = following.witness.as_mut().and_then(Witness::ending);
        }
        if !rows.row_vector.is_empty() || rows.end_reason_option.is_some() {
            rows.write_to(&mut stream)?;
        }
        if full {
            continue;
        }
        if following.wait(&stream, &mut buffer)? {
            return Ok(());
        }
    }
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
    watching: OwnedFd,
    root: Appearing,
    root_watch: Option<i32>,
    projects: BTreeMap<i32, PathBuf>,
    reported: BTreeSet<PathBuf>,
}

impl Discovery {
    const WATCHED: inotify::WatchFlags = inotify::WatchFlags::CREATE.union(inotify::WatchFlags::MOVED_TO).union(inotify::WatchFlags::ONLYDIR);

    /// Each directory directly under the root, as it is now.
    fn directories(&self) -> Vec<PathBuf> {
        let Ok(entries) = std::fs::read_dir(&self.root.path) else { return Vec::new() };
        entries.flatten().filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir())).map(|entry| entry.path()).collect()
    }

    /// A project directory is watched for the transcripts that appear in it.
    fn watch(&mut self, project: PathBuf) {
        if let Ok(watch) = inotify::add_watch(&self.watching, &project, Discovery::WATCHED) {
            self.projects.insert(watch, project);
        }
    }

    /// A project directory that appears after the discovery began is new
    /// whole: what is in it already appeared, and is reported.
    fn appeared(&mut self, project: PathBuf) -> io::Result<()> {
        self.watch(project.clone());
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

    /// What the watches have reported since the last read: each watch, what it
    /// saw and the name it saw it on.
    fn events(&self, buffer: &mut [MaybeUninit<u8>]) -> io::Result<Vec<(i32, inotify::ReadFlags, Option<std::ffi::OsString>)>> {
        use std::os::unix::ffi::OsStrExt;
        let mut reader = inotify::Reader::new(&self.watching, buffer);
        let mut events = Vec::new();
        loop {
            let event = reader.next()?;
            let name = event.file_name().map(|name| std::ffi::OsStr::from_bytes(name.to_bytes()).to_owned());
            events.push((event.wd(), event.events(), name));
            if reader.is_buffer_empty() {
                return Ok(events);
            }
        }
    }

    /// The root has come to exist: each project directory in it is watched, and
    /// what is in a root that appeared after the discovery began is reported.
    fn rooted(&mut self, appeared: bool) -> io::Result<()> {
        for project in self.directories() {
            if appeared {
                self.appeared(project)?;
            } else {
                self.watch(project);
            }
        }
        Ok(())
    }
}

/// A lasting emission: a Discovered for each session that appears under the
/// root, as `<project directory>/<session id>.jsonl`, until the Nexus closes
/// the connection. Every project directory directly under the root is watched,
/// those there at the start and those created later. What is deeper, a
/// subagent's transcript or a tool's result, is no session's; what is there
/// when the discovery begins is not reported. A root that does not exist yet
/// is waited for, as a transcript is.
pub fn discover(stream: UnixStream, discover: Discover_Data) -> io::Result<()> {
    let watching = inotify::init(inotify::CreateFlags::CLOEXEC)?;
    let mut discovery = Discovery { stream, watching, root: Appearing::new(PathBuf::from(&discover.file_path.0)), root_watch: None, projects: BTreeMap::new(), reported: BTreeSet::new() };
    discovery.root_watch = discovery.root.watch(&discovery.watching, Discovery::WATCHED)?;
    if discovery.root_watch.is_some() {
        discovery.rooted(false)?;
    }
    let mut buffer = [MaybeUninit::<u8>::uninit(); 4096];
    loop {
        if closed(&discovery.watching, &discovery.stream)? {
            return Ok(());
        }
        let events = discovery.events(&mut buffer)?;
        let Some(root_watch) = discovery.root_watch else {
            discovery.root_watch = discovery.root.watch(&discovery.watching, Discovery::WATCHED)?;
            if discovery.root_watch.is_some() {
                discovery.rooted(true)?;
            }
            continue;
        };
        for (watch, seen, name) in events {
            let Some(name) = name else { continue };
            if watch == root_watch && seen.contains(inotify::ReadFlags::ISDIR) {
                discovery.appeared(discovery.root.path.join(name))?;
            } else if !seen.contains(inotify::ReadFlags::ISDIR)
                && let Some(path) = discovery.projects.get(&watch).map(|project| project.join(&name))
            {
                discovery.report(path)?;
            }
        }
    }
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
