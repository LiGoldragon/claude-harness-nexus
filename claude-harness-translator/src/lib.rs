//! The Claude Harness Nexus's translator: the one place that holds Claude
//! Code's words. It reads a hook's payload into a Hook, follows a transcript
//! for its Nexus, reading each new row, and discovers each session Claude
//! starts under its transcript root.

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
    let mut rows = Following_Data { row_vector: Vec::new(), offset: *offset };
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

/// A lasting emission: an outcome each time the transcript grows, until the
/// Nexus closes the connection. A transcript not yet written is waited for.
/// A path with no ancestor at all ends the connection, and the emission is Unanswered.
pub fn follow(mut stream: UnixStream, follow: Follow_Data) -> io::Result<()> {
    let watching = inotify::init(inotify::CreateFlags::CLOEXEC)?;
    let mut appearing = Appearing::new(PathBuf::from(&follow.file_path.0));
    let mut followed = false;
    let mut read_from = follow.read_from.clone();
    let mut events = [0u8; 4096];
    loop {
        if !followed {
            followed = appearing.watch(&watching, inotify::WatchFlags::MODIFY)?.is_some();
        }
        if followed {
            let offset = match &read_from {
                ReadFrom::Offset(offset) => *offset,
                ReadFrom::FileEnd => Offset(std::fs::metadata(&appearing.path)?.len().try_into().unwrap_or(i64::MAX)),
            };
            let rows = rows_after(&follow.file_path.0, &offset, &follow.row_count)?;
            read_from = ReadFrom::Offset(rows.offset);
            let full = rows.row_vector.len() as i64 >= follow.row_count.0;
            if !rows.row_vector.is_empty() {
                rows.write_to(&mut stream)?;
            }
            if full {
                continue;
            }
        }
        if closed(&watching, &stream)? {
            return Ok(());
        }
        rustix::io::read(&watching, &mut events)?;
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
