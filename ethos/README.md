The ethos of each root of the Claude Harness Nexus, in the forms of ethos-zero 31.1.0
(github:LiGoldragon/ethos-zero, 6e67995).

signal.ethos, meta.ethos, emission.ethos, library.ethos and memory.ethos are checked and
generated together, into claude-harness-process/src/generated/, which is checked in:

```sh
ethos-zero 'Generate.{ [ «ethos/signal.ethos» «ethos/meta.ethos» «ethos/emission.ethos» «ethos/library.ethos» «ethos/memory.ethos» ] «claude-harness-process/src/generated» }'
```

- signal.ethos is the Nexus's own contract with its translator. Listening, in its inputs,
  is the library's input every Nexus has: its translator tells it that it has begun to
  listen, and it is answered at once by Acknowledged.
- meta.ethos is the meta contract: Configure, answered by Configured. TranscriptRoot is the
  directory Claude keeps its projects in, set as `Configure.TranscriptRoot.«/abs/path»`;
  SessionRoot the directory Claude keeps a record of each running process in, `<pid>.json`
  naming its pid, its procStart and its sessionId, set as
  `Configure.SessionRoot.«/home/li/.claude/sessions»`. A Watch is born with the SessionRoot
  configured; one configured anew ends each living Watch Reconfigured, and it is born again
  with it.
- emission.ethos is the Emission root: Follow, Install and Discover, and what the translator
  observed. Discover is lasting, as Follow is: a Discovered for each session that appears
  under the root. A Discover given a SessionRoot reports at its start the transcript of each
  session a live Claude process holds, as the sessions directory records it, so a session
  made or resumed while no discovery ran is watched too; the Discover is born with the
  SessionRoot configured, and one configured anew bears it again. A TranscriptRoot configured
  with no SessionRoot is accepted, and reported on the Nexus's stderr as each discovery is
  born (TranscriptRootWithoutSessionRoot): its sessions are told Ended only by a hook. A Follow given a SessionRoot witnesses the session its transcript is named
  for: each process whose record names the session is held by a pidfd, taken only when the
  record's procStart is the start /proc gives that pid, so a pid given again is not taken.
  When no live process holds the session, at the start or once the last pidfd is readable,
  the Following carries `Some.Died` after every row read up to it, once until a process
  holds the session again; the follow goes on, since a resume writes on in the same file.
  A record that cannot be read is reported and skipped, so it never holds back another
  session's end; a sessions directory not read, or deleted, holds every end back until it is
  read again.
- A Row's TurnEnded carries the PendingAgentCount of its turn_duration row,
  `pendingBackgroundAgentCount` (none where the row has none): its events are InFlight, of one
  counted Subagent work for each, and TurnEnded, so a turn left with agents running ends Waiting.
- library.ethos holds what the roots share: Offset, ReadFrom and RowCount.
- The Memory keeps an ended session's Observed, Ended, and its Watch; a second Ended is no
  change, as a second TurnEnded is not, so a SessionEnd hook after the witness's end, or the
  witness's after the hook's, is dropped. A resume's turn-start row brings it to Working.
- memory.ethos has the Memory's five sections: its records, Watch, Observed and Keep; its
  changes, Observe and Recall; their answers, Observation and Recollection, by place. Keep is
  a record, so what returns for it is the process library's plain Answer.

process.ethos is not generated. Subscribe, Subscribed and Changed are signal-harness's
(github LiGoldragon/signal-harness, branch next) and Prepare and Prepared
signal-claude-prepare's, each served on a socket of its own; ethos-zero resolves no import
across crates, so the set with process.ethos is refused as `Foreign.Changed`.
claude-harness-process/src/processes.rs is written by hand as ethos-zero writes it when the two
contracts are declared as Signals of the Nexus (its fixture nexus/claude-harness), holding
Changed, Subscribed and Prepared as the processes hold them. The memory imports Changed from
that module. Discover is born of no call, record or process given, which ethos-zero refuses
as `Unborn.Discover`; it is written as ethos-zero writes a process born of a record of the
Memory holding its configured values, owned by the Memory, and the World bears it where a
transcript root is configured.
