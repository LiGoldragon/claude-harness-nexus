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
- meta.ethos is the meta contract: Configure, answered by Configured.
- emission.ethos is the Emission root: Follow and Install, and what the translator observed.
- library.ethos holds what the roots share: Offset, ReadFrom and RowCount.
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
that module.
