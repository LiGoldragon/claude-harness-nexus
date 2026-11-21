The ethos of each root of the Claude Harness Nexus; generated.rs in claude-harness-process is
written by hand from them. Checked with ethos-zero at github:LiGoldragon/ethos-zero, branch process-root, 48f155d:

- signal.ethos, meta.ethos, emission.ethos (an Emission written as a Signal): Checked.
  signal.ethos is the Nexus's own contract with its translator. Subscribe is signal-harness's
  (github LiGoldragon/signal-harness, branch next) and Prepare signal-claude-prepare's, each
  served on a socket of its own.
  Listening, in the Signal's inputs, is the library's input every Nexus has: its translator
  tells it that it has begun to listen, and it is answered at once by Acknowledged.
- memory.ethos: Checked. The Memory root holds its two sections, the imports and the records.
- library.ethos: Checked. The changes and their answers, Observe, Observation, Keep, Recall,
  Recollection, Answer, and Offset and RowCount, are declared here until the Memory page
  places them.
- process.ethos: Checked with ethos-zero branch process-root (48f155d), which knows the Process
  root. Its generated outcome types (Reporting, Watching, Configuring, Preparing) are the ones
  generated.rs holds; Configuring.Configured carries meta's Configured, as the variant of a
  defined type's name does.
