The ethos of each root of the Claude Harness Nexus; generated.rs in claude-harness-process is
written by hand from them. Checked with ethos-zero 30.1.0:

- signal.ethos, emission.ethos (an Emission written as a Signal): Checked.
- meta.ethos: Rejected, TooFewVariants.Set. The meta surface sets one value, Count.
- memory.ethos: Rejected, Arity.{ 2 5 }. It follows Process.4's Memory sketch (records, changes,
  answers, types); ethos-zero's Memory root takes two sections.
- process.ethos: Rejected, Root. ethos-zero knows no Process root; the file follows Process.4.
