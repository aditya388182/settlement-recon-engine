
D21 — the DAG stages the delivery locally before the gate. GE validates the
      file you were sent, byte for byte, including its SHA256, so it has to
      see the actual bytes. Staging also keeps run_checkpoint.py free of any
      cloud dependency, which is what lets CI run it hermetically on the
      committed ci_mini fixture on Day 6.

D22 — canonicalisation is a DAG task, not a manual prerequisite. The engine
      reads canonical tables. A DAG that jumped from the gate straight to the
      engine would reconcile whatever happened to be in the lake from the last
      manual run, and the green run would prove nothing.

D23 — the negative control is a flag (--ignore-point-in-time), not a hacked
      copy of the job. Same discipline as --chaos-drop-one: a proof that
      requires editing code to reproduce is not a proof anyone else can run,
      and it cannot go into CI.
