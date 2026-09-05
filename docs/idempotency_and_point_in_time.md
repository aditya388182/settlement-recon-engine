# Idempotency and Point-in-Time Reproducibility

## 1. The T-5 Proof (Bitemporal Validity)
Fee schedule `v2` took force on 9 July. Rerunning 6 July after that change reproduced the original output byte for byte:
`sha256 = efca74a30252c223d9a0aa5c61bb119b3924df1beb0c9fe7551aed71bc1d86a5` (39,884 rows).
The AS OF join correctly selected `v1` because 6 July is in `[1 July, 9 July)`. Tolerances were identical, matching was identical, output was identical.

## 2. The Negative Control
Forcing "always use today's fees" (ignoring point-in-time) moves the hash to:
`sha256 = 9e8900566d6755faf3d592dee77df1617f96686df1704bfa636c35d7e3ce8420`.
Break-class COUNTS do not move, because the simulated amount mismatches are corrupted beyond both the old and new tolerances. What the wrong fee model breaks is the internal ranking and assignment of close matches.

## Delta Time Travel vs Bitemporal Validity
`VERSION AS OF` answers "what did the table look like when we ran?". 
Bitemporal validity columns answer "what fee was in force on the transaction's date?". 
Reconciliation needs the second.
