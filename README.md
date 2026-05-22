# Piper

Piper is a Rust library for building staged data pipelines.

- `Pipe` is the fixed-width worker primitive for accumulator-style workloads.
- `Piper` is the managed pipeline harness with live outputs, graph-based fork/join execution, dynamic anchor-based worker scaling, pull-based telemetry, optional CSV telemetry, graceful shutdown, abort, and reusable internal buffer leases.
- `pipeline!` is re-exported from the runtime crate and generates a typed pipeline wrapper from either linear stage sugar or named graph edges.

Forks are MPMC work-sharing fan-out: each item emitted onto a forked link is consumed by one downstream branch, not broadcast to every branch. Joins are merged streams where multiple upstream stages send the same type into one downstream link.

Dynamic pipelines can mark one or more heavy stages with `anchor(...)`; Piper scales scalable anchors carefully up to their configured maximum and scales surrounding stages to keep anchors fed and drained. `anchor(...).fixed_threads(n)` marks a fixed control point that the manager tunes around but never resizes.
