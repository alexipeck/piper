# Piper

Piper is a Rust library for building staged data pipelines.

- `Pipe` is the fixed-width worker primitive for accumulator-style workloads.
- `Piper` is the managed linear pipeline harness with live outputs, dynamic worker scaling, snapshots, graceful shutdown, abort, and reusable internal buffer leases.
- `pipeline!` is re-exported from the runtime crate and generates a typed pipeline wrapper from a compact DSL.
