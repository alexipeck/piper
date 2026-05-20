# Piper

Piper is a Rust library for building staged data pipelines.

- `Pipe` is the fixed-width worker primitive for accumulator-style workloads.
- `Piper` is the managed linear pipeline harness with live outputs, dynamic anchor-based worker scaling, pull-based telemetry, optional CSV telemetry, graceful shutdown, abort, and reusable internal buffer leases.
- `pipeline!` is re-exported from the runtime crate and generates a typed pipeline wrapper from a compact DSL.

Dynamic pipelines mark exactly one heavy stage with `anchor(...)`; Piper scales that anchor carefully up to its configured maximum and scales the surrounding stages to keep it fed and drained.
