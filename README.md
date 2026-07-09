# rust-pubsub-lib

`rust-pubsub-lib` provides a shared abstraction for publishing to, snapshotting from, and subscribing to broker-backed topics from Rust applications. It hides backend-specific connection details behind a small set of async traits so application code can stay focused on message handling instead of transport setup.

## Interface

The primary abstractions provided by this library are the [`Publisher`](src/lib.rs), [`Snapshot`](src/lib.rs), and [`Subscriber`](src/lib.rs) traits.

- [`Publisher`](src/lib.rs) asynchronously sends a message to a configured topic.
- [`Snapshot`](src/lib.rs) reads the currently available messages for a topic in one operation.
- [`Subscriber`](src/lib.rs) yields a stream of messages as they arrive. Backend errors are handled
  internally; the stream yields only successfully decoded messages.
- [`Message`](src/lib.rs) describes the message shape used across all backends.

The library also provides concrete message helpers:
- [`ByteMessage`](src/lib.rs) for raw byte payloads.
- [`StringMessage`](src/lib.rs) for UTF-8-oriented payloads, using lossy decoding when byte input is not valid UTF-8.

## Feature selection

Select one or more crate features depending on the broker backend your application uses.

- `kafka`: enables [`kafka_impl`](src/kafka_impl/mod.rs) with Kafka-backed implementations of [`Publisher`](src/lib.rs), [`Snapshot`](src/lib.rs), and [`Subscriber`](src/lib.rs).
- `redis-pubsub`: enables [`redis_impls::pubsub`](src/redis_impls/pubsub/mod.rs) with Redis pub/sub implementations of [`Publisher`](src/lib.rs) and [`Subscriber`](src/lib.rs).
- `redis-stream`: enables [`redis_impls::stream`](src/redis_impls/stream/mod.rs) with Redis Stream implementations of [`Publisher`](src/lib.rs), [`Snapshot`](src/lib.rs), and [`Subscriber`](src/lib.rs).
- `testing-utils`: enables backend-specific test harness helpers such as [`kafka_impl::testing_utils`](src/kafka_impl/testing_utils/mod.rs) for tests that exercise broker-facing code.

## Shared subscriber cache

All three streaming backends (Kafka, Redis pub/sub, and Redis Stream) share a single process-wide
runtime cache in [`src/cache.rs`](src/cache.rs). Each `(host, topic)` pair reuses one background
task rather than spinning up a new connection per subscriber. Idle runtimes are evicted
automatically after a grace period once they have no active listeners.

## Environment variables

The following environment variables are read by the Kafka backend:

- `KAFKA_CONNECTION_SECONDS`: controls the timeout used by Kafka operations in
  [`get_kafka_timeout_val()`](src/kafka_impl/mod.rs). Defaults to `1` second if not set.

Redis-backed implementations do not read any crate-specific environment variables, but they do
require a valid Redis connection URI to be passed to [`Publisher::new()`](src/lib.rs),
[`Snapshot::get()`](src/lib.rs), or [`Subscriber::new()`](src/lib.rs).

## Documentation

Generate the crate documentation locally with [`cargo doc --all-features`](Cargo.toml) and open the output from `target/doc/`. The most important API entry points are documented in [`src/lib.rs`](src/lib.rs), with backend-specific details under [`src/kafka_impl/mod.rs`](src/kafka_impl/mod.rs) and [`src/redis_impls/mod.rs`](src/redis_impls/mod.rs).

## Development

The following packages must be present on the host machine when building this library:

- `cmake` (Or equivalent CMake provider)
- `gcc`/`g++` (Or equivalent C/C++ compiler/linker)
- `libcurl4-openssl-dev` (Or equivalent `curl` dev files)

The configured Dev Container in [`.devcontainer/`](.devcontainer/) has the necessary tools to build without additional installation.
