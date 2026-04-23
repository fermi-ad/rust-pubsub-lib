# rust-pubsub-lib

`rust-pubsub-lib` provides a shared abstraction for publishing to, snapshotting from, and subscribing to broker-backed topics from Rust applications. It hides backend-specific connection details behind a small set of async traits so application code can stay focused on message handling instead of transport setup.

## Interface

The primary abstractions provided by this library are the [`Publisher`](src/lib.rs:193), [`Snapshot`](src/lib.rs:206), and [`Subscriber`](src/lib.rs:216) traits.

- [`Publisher`](src/lib.rs:193) asynchronously sends a message to a configured topic.
- [`Snapshot`](src/lib.rs:206) reads the currently available messages for a topic in one operation.
- [`Subscriber`](src/lib.rs:216) yields a stream of messages as they arrive.
- [`Message`](src/lib.rs:168) describes the message shape used across all backends.

The library also provides concrete message helpers:
- [`ByteMessage`](src/lib.rs:25) for raw byte payloads.
- [`StringMessage`](src/lib.rs:64) for UTF-8-oriented payloads, using lossy decoding when byte input is not valid UTF-8.

## Feature selection

Select one or more crate features depending on the broker backend your application uses.

- `kafka`: enables [`kafka_impl`](src/kafka_impl/mod.rs) with Kafka-backed implementations of [`Publisher`](src/lib.rs:193), [`Snapshot`](src/lib.rs:206), and [`Subscriber`](src/lib.rs:216).
- `redis-pubsub`: enables [`redis_impls::pubsub`](src/redis_impls/pubsub/mod.rs) with Redis pub/sub implementations of [`Publisher`](src/lib.rs:193) and [`Subscriber`](src/lib.rs:216).
- `redis-stream`: enables [`redis_impls::stream`](src/redis_impls/stream/mod.rs) with Redis Stream implementations of [`Publisher`](src/lib.rs:193), [`Snapshot`](src/lib.rs:206), and [`Subscriber`](src/lib.rs:216).
- `testing-utils`: enables backend-specific test harness helpers such as [`kafka_impl::testing_utils`](src/kafka_impl/testing_utils/mod.rs) for tests that exercise broker-facing code.

## Required environment variables

The following environment variables are required for Kafka-backed behavior:

- `KAFKA_CONNECTION_SECONDS`: controls the timeout used by Kafka operations in [`get_kafka_timeout_val()`](src/kafka_impl/mod.rs:253).

Redis-backed implementations do not currently require a crate-specific environment variable, but they do require a valid Redis connection URI to be passed to [`Publisher::new()`](src/lib.rs:195), [`Snapshot::get()`](src/lib.rs:211), or [`Subscriber::new()`](src/lib.rs:220).

## Documentation

Generate the crate documentation locally with [`cargo doc --all-features`](Cargo.toml) and open the output from `target/doc/`. The most important API entry points are documented in [`src/lib.rs`](src/lib.rs), with backend-specific details under [`src/kafka_impl/mod.rs`](src/kafka_impl/mod.rs) and [`src/redis_impls/mod.rs`](src/redis_impls/mod.rs).

## Development

The following packages must be present on the host machine when building this library:

- `cmake`
- `libcurl4-openssl-dev`
- `libsasl2-dev`
- `zlib`

The configured Dev Container in [`.devcontainer/`](.devcontainer/) has the necessary tools to build without additional installation.
