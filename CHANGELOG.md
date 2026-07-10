# Changelog

All notable changes to this project will be documented in this file.

## [10.0.0]

### Breaking Changes

#### `Subscriber::get_stream()` now returns `MessageStream<M>` instead of `Result<MessageStream<M>, PubSubError>`

The `Subscriber::get_stream()` trait method signature changed from:

```rust
fn get_stream<'a, M: Message + 'static>(
    &'a self,
) -> impl Future<Output = Result<MessageStream<M>, PubSubError>> + Send + use<'a, Self, M>;
```

to:

```rust
fn get_stream<'a, M: Message + 'static>(
    &'a self,
) -> impl Future<Output = MessageStream<M>> + Send + use<'a, Self, M>;
```

**Migration:** Remove the outer `Result` unwrap from every `get_stream().await` call site.

Before:

```rust
let mut stream = subscriber.get_stream::<StringMessage>().await.unwrap();
```

After:

```rust
let mut stream = subscriber.get_stream::<StringMessage>().await;
```

Because errors are now absorbed internally by each backend, `get_stream` can no longer fail at the
point of subscription. Returning a `Result` is therefore no longer meaningful.

#### `MessageStream<M>` now yields `M` directly instead of `Result<M, PubSubError>`

The `MessageStream<M>` type alias changed from:

```rust
Pin<Box<dyn Stream<Item = Result<M, PubSubError>> + Send + 'static>>
```

to:

```rust
Pin<Box<dyn Stream<Item = M> + Send + 'static>>
```

**Migration:** Remove the inner `Result` unwrap from every `stream.next().await` call site.

Before:

```rust
let msg: StringMessage = stream.next().await   // Option<Result<StringMessage, PubSubError>>
    .unwrap()   // Result<StringMessage, PubSubError>
    .unwrap();  // StringMessage
```

After:

```rust
let msg: StringMessage = stream.next().await  // Option<StringMessage>
    .unwrap();                                 // StringMessage
```

Connection errors, broker failures, and decode errors are now handled internally by each backend.
The library logs them, applies exponential backoff, and reconnects automatically. Callers receive
only successfully decoded messages; no per-item error handling is required on the stream.

If the consumer falls behind the internal broadcast buffer, messages may be silently dropped. A
warning is logged when this occurs. Callers that require guaranteed delivery should use the
corresponding `Snapshot` implementation to re-hydrate missed state.

### New Features

#### Shared process-wide subscriber cache (`src/cache.rs`)

All three streaming backends — Kafka, Redis pub/sub, and Redis Stream — now share a single
process-wide runtime cache. Each `(host, topic)` pair reuses one background task rather than
spinning up a new connection per subscriber. Idle runtimes are evicted automatically after a grace
period once they have no active listeners.

Previously each backend maintained its own independent cache module (`kafka_impl/cache`,
`redis_impls/stream/cache`). These have been consolidated into a single generic `cache` module at
the crate root. The cache accepts any `start_stream` function matching the signature
`(CancellationToken, String, String, Sender<ByteMessage>) -> impl Future<Output = ()>`, so adding
new backends requires no changes to the cache itself.

#### Automatic reconnection for Redis pub/sub subscribers

`RedisSubscriber` (Redis pub/sub) now runs a background reconnect loop. When the connection drops,
the subscriber applies exponential backoff (starting at 1 s, capped at 30 s) and reconnects
automatically. Previously, a dropped connection silently ended the stream.

`RedisSubscriber` no longer holds a `CancellationToken` or implements `Drop` directly. Lifetime
management is now handled by the shared cache layer, which cancels the background task via its own
`CancellationToken` when the cached runtime is evicted.

#### Error deduplication and structured backoff (`OutageState`)

A new internal `OutageState<K>` helper is shared across **all** backends — Redis pub/sub, Redis
Stream, and Kafka. It suppresses repeated log lines for the same error kind during a sustained
outage and resets the backoff timer on recovery. Recovery events are logged at `INFO` level; errors
are logged at `ERROR` level (first occurrence of each kind only).

The type is generic over the error-kind key `K: PartialEq`:
- Redis backends pass `err.kind()` (`redis::ErrorKind`) as the key.
- The Kafka backend passes `mem::discriminant(&err)` (`Discriminant<KafkaError>`) as the key.

This replaces the ad-hoc `handle_connection_err` free function that previously existed in both the
Kafka stream and Redis Stream implementations. The Kafka backend's reconnect cap also changed from
300 s to 30 s to match the shared `MAX_BACKOFF` constant.

### Improvements

- **Redis Stream subscriber:** `XREAD` errors and connection failures are now absorbed internally.
  The stream yields only successfully decoded messages. Failed stream-entry conversions are logged
  as warnings rather than being forwarded as error items.
- **Kafka subscriber:** Broadcast receiver lag (dropped messages) is now logged as a warning
  rather than surfaced as an error item on the stream.
- **`KAFKA_CONNECTION_SECONDS` now has a default:** If the environment variable is not set, the
  Kafka timeout defaults to `1` second instead of panicking or producing an error.
- **Test performance:** Cache and lazy-startup tests replaced `sleep(200ms)` with `yield_now()`
  and the idle-eviction test replaced `sleep(4s)` with `tokio::time::advance` under
  `start_paused = true`. Tests are now instant and deterministic.
- **Reduced dependencies:** The `gssapi` feature was removed from `rdkafka`, eliminating
  `openssl-sys`, `sasl2-sys`, `duct`, `os_pipe`, `shared_child`, `sigchld`, and `signal-hook`
  from the dependency graph. The build no longer requires `libcurl4-openssl-dev`, `libsasl2-dev`,
  or `zlib` on the host machine.
- **`KafkaPublisher` and `KafkaSubscriber` now derive `Debug`** instead of providing manual
  `Debug` implementations. The output is identical; the manual impls were removed as dead code.
- **`RedisSubscriber` (Redis Stream) now derives `Debug`** instead of providing a manual
  `Debug` implementation.
- **`monitor_stream` return type simplified:** The internal Redis Stream `monitor_stream` function
  now returns `Result<(), RedisError>` directly instead of `Option<RedisError>`, removing a
  `.flatten()` call at the call site and making the error path more idiomatic.
- **`StreamMessage` and `MapMessage` extracted** from `redis_impls/stream/mod.rs` into a
  dedicated `redis_impls/stream/stream_message.rs` module, reducing the size of the main stream
  module and making the type definitions easier to locate.
- **`From<KafkaError> for PubSubError`** moved to the top of `kafka_impl/mod.rs` alongside the
  other trait impls, improving readability.

### Removed

- **`KafkaStream` struct** (`kafka_impl/stream/mod.rs`) — replaced by the generic shared cache
  and the `kafka_impl/runtime/mod.rs` `start_stream` function. The `stream` sub-module has been
  renamed to `runtime`.
- **`RedisStream` struct** (`redis_impls/stream/runtime/mod.rs`) — replaced by the generic shared
  cache. The struct's `new`, `get_stream`, `receiver_count`, and `ensure_started` methods are no
  longer needed; the cache layer handles all of this.
- **`ConsumerCacheKey` and `ConsumerCacheEntry`** from `kafka_impl/cache` — the Kafka consumer
  cache has been removed entirely. Kafka subscriber streams are now managed by the shared
  `cache::get_stream` function. The `kafka_impl/cache` module has been renamed to
  `kafka_impl/kafka_cache` and retains only the producer cache.
- **`redis_impls/stream/cache` module** — the Redis Stream subscriber cache has been removed and
  replaced by the shared `cache::get_stream` function.
- Tests that verified error propagation through the stream (`test_subscribe_fails_for_invalid_host`,
  `redis_stream_reuses_one_cached_stream_per_host_and_topic`,
  `redis_stream_cached_stream_is_evicted_after_going_idle`,
  `redis_stream_subscriber_reports_connection_failure`) have been removed. These tests relied on
  connection errors being surfaced as stream items, which is no longer the case. The underlying
  behavior (reconnection, backoff, error logging) is now tested via `OutageState` unit tests.
- **`kafka_impl/cache/tests.rs`** — the Kafka consumer cache tests (`kafka_subscriber_shares_cached_stream_per_host_topic`,
  `kafka_cached_stream_starts_when_first_receiver_is_requested`) have been removed along with the
  consumer cache itself. The producer cache test (`kafka_producer_hot_path_does_not_insert_duplicate_entry`)
  is retained in `kafka_impl/kafka_cache/tests.rs`.

### Dependency Updates

- `redis` 1.2.4 → 1.3.0
- `arc-swap` 1.9.1 → 1.9.2
- `wasm-bindgen` 0.2.125 → 0.2.126
- `xxhash-rust` 0.8.15 → 0.8.16
- `rustversion` 1.0.22 → 1.0.23
- `crossbeam-utils` 0.8.21 → 0.8.22
- Added `tokio` (dev-dependency, `test-util` feature) for `start_paused` and `advance` support
  in tests
