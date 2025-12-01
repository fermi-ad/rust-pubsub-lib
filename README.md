# rust-pubsub-lib
This is a library for connecting to a message broker from within a Rust app. It encapsulates the specifics of the broker connection logic, exposing message access through a consistent interface. The intention is that all Rust apps import this library as a dependency when they need access to pub-sub capabilities, so necessary changes to how our services interact with the message broker can be managed from one place.

## Interface 
The primary abstractions provided by this library are the `Publisher`, `Snapshot`, and `Subscriber` structs. `Publisher` and `Subscriber` expose a predefined set of methods for asynchronously publishing and subscribing to messages on a given topic, while `Snapshot` represents a one-off request for all messages currently on the topic.

#### Required environment variables
For this lib to operate successfully, the following environment variables must be set:
- `KAFKA_HOST` -> At time of writing, Kafka is the message broker/pub-sub service of choice. This variable specifies the location of the Kafka instance to connect to.

## Docs

The Rust documentation and a getting-started guide can be found [here](https://doc.rust-lang.org/book/title-page.html).