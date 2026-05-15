//! The tests for the [`crate`] core message and error types.

use super::*;

/// Test-only [`Message`] implementation used to validate the public extension-point story.
///
/// Unlike most tests in this file, this is not focused on one of the crate's built-in message
/// types. Instead, it models what a consuming application might do: define its own message type,
/// store owned data internally as [`Vec<u8>`], expose borrowed access as slices, and bridge broker
/// interoperability through [`ByteMessage`].
///
/// This makes the borrowed-view redesign concrete by proving that external implementations are not
/// limited to borrowed container references like `&Vec<u8>`. A downstream type can keep owned byte
/// storage while presenting the more idiomatic borrowed forms expected by generic backend code.
#[derive(Clone, Debug, PartialEq)]
struct SliceBackedMessage {
    key: Option<Vec<u8>>,
    value: Vec<u8>,
}

impl From<ByteMessage> for SliceBackedMessage {
    fn from(bytes: ByteMessage) -> Self {
        let (key, value) = bytes.extract_key_value();
        Self { key, value }
    }
}

impl Message for SliceBackedMessage {
    type Key = Vec<u8>;
    type Value = Vec<u8>;
    type KeyRef<'a> = &'a [u8];
    type ValueRef<'a> = &'a [u8];

    fn new(key: Option<Vec<u8>>, value: Vec<u8>) -> Self {
        Self { key, value }
    }

    fn from_value(value: Vec<u8>) -> Self {
        Self { key: None, value }
    }

    fn from_bytes(key: Option<&[u8]>, value: &[u8]) -> Self {
        Self {
            key: key.map(|bytes| bytes.to_vec()),
            value: value.to_vec(),
        }
    }

    fn extract_key(self) -> Option<Vec<u8>> {
        self.key
    }

    fn extract_key_value(self) -> (Option<Vec<u8>>, Vec<u8>) {
        (self.key, self.value)
    }

    fn extract_value(self) -> Vec<u8> {
        self.value
    }

    fn into_bytes(self) -> ByteMessage {
        ByteMessage::new(self.key, self.value)
    }

    fn key(&self) -> Option<Vec<u8>> {
        self.key.clone()
    }

    fn key_ref(&self) -> Option<Self::KeyRef<'_>> {
        self.key.as_deref()
    }

    fn value(&self) -> Vec<u8> {
        self.value.clone()
    }

    fn value_ref(&self) -> Self::ValueRef<'_> {
        self.value.as_slice()
    }
}

#[test]
fn pubsub_error_default_and_display_without_cause() {
    let err = PubSubError::default();
    assert_eq!(None, err.cause_message());
    assert_eq!(CANNED_ERR_MESSAGE, format!("{err}"));
}

#[test]
fn pubsub_error_from_debug_captures_debug_cause() {
    let err = PubSubError::from_debug(PubSubError::default());
    assert_eq!(Some("PubSubError { cause: None }"), err.cause_message());
    assert_eq!(format!("{CANNED_ERR_MESSAGE}"), format!("{err}"));
    assert_eq!(
        "PubSubError { cause: Some(\"PubSubError { cause: None }\") }",
        format!("{err:?}")
    );
}

#[test]
fn byte_message_new_and_accessors_preserve_key_and_value() {
    let key = Some(vec![1_u8, 2, 3]);
    let value = vec![4_u8, 5, 6];
    let message = ByteMessage::new(key.clone(), value.clone());

    assert_eq!(key.as_deref(), message.key_ref());
    assert_eq!(value.as_slice(), message.value_ref());
}

#[test]
fn byte_message_from_value_has_no_key() {
    let value = vec![7_u8, 8, 9];
    let message = ByteMessage::from_value(value.clone());

    assert_eq!(None, message.key_ref());
    assert_eq!(value.as_slice(), message.value_ref());
}

#[test]
fn byte_message_from_bytes_clones_input_slices() {
    let key = b"byte-key";
    let value = b"byte-value";
    let message = ByteMessage::from_bytes(Some(key), value);

    assert_eq!(Some(key.as_slice()), message.key_ref());
    assert_eq!(value.as_slice(), message.value_ref());
}

#[test]
fn byte_message_extract_key_returns_owned_key() {
    let key = Some(vec![1_u8, 2, 3]);
    let message = ByteMessage::new(key.clone(), vec![4_u8, 5, 6]);

    assert_eq!(key, message.extract_key());
}

#[test]
fn byte_message_extract_value_returns_owned_value() {
    let value = vec![4_u8, 5, 6];
    let message = ByteMessage::new(Some(vec![1_u8, 2, 3]), value.clone());

    assert_eq!(value, message.extract_value());
}

#[test]
fn byte_message_extract_key_value_returns_owned_parts() {
    let key = Some(vec![1_u8, 2, 3]);
    let value = vec![4_u8, 5, 6];
    let message = ByteMessage::new(key.clone(), value.clone());

    assert_eq!((key, value), message.extract_key_value());
}

#[test]
fn byte_message_extractors_handle_missing_key() {
    let value = vec![7_u8, 8, 9];

    assert_eq!(None, ByteMessage::from_value(value.clone()).extract_key());
    assert_eq!(
        (None, value.clone()),
        ByteMessage::from_value(value.clone()).extract_key_value()
    );
    assert_eq!(
        value,
        ByteMessage::from_value(vec![7_u8, 8, 9]).extract_value()
    );
}

#[test]
fn string_message_new_and_from_value_are_consistent() {
    let keyed = StringMessage::new(Some("some key".to_string()), "some text".to_string());
    assert_eq!(Some("some key"), keyed.key_ref());
    assert_eq!("some text", keyed.value_ref());

    let unkeyed = StringMessage::from_value("payload".to_string());
    assert_eq!(None, unkeyed.key_ref());
    assert_eq!("payload", unkeyed.value_ref());
}

#[test]
fn string_message_into_bytes_round_trips_key_and_value() {
    let message = StringMessage::new(Some("into-key".to_string()), "into-value".to_string());
    let bytes = message.clone().into_bytes();

    assert_eq!(Some(b"into-key".as_slice()), bytes.key_ref());
    assert_eq!(b"into-value".as_slice(), bytes.value_ref());
    assert_eq!(message, StringMessage::from(bytes));
}

#[test]
fn string_message_from_byte_message_uses_lossy_utf8_decoding() {
    let original = ByteMessage::new(Some(vec![0x66, 0x6f, 0x80]), vec![0x76, 0x61, 0x80]);
    let converted = StringMessage::from(original);

    assert_eq!(Some("fo�"), converted.key_ref());
    assert_eq!("va�", converted.value_ref());
}

#[test]
fn string_message_from_bytes_handles_missing_key() {
    let message = StringMessage::from_bytes(None, b"payload");
    assert_eq!(None, message.key_ref());
    assert_eq!("payload", message.value_ref());
}

#[test]
fn string_message_extract_key_returns_owned_key() {
    let key = Some("some key".to_string());
    let message = StringMessage::new(key.clone(), "some text".to_string());

    assert_eq!(key, message.extract_key());
}

#[test]
fn string_message_extract_value_returns_owned_value() {
    let value = "some text".to_string();
    let message = StringMessage::new(Some("some key".to_string()), value.clone());

    assert_eq!(value, message.extract_value());
}

#[test]
fn string_message_extract_key_value_returns_owned_parts() {
    let key = Some("some key".to_string());
    let value = "some text".to_string();
    let message = StringMessage::new(key.clone(), value.clone());

    assert_eq!((key, value), message.extract_key_value());
}

#[test]
fn string_message_extractors_handle_missing_key() {
    let value = "payload".to_string();

    assert_eq!(None, StringMessage::from_value(value.clone()).extract_key());
    assert_eq!(
        (None, value.clone()),
        StringMessage::from_value(value.clone()).extract_key_value()
    );
    assert_eq!(
        value,
        StringMessage::from_value("payload".to_string()).extract_value()
    );
}

#[test]
fn message_borrowed_views_use_slices_and_strs() {
    let bytes = ByteMessage::new(Some(vec![1_u8, 2, 3]), vec![4_u8, 5, 6]);
    let text = StringMessage::new(Some("hello".to_string()), "world".to_string());

    assert_eq!(Some([1_u8, 2, 3].as_slice()), bytes.key_ref());
    assert_eq!([4_u8, 5, 6].as_slice(), bytes.value_ref());
    assert_eq!(Some("hello"), text.key_ref());
    assert_eq!("world", text.value_ref());
}

#[test]
fn custom_message_can_expose_slice_borrowed_views() {
    let message = SliceBackedMessage::from_bytes(Some(b"custom-key"), b"custom-value");

    assert_eq!(Some(b"custom-key".as_slice()), message.key_ref());
    assert_eq!(b"custom-value".as_slice(), message.value_ref());
}

#[test]
fn custom_message_round_trips_through_byte_message_backbone() {
    let message = SliceBackedMessage::from_bytes(Some(b"custom-key"), b"custom-value");

    assert_eq!(
        ByteMessage::new(Some(b"custom-key".to_vec()), b"custom-value".to_vec()),
        message.clone().into_bytes()
    );
    assert_eq!(
        (Some(b"custom-key".to_vec()), b"custom-value".to_vec()),
        message.extract_key_value()
    );
}
