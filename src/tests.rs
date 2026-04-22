//! The tests for the [`crate`] core message and error types.

use super::*;

#[test]
fn pubsub_error_default_and_display_without_cause() {
    let err = PubSubError::default();
    assert_eq!(CANNED_ERR_MESSAGE, format!("{err}"));
}

#[test]
fn pubsub_error_from_debug_captures_debug_cause() {
    let err = PubSubError::from_debug(PubSubError::default());
    assert_eq!(
        format!(
            "{}\n Cause: {:?}",
            CANNED_ERR_MESSAGE,
            PubSubError::default()
        ),
        format!("{err}")
    );
}

#[test]
fn pubsub_error_from_display_captures_display_cause() {
    let err = PubSubError::from_display(PubSubError::default());
    assert_eq!(
        format!("{}\n Cause: {}", CANNED_ERR_MESSAGE, PubSubError::default()),
        format!("{err}")
    );
}

#[test]
fn byte_message_new_and_accessors_preserve_key_and_value() {
    let key = Some(vec![1_u8, 2, 3]);
    let value = vec![4_u8, 5, 6];
    let message = ByteMessage::new(key.clone(), value.clone());

    assert_eq!(key, message.key());
    assert_eq!(value, message.value());
}

#[test]
fn byte_message_from_value_has_no_key() {
    let value = vec![7_u8, 8, 9];
    let message = ByteMessage::from_value(value.clone());

    assert_eq!(None, message.key());
    assert_eq!(value, message.value());
}

#[test]
fn byte_message_from_bytes_clones_input_slices() {
    let key = b"byte-key";
    let value = b"byte-value";
    let message = ByteMessage::from_bytes(Some(key), value);

    assert_eq!(Some(key.to_vec()), message.key());
    assert_eq!(value.to_vec(), message.value());
}

#[test]
fn string_message_new_and_from_value_are_consistent() {
    let keyed = StringMessage::new(Some("some key".to_string()), "some text".to_string());
    assert_eq!(Some("some key".to_string()), keyed.key());
    assert_eq!("some text".to_string(), keyed.value());

    let unkeyed = StringMessage::from_value("payload".to_string());
    assert_eq!(None, unkeyed.key());
    assert_eq!("payload".to_string(), unkeyed.value());
}

#[test]
fn string_message_into_bytes_round_trips_key_and_value() {
    let message = StringMessage::new(Some("into-key".to_string()), "into-value".to_string());
    let bytes = message.clone().into_bytes();

    assert_eq!(Some(b"into-key".to_vec()), bytes.key());
    assert_eq!(b"into-value".to_vec(), bytes.value());
    assert_eq!(message, StringMessage::from(bytes));
}

#[test]
fn string_message_from_byte_message_uses_lossy_utf8_decoding() {
    let original = ByteMessage::new(Some(vec![0x66, 0x6f, 0x80]), vec![0x76, 0x61, 0x80]);
    let converted = StringMessage::from(original);

    assert_eq!(Some("fo�".to_string()), converted.key());
    assert_eq!("va�".to_string(), converted.value());
}

#[test]
fn string_message_from_bytes_handles_missing_key() {
    let message = StringMessage::from_bytes(None, b"payload");
    assert_eq!(None, message.key());
    assert_eq!("payload".to_string(), message.value());
}
