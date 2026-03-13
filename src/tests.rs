//! The tests for the lib.rs file

use super::*;

#[test]
fn pubsub_error_display() {
    let err = PubSubError::default();
    assert_eq!(CANNED_ERR_MESSAGE, format!("{}", err));

    let err = PubSubError {
        message: "test".to_string(),
        cause: Some(Box::new(PubSubError::default())),
    };
    assert_eq!(
        "test\n Cause: ".to_owned() + CANNED_ERR_MESSAGE,
        format!("{}", err)
    );
}

#[test]
fn message_from_value() {
    let val = String::from("some text");
    let output = Message::from_value(val.clone());
    assert_eq!(output.key, None);
    assert_eq!(output.value, val);
}

#[test]
fn message_from_key_value() {
    let key = Some(String::from("some key"));
    let val = String::from("some text");
    let output = Message::new(key.clone(), val.clone());
    assert_eq!(output.key, key);
    assert_eq!(output.value, val);
}
