//! The tests for the lib.rs file

use super::*;

#[test]
fn pubsub_error_display() {
    let err = PubSubError::default();
    assert_eq!(CANNED_ERR_MESSAGE, format!("{}", err));

    let err = PubSubError::from_debug(PubSubError::default());
    assert_eq!(
        format!(
            "{}\n Cause: {:?}",
            CANNED_ERR_MESSAGE,
            PubSubError::default()
        ),
        format!("{}", err)
    );

    let err = PubSubError::from_display(PubSubError::default());
    assert_eq!(
        format!("{}\n Cause: {}", CANNED_ERR_MESSAGE, PubSubError::default()),
        format!("{}", err)
    );
}

#[test]
fn message_from_value() {
    let val = String::from("some text");
    let output = StringMessage::from_value(val.clone());
    assert_eq!(output.key, None);
    assert_eq!(output.value, val);
}

#[test]
fn message_from_key_value() {
    let key = Some(String::from("some key"));
    let val = String::from("some text");
    let output = StringMessage::new(key.clone(), val.clone());
    assert_eq!(output.key, key);
    assert_eq!(output.value, val);
}
