use super::*;

#[test]
fn error_display_text_of_each_variant() {
    assert_eq!(
        SectionPluginError::Rejected {
            detail: "bad".to_owned(),
            location: Some("/a".to_owned()),
        }
        .to_string(),
        "section rejected: bad"
    );
    assert_eq!(
        SectionPluginError::NotPermitted {
            detail: "no".to_owned(),
        }
        .to_string(),
        "not permitted: no"
    );
    assert_eq!(
        SectionPluginError::Internal {
            message: "boom".to_owned(),
        }
        .to_string(),
        "internal plugin error: boom"
    );
}
