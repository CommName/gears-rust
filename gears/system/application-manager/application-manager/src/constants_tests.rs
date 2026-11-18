use super::*;

#[test]
fn section_key_grammar() {
    for key in ["noop", "a", "app-config_v1.2", "0", "a.b-c_d"] {
        assert!(is_valid_section_key(key), "{key:?} should be valid");
    }
    for key in [
        "",
        "Noop",
        "a b",
        "a/b",
        "\u{43a}\u{43b}",
        "a:b",
        "A",
        "a\n",
    ] {
        assert!(!is_valid_section_key(key), "{key:?} should be invalid");
    }
}
