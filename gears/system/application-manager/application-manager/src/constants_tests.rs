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

#[test]
fn cut_keeps_whole_characters() {
    assert_eq!(cut("abc", 3), "abc");
    assert_eq!(cut("abc", 2), "ab");
    assert_eq!(cut("abc", 0), "");
    // "é" is two bytes: a cut inside it keeps only the whole characters.
    assert_eq!(cut("a\u{e9}", 2), "a");
    assert_eq!(cut("a\u{e9}", 3), "a\u{e9}");
}

#[test]
fn the_sweeper_expires_more_often_than_it_purges() {
    assert!(!SWEEP_INTERVAL.is_zero());
    assert!(SWEEP_INTERVAL < PURGE_INTERVAL);
}
