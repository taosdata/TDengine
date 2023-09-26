#[test]
fn test_slice_overflow() {
    let s = &"abc"[..3.min(4)];
    assert_eq!(s, "abc");
}
