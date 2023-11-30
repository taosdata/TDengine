use rhai::ImmutableString;

#[allow(dead_code)]
pub fn custom_starts_with(s: ImmutableString, pattern: ImmutableString) -> bool {
    s.starts_with(pattern.as_str())
}
