use std::fmt::Write;

/// Escape a string value for SQL.
pub struct SingleQuoteSqlValueEscaped<'a>(&'a str);

impl std::fmt::Display for SingleQuoteSqlValueEscaped<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = self.0;
        f.write_char('\'')?;

        for c in value.chars() {
            match c {
                '\0' => {
                    // taosc uses C escape syntax for SQL which not support null byte escape,
                    // so we need to ignore null byte.
                }
                '\'' => {
                    f.write_char('\'')?;
                    f.write_char('\'')?;
                }

                '\t' => {
                    f.write_char('\\')?;
                    f.write_char('t')?;
                }
                '\r' => {
                    f.write_char('\\')?;
                    f.write_char('r')?;
                }
                '\n' => {
                    f.write_char('\\')?;
                    f.write_char('n')?;
                }
                '\\' | '"' => {
                    f.write_char('\\')?;
                    f.write_char(c)?;
                }
                _ => {
                    f.write_char(c)?;
                }
            }
        }
        f.write_char('\'')
    }
}

pub fn sql_value_escaped_fmt(value: &str) -> SingleQuoteSqlValueEscaped<'_> {
    SingleQuoteSqlValueEscaped(value)
}
/// Escape a string value for SQL.
pub fn sql_value_escape(value: &str) -> String {
    SingleQuoteSqlValueEscaped(value).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_value_escape_wraps_plain_text_in_single_quotes() {
        assert_eq!(sql_value_escape("abc"), "'abc'");
        assert_eq!(sql_value_escape(""), "''");
    }

    #[test]
    fn sql_value_escape_doubles_single_quotes() {
        assert_eq!(sql_value_escape("it's ok"), "'it''s ok'");
    }

    #[test]
    fn sql_value_escape_backslash_escapes_control_chars_and_quotes() {
        assert_eq!(
            sql_value_escape("a\tb\rc\nd\\e\"f"),
            "'a\\tb\\rc\\nd\\\\e\\\"f'"
        );
    }

    #[test]
    fn sql_value_escape_omits_null_bytes() {
        assert_eq!(sql_value_escape("a\0b"), "'ab'");
    }

    #[test]
    fn sql_value_escaped_fmt_can_be_used_with_format_args() {
        let escaped = format!("values ({})", sql_value_escaped_fmt("x'y"));

        assert_eq!(escaped, "values ('x''y')");
    }
}
