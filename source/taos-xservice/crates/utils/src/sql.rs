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
