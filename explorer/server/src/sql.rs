use std::sync::LazyLock;

use regex::Regex;

pub(crate) fn need_limit(sql: &str) -> bool {
    let sql = sql.trim().to_uppercase();
    if !sql.starts_with("SELECT") {
        return false;
    }
    static RE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"LIMIT\s+(\d+)(?:\s*(?:,\s*(\d+)|OFFSET\s+(\d+)))?\b").unwrap()
    });

    !RE.is_match(&sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_has_limit_test() {
        assert!(need_limit("select * from test;"));
        assert!(!need_limit("select * from test limit 5, 2;"));
        assert!(!need_limit("select * from test limit 2 offset 5;"));
        assert!(!need_limit("select * from test limit 5;"));

        assert!(!need_limit(
            "INSERT INTO d1001 USING meters TAGS('Beijing.Chaoyang', 2) VALUES('a');"
        ));
        assert!(!need_limit("delete from test"));
        assert!(need_limit("select * from test where a like '% LIMIT %'"));
    }
}
