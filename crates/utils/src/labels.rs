use std::collections::HashMap;

/// Error type for label parsing failures
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LabelParseError;

impl std::fmt::Display for LabelParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid label format, expected 'key::value'")
    }
}

impl std::error::Error for LabelParseError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Label<'a>(&'a str, &'a str);

impl<'a> Label<'a> {
    /// Separator for label format
    pub const SEPARATOR: &'static str = "::";

    /// Create a new label from key and value
    pub fn new(key: &'a str, value: &'a str) -> Self {
        Self(key, value)
    }

    /// Get the key
    pub fn key(&self) -> &'a str {
        self.0
    }

    /// Get the value
    pub fn value(&self) -> &'a str {
        self.1
    }

    /// Parse a label from string in "key::value" format
    pub fn parse(s: &'a str) -> Result<Self, LabelParseError> {
        s.split_once(Self::SEPARATOR)
            .map(|(key, value)| Self::new(key, value))
            .ok_or(LabelParseError)
    }
}

impl<'a> std::fmt::Display for Label<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}{}", self.0, Self::SEPARATOR, self.1)
    }
}

impl<'a> std::convert::From<(&'a str, &'a str)> for Label<'a> {
    fn from(value: (&'a str, &'a str)) -> Self {
        Self::new(value.0, value.1)
    }
}

impl<'a> std::convert::From<Label<'a>> for (&'a str, &'a str) {
    fn from(value: Label<'a>) -> Self {
        (value.0, value.1)
    }
}

impl<'a> std::convert::From<&Label<'a>> for (&'a str, &'a str) {
    fn from(value: &Label<'a>) -> Self {
        (value.0, value.1)
    }
}

/// Struct for filtering JSON values, supporting chain calls and nested path matching
#[derive(Debug, Default, Clone)]
pub struct LabelFilter {
    filters: HashMap<String, serde_json::Value>,
}

impl LabelFilter {
    /// Create a new LabelFilter
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a filter condition, supporting chain calls
    ///
    /// # Examples
    /// ```
    /// use taosx_utils::labels::LabelFilter;
    ///
    /// let filter = LabelFilter::default()
    ///     .with("key1", "value1")
    ///     .with("nested.key", "value2");
    /// ```
    pub fn with(mut self, key: impl Into<String>, value: impl Into<serde_json::Value>) -> Self {
        self.filters.insert(key.into(), value.into());
        self
    }

    /// Check if the given JSON value matches all filter conditions
    ///
    /// # Examples
    /// ```
    /// use taosx_utils::labels::LabelFilter;
    ///
    /// let filter = LabelFilter::default()
    ///     .with("a", "b");
    ///
    /// let json = serde_json::json!({"a": "b"});
    /// assert!(filter.matches(&json));
    /// ```
    pub fn matches(&self, value: &serde_json::Value) -> bool {
        self.filters
            .iter()
            .all(|(key, expected_value)| Self::match_value(value, key, expected_value))
    }

    /// Use dot notation paths to match values in JSON
    fn match_value(json: &serde_json::Value, path: &str, expected: &serde_json::Value) -> bool {
        let mut current = json;

        // Traverse each path segment without allocating a Vec
        for part in path.split('.') {
            match current.get(part) {
                Some(next) => current = next,
                None => return false,
            }
        }

        // Compare the final value
        current == expected
    }
}

/// Parse labels from a comma-separated string
///
/// Empty strings and blank entries are ignored.
///
/// # Examples
/// ```
/// use taosx_utils::labels::parse_labels;
///
/// let pairs = parse_labels("key1::value1,key2::value2");
/// assert_eq!(pairs.len(), 2);
///
/// assert_eq!(parse_labels("").len(), 0);
/// ```
pub fn parse_labels(labels: &str) -> Vec<Label<'_>> {
    parse_label_pairs(labels.split(',').map(str::trim).filter(|s| !s.is_empty()))
}

/// Parse label pairs from a string iterator (key::value format)
///
/// # Examples
/// ```
/// use taosx_utils::labels::parse_label_pairs;
///
/// let labels = vec!["key1::value1", "key2::value2"];
/// let pairs = parse_label_pairs(labels);
/// assert_eq!(pairs.len(), 2);
/// ```
pub fn parse_label_pairs<'a, I>(labels: I) -> Vec<Label<'a>>
where
    I: IntoIterator<Item = &'a str>,
{
    let labels = labels.into_iter();
    let mut kvs = Vec::with_capacity(labels.size_hint().1.unwrap_or(labels.size_hint().0));
    for label in labels {
        if let Ok(l) = Label::parse(label) {
            kvs.push(l);
        }
    }
    kvs
}

/// Internal helper: insert parsed `key::value` pairs into a JSON map.
fn insert_labels_into_map<'a>(
    map: &mut serde_json::Map<String, serde_json::Value>,
    labels: impl Iterator<Item = &'a str>,
) {
    for label in labels {
        if let Ok(l) = Label::parse(label) {
            map.insert(
                l.key().to_string(),
                serde_json::Value::String(l.value().to_string()),
            );
        }
    }
}

/// Build a JSON object from a list of labels
///
/// Supports both `&[&str]` and `&[String]` types
///
/// # Examples
/// ```
/// use taosx_utils::labels::build_json_labels_from_iter;
///
/// // Using &[&str]
/// let labels = vec!["key1::value1", "key2::value2"];
/// let json = build_json_labels_from_iter(&labels);
/// assert_eq!(json.get("key1").and_then(|v| v.as_str()), Some("value1"));
///
/// // Using &[String]
/// let labels_owned = vec![String::from("key1::value1"), String::from("key2::value2")];
/// let json = build_json_labels_from_iter(&labels_owned);
/// assert_eq!(json.get("key1").and_then(|v| v.as_str()), Some("value1"));
/// ```
pub fn build_json_labels_from_iter<T: AsRef<str>>(labels: &[T]) -> serde_json::Value {
    let mut map = serde_json::Map::with_capacity(labels.len());
    insert_labels_into_map(&mut map, labels.iter().map(|s| s.as_ref()));
    serde_json::Value::Object(map)
}

/// Extract label pairs from a JSON object (only extracts string values)
///
/// # Examples
/// ```
/// use taosx_utils::labels::extract_label_pairs;
///
/// let json = serde_json::json!({"key1": "value1", "key2": "value2"});
/// let pairs = extract_label_pairs(&json);
/// assert_eq!(pairs.len(), 2);
/// ```
pub fn extract_label_pairs(json: &serde_json::Value) -> Vec<Label<'_>> {
    let Some(json) = json.as_object() else {
        return Vec::new();
    };
    let mut kvs = Vec::with_capacity(json.len());
    for (k, v) in json {
        let serde_json::Value::String(v) = v else {
            continue;
        };
        kvs.push((k.as_str(), v.as_str()).into());
    }
    kvs
}

/// Build a JSON object from a comma-separated string
///
/// Internally calls `parse_labels(labels)` to build the JSON object directly
///
/// # Examples
/// ```
/// use taosx_utils::labels::build_json_labels_from_string;
///
/// let json = build_json_labels_from_string("service::api,env::prod");
/// assert_eq!(json.get("service").and_then(|v| v.as_str()), Some("api"));
/// assert_eq!(json.get("env").and_then(|v| v.as_str()), Some("prod"));
/// ```
pub fn build_json_labels_from_string(labels: &str) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    insert_labels_into_map(&mut map, labels.split(',').filter(|s| !s.is_empty()));
    serde_json::Value::Object(map)
}

/// Extract labels from JSON and convert to string format
///
/// # Examples
/// ```
/// use taosx_utils::labels::extract_labels_as_strings;
///
/// let json = serde_json::json!({"key1": "value1", "key2": "value2"});
/// let labels = extract_labels_as_strings(&json);
/// assert_eq!(labels.len(), 2);
/// assert!(labels.contains(&"key1::value1".to_string()));
/// assert!(labels.contains(&"key2::value2".to_string()));
/// ```
pub fn extract_labels_as_strings(json: &serde_json::Value) -> Vec<String> {
    extract_label_pairs(json)
        .into_iter()
        .map(|label| label.to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== LabelFilter Tests ====================

    #[test]
    fn test_label_filter_default() {
        let filter = LabelFilter::default();
        assert_eq!(filter.filters.len(), 0);
    }

    #[test]
    fn test_label_filter_new() {
        let filter = LabelFilter::new();
        assert_eq!(filter.filters.len(), 0);
    }

    #[test]
    fn test_label_filter_with_single_condition() {
        let filter = LabelFilter::default().with("key", "value");
        assert_eq!(filter.filters.len(), 1);
    }

    #[test]
    fn test_label_filter_with_multiple_conditions() {
        let filter = LabelFilter::default()
            .with("key1", "value1")
            .with("key2", "value2")
            .with("key3", "value3");
        assert_eq!(filter.filters.len(), 3);
    }

    #[test]
    fn test_label_filter_simple_match() {
        let filter = LabelFilter::default().with("a", "b");
        let json = serde_json::json!({"a": "b"});
        assert!(filter.matches(&json));
    }

    #[test]
    fn test_label_filter_simple_no_match() {
        let filter = LabelFilter::default().with("a", "b");
        let json = serde_json::json!({"a": "c"});
        assert!(!filter.matches(&json));
    }

    #[test]
    fn test_label_filter_simple_key_missing() {
        let filter = LabelFilter::default().with("a", "b");
        let json = serde_json::json!({"c": "b"});
        assert!(!filter.matches(&json));
    }

    #[test]
    fn test_label_filter_nested_match() {
        let filter = LabelFilter::default().with("a.b", "c");
        let json = serde_json::json!({"a": {"b": "c"}});
        assert!(filter.matches(&json));
    }

    #[test]
    fn test_label_filter_nested_no_match() {
        let filter = LabelFilter::default().with("a.b", "c");
        let json = serde_json::json!({"a": {"b": "d"}});
        assert!(!filter.matches(&json));
    }

    #[test]
    fn test_label_filter_nested_key_missing() {
        let filter = LabelFilter::default().with("a.b", "c");
        let json = serde_json::json!({"a": {"c": "c"}});
        assert!(!filter.matches(&json));
    }

    #[test]
    fn test_label_filter_deeply_nested_match() {
        let filter = LabelFilter::default().with("a.b.c.d", "value");
        let json = serde_json::json!({"a": {"b": {"c": {"d": "value"}}}});
        assert!(filter.matches(&json));
    }

    #[test]
    fn test_label_filter_deeply_nested_no_match() {
        let filter = LabelFilter::default().with("a.b.c.d", "value");
        let json = serde_json::json!({"a": {"b": {"c": {"d": "wrong"}}}});
        assert!(!filter.matches(&json));
    }

    #[test]
    fn test_label_filter_multiple_conditions_all_match() {
        let filter = LabelFilter::default()
            .with("a", "1")
            .with("b", "2")
            .with("c", "3");
        let json = serde_json::json!({"a": "1", "b": "2", "c": "3"});
        assert!(filter.matches(&json));
    }

    #[test]
    fn test_label_filter_multiple_conditions_one_mismatch() {
        let filter = LabelFilter::default()
            .with("a", "1")
            .with("b", "2")
            .with("c", "3");
        let json = serde_json::json!({"a": "1", "b": "2", "c": "wrong"});
        assert!(!filter.matches(&json));
    }

    #[test]
    fn test_label_filter_multiple_conditions_with_nested() {
        let filter = LabelFilter::default().with("a", "1").with("b.c", "2");
        let json = serde_json::json!({"a": "1", "b": {"c": "2"}});
        assert!(filter.matches(&json));
    }

    #[test]
    fn test_label_filter_empty_filter_matches_any() {
        let filter = LabelFilter::default();
        let json = serde_json::json!({"a": "b"});
        assert!(filter.matches(&json));
    }

    #[test]
    fn test_label_filter_with_string_values() {
        let filter = LabelFilter::default().with("key", "string_value");
        let json = serde_json::json!({"key": "string_value"});
        assert!(filter.matches(&json));
    }

    #[test]
    fn test_label_filter_with_numeric_values() {
        let filter = LabelFilter::default().with("key", 42);
        let json = serde_json::json!({"key": 42});
        assert!(filter.matches(&json));
    }

    #[test]
    fn test_label_filter_with_boolean_values() {
        let filter = LabelFilter::default().with("key", true);
        let json = serde_json::json!({"key": true});
        assert!(filter.matches(&json));
    }

    #[test]
    fn test_label_filter_clone() {
        let filter1 = LabelFilter::default().with("a", "b");
        let filter2 = filter1.clone();
        assert!(filter2.matches(&serde_json::json!({"a": "b"})));
    }

    #[test]
    fn test_label_filter_multi() {
        let filter = LabelFilter::default().with("a", "b");
        let json = serde_json::json!({"a": "b", "b": "c"});
        assert!(filter.matches(&json));
    }

    // ==================== parse_label_pairs Tests ====================

    #[test]
    fn test_parse_label_pairs_valid() {
        let labels = vec!["key1::value1", "key2::value2", "key3::value3"];
        let result = parse_label_pairs(labels);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], ("key1", "value1").into());
        assert_eq!(result[1], ("key2", "value2").into());
        assert_eq!(result[2], ("key3", "value3").into());
    }

    #[test]
    fn test_parse_label_pairs_empty() {
        let labels: Vec<&str> = vec![];
        let result = parse_label_pairs(labels);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_parse_label_pairs_invalid_format() {
        let labels = vec!["invalid_format"];
        let result = parse_label_pairs(labels);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_parse_label_pairs_mixed_valid_invalid() {
        let labels = vec!["key1::value1", "invalid", "key2::value2"];
        let result = parse_label_pairs(labels);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ("key1", "value1").into());
        assert_eq!(result[1], ("key2", "value2").into());
    }

    #[test]
    fn test_parse_label_pairs_with_empty_value() {
        let labels = vec!["key::"];
        let result = parse_label_pairs(labels);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], ("key", "").into());
    }

    #[test]
    fn test_parse_label_pairs_with_empty_key() {
        let labels = vec!["::value"];
        let result = parse_label_pairs(labels);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], ("", "value").into());
    }

    #[test]
    fn test_parse_label_pairs_multiple_separators() {
        let labels = vec!["key::value::extra"];
        let result = parse_label_pairs(labels);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], ("key", "value::extra").into());
    }

    // ==================== build_json_from_labels Tests ====================

    #[test]
    fn test_build_json_from_labels_single_label() {
        let labels = vec!["key::value"];
        let json = build_json_labels_from_iter(&labels);
        assert_eq!(json.get("key").and_then(|v| v.as_str()), Some("value"));
    }

    #[test]
    fn test_build_json_from_labels_multiple_labels() {
        let labels = vec!["key1::value1", "key2::value2"];
        let json = build_json_labels_from_iter(&labels);
        assert_eq!(json.get("key1").and_then(|v| v.as_str()), Some("value1"));
        assert_eq!(json.get("key2").and_then(|v| v.as_str()), Some("value2"));
    }

    #[test]
    fn test_build_json_from_labels_empty() {
        let labels: Vec<&str> = vec![];
        let json = build_json_labels_from_iter(&labels);
        assert!(json.is_object());
        assert_eq!(json.as_object().unwrap().len(), 0);
    }

    #[test]
    fn test_build_json_from_labels_with_invalid_format() {
        let labels = vec!["key1::value1", "invalid"];
        let json = build_json_labels_from_iter(&labels);
        assert_eq!(json.as_object().unwrap().len(), 1);
        assert_eq!(json.get("key1").and_then(|v| v.as_str()), Some("value1"));
    }

    #[test]
    fn test_build_json_from_labels_duplicate_keys() {
        let labels = vec!["key::value1", "key::value2"];
        let json = build_json_labels_from_iter(&labels);
        // Later value overrides earlier value
        assert_eq!(json.get("key").and_then(|v| v.as_str()), Some("value2"));
    }

    #[test]
    fn test_build_json_from_labels_with_string_vec() {
        let labels = vec![String::from("key1::value1"), String::from("key2::value2")];
        let json = build_json_labels_from_iter(&labels);
        assert_eq!(json.get("key1").and_then(|v| v.as_str()), Some("value1"));
        assert_eq!(json.get("key2").and_then(|v| v.as_str()), Some("value2"));
    }

    #[test]
    fn test_build_json_from_labels_with_owned_strings() {
        let labels = [String::from("service::api"), String::from("env::prod")];
        let json = build_json_labels_from_iter(&labels);
        assert_eq!(json.get("service").and_then(|v| v.as_str()), Some("api"));
        assert_eq!(json.get("env").and_then(|v| v.as_str()), Some("prod"));
    }

    #[test]
    fn test_build_json_from_labels_mixed_type_compatibility() {
        // Test &[&str] type
        let str_refs = vec!["key::value"];
        let json1 = build_json_labels_from_iter(&str_refs);

        // Test &[String] type
        let owned_strings = vec![String::from("key::value")];
        let json2 = build_json_labels_from_iter(&owned_strings);

        // Both methods should produce the same result
        assert_eq!(
            json1.get("key").and_then(|v| v.as_str()),
            json2.get("key").and_then(|v| v.as_str())
        );
    }

    // ==================== extract_label_pairs Tests ====================

    #[test]
    fn test_extract_label_pairs_single_string_value() {
        let json = serde_json::json!({"key": "value"});
        let result = extract_label_pairs(&json);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], ("key", "value").into());
    }

    #[test]
    fn test_extract_label_pairs_multiple_string_values() {
        let json = serde_json::json!({"key1": "value1", "key2": "value2"});
        let result = extract_label_pairs(&json);
        assert_eq!(result.len(), 2);
        // HashMap order is not guaranteed, so check containment
        let result_map: std::collections::HashMap<_, _> =
            result.into_iter().map(|l| (l.0, l.1)).collect();
        assert_eq!(result_map.get("key1").copied(), Some("value1"));
        assert_eq!(result_map.get("key2").copied(), Some("value2"));
    }

    #[test]
    fn test_extract_label_pairs_skip_non_string_values() {
        let json = serde_json::json!({"string_key": "value", "number_key": 42, "bool_key": true});
        let result = extract_label_pairs(&json);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], ("string_key", "value").into());
    }

    #[test]
    fn test_extract_label_pairs_empty_object() {
        let json = serde_json::json!({});
        let result = extract_label_pairs(&json);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_extract_label_pairs_non_object() {
        let json = serde_json::json!("not an object");
        let result = extract_label_pairs(&json);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_extract_label_pairs_array() {
        let json = serde_json::json!([1, 2, 3]);
        let result = extract_label_pairs(&json);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_extract_label_pairs_null() {
        let json = serde_json::Value::Null;
        let result = extract_label_pairs(&json);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_extract_label_pairs_nested_object_ignored() {
        let json = serde_json::json!({"key": "value", "nested": {"inner": "data"}});
        let result = extract_label_pairs(&json);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], ("key", "value").into());
    }

    #[test]
    fn test_extract_label_pairs_empty_string_value() {
        let json = serde_json::json!({"key": ""});
        let result = extract_label_pairs(&json);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], ("key", "").into());
    }

    // ==================== Integration Tests ====================

    #[test]
    fn test_integration_extract_and_filter() {
        let labels = vec!["service::api", "env::prod"];
        let json = build_json_labels_from_iter(&labels);

        let filter = LabelFilter::default()
            .with("service", "api")
            .with("env", "prod");

        assert!(filter.matches(&json));
    }

    #[test]
    fn test_integration_extract_and_filter_no_match() {
        let labels = vec!["service::api", "env::prod"];
        let json = build_json_labels_from_iter(&labels);

        let filter = LabelFilter::default()
            .with("service", "api")
            .with("env", "dev");

        assert!(!filter.matches(&json));
    }

    #[test]
    fn test_integration_roundtrip() {
        let labels = vec!["key1::value1", "key2::value2", "key3::value3"];
        let json = build_json_labels_from_iter(&labels);
        let extracted = extract_label_pairs(&json);

        assert_eq!(extracted.len(), 3);
    }

    #[test]
    fn test_parse_labels_from_string() {
        let labels_str = "key1::value1,key2::value2,key3::value3";
        let result = parse_labels(labels_str);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], ("key1", "value1").into());
        assert_eq!(result[1], ("key2", "value2").into());
        assert_eq!(result[2], ("key3", "value3").into());
    }

    #[test]
    fn test_parse_labels_empty_string() {
        let labels_str = "";
        let result = parse_labels(labels_str);
        assert_eq!(result.len(), 0);
    }

    // ==================== build_json_from_string Tests ====================

    #[test]
    fn test_build_json_from_string_single_label() {
        let json = build_json_labels_from_string("key::value");
        assert_eq!(json.get("key").and_then(|v| v.as_str()), Some("value"));
    }

    #[test]
    fn test_build_json_from_string_multiple_labels() {
        let json = build_json_labels_from_string("service::api,env::prod,region::us-west");
        assert_eq!(json.get("service").and_then(|v| v.as_str()), Some("api"));
        assert_eq!(json.get("env").and_then(|v| v.as_str()), Some("prod"));
        assert_eq!(json.get("region").and_then(|v| v.as_str()), Some("us-west"));
    }

    #[test]
    fn test_build_json_from_string_empty_string() {
        let json = build_json_labels_from_string("");
        assert!(json.is_object());
        assert_eq!(json.as_object().unwrap().len(), 0);
    }

    #[test]
    fn test_build_json_from_string_with_spaces() {
        let json = build_json_labels_from_string("key1::value 1,key2::value 2");
        assert_eq!(json.get("key1").and_then(|v| v.as_str()), Some("value 1"));
        assert_eq!(json.get("key2").and_then(|v| v.as_str()), Some("value 2"));
    }

    #[test]
    fn test_build_json_from_string_with_special_characters() {
        let json = build_json_labels_from_string("url::http://example.com,path::/api/v1");
        assert_eq!(
            json.get("url").and_then(|v| v.as_str()),
            Some("http://example.com")
        );
        assert_eq!(json.get("path").and_then(|v| v.as_str()), Some("/api/v1"));
    }

    #[test]
    fn test_build_json_from_string_with_invalid_format() {
        let json = build_json_labels_from_string("key1::value1,invalid_format,key2::value2");
        assert_eq!(json.as_object().unwrap().len(), 2);
        assert_eq!(json.get("key1").and_then(|v| v.as_str()), Some("value1"));
        assert_eq!(json.get("key2").and_then(|v| v.as_str()), Some("value2"));
    }

    #[test]
    fn test_build_json_from_string_with_empty_value() {
        let json = build_json_labels_from_string("key1::,key2::value2");
        assert_eq!(json.get("key1").and_then(|v| v.as_str()), Some(""));
        assert_eq!(json.get("key2").and_then(|v| v.as_str()), Some("value2"));
    }

    #[test]
    fn test_build_json_from_string_direct_to_filter() {
        let json = build_json_labels_from_string("service::api,env::prod");
        let filter = LabelFilter::default()
            .with("service", "api")
            .with("env", "prod");
        assert!(filter.matches(&json));
    }

    #[test]
    fn test_build_json_from_string_to_extract_roundtrip() {
        let original = "key1::value1,key2::value2,key3::value3";
        let json = build_json_labels_from_string(original);
        let extracted = extract_label_pairs(&json);
        assert_eq!(extracted.len(), 3);
    }

    #[test]
    fn test_extract_labels_as_strings() {
        let json = serde_json::json!({"key1": "value1", "key2": "value2"});
        let labels = extract_labels_as_strings(&json);
        assert_eq!(labels.len(), 2);
        assert!(labels.contains(&"key1::value1".to_string()));
        assert!(labels.contains(&"key2::value2".to_string()));
    }

    #[test]
    fn test_extract_labels_as_strings_empty() {
        let json = serde_json::json!({});
        let labels = extract_labels_as_strings(&json);
        assert!(labels.is_empty());
    }

    #[test]
    fn test_extract_labels_as_strings_with_non_string() {
        let json = serde_json::json!({"key1": "value1", "number": 42, "bool": true});
        let labels = extract_labels_as_strings(&json);
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0], "key1::value1");
    }
}
