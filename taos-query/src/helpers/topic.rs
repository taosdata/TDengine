use chrono::NaiveDateTime;
use paste::paste;
use serde::{Deserialize, Serialize};

use std::{fmt::Display, str::FromStr};

use crate::common::{Precision, Timestamp};

/// Information for `show topics` record.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Topic {
    topic_name: String,
    db_name: String,
    create_time: Timestamp,
    sql: String,
}

impl Topic {
    /// Topic name.
    pub fn name(&self) -> &str {
        &self.topic_name
    }

    /// Database name of the topic.
    pub fn db_name(&self) -> &str {
        &self.db_name
    }

    /// Created time of the topic
    pub fn create_time(&self) -> NaiveDateTime {
        self.create_time.to_naive_datetime()
    }

    /// The create sql for the topic
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Check if the topic is a database-scope topic, otherwise is table-scope topic.
    pub fn is_db_topic(&self) -> bool {
        self.sql.contains("as database")
    }

    pub fn is_stable_topic(&self) -> bool {
        self.sql.contains("as stable")
    }
}
