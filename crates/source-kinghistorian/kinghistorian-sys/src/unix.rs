use std::collections::HashMap;

use chrono::{DateTime, Utc};

#[derive(Debug)]
pub enum Error {
    KDB,
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub fn api_start_up() -> Result<()> {
    unimplemented!("KingDB not supported on unix platform");
}

pub fn api_cleanup() -> Result<()> {
    unimplemented!("KingDB not supported on unix platform");
}

pub fn api_trace(_filename: &str, _enable: bool) {
    unimplemented!("KingDB not supported on unix platform");
}

pub fn api_version() -> Option<String> {
    unimplemented!("KingDB not supported on unix platform");
}

pub struct ConnectionOptionsBuilder<'a> {
    _a: std::marker::PhantomData<&'a ()>,
}

impl<'a> ConnectionOptionsBuilder<'a> {
    pub fn application_name(self, _application_name: &'a str) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn client_name(self, _client_name: &'a str) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn collector_name(self, _collector_name: &'a str) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn network_timeout_ms(self, _network_timeout_ms: u32) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn session_id(self, _session_id: &'a str) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn build(self) -> ConnectionOptions {
        unimplemented!("KingDB not supported on unix platform");
    }
}

pub struct ConnectionOptions {}

impl ConnectionOptions {
    pub fn builder<'a>(
        _server_name: &'a str,
        _server_port: &'a str,
        _username: &'a str,
        _password: &'a str,
    ) -> ConnectionOptionsBuilder<'a> {
        unimplemented!("KingDB not supported on unix platform");
    }
}

pub struct ServerConnection;

impl ServerConnection {
    pub fn new(_options: ConnectionOptions) -> Result<Self> {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn is_connected(&mut self) -> bool {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn get_server_time(&mut self) -> Result<Option<DateTime<Utc>>> {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn query_tag_values(
        &mut self,
        _criteria: DataCriteria,
    ) -> Result<HashMap<String, Vec<Data>>> {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn tag_exists(&mut self, _tag_name: &str) -> Result<bool> {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn all_tags(&mut self) -> Result<Vec<String>> {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn get_tag_names_by_filter(
        &mut self,
        _tag_name_mask: Option<String>,
        _description_mask: Option<String>,
        _collector_name: Option<String>,
        _source_address: Option<String>,
    ) -> Result<Vec<String>> {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn query_tag_properties(
        &mut self,
        _criteria: TagCriteria,
        _fields: TagFields,
    ) -> Result<Vec<TagProperties>> {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn get_tag_properties(
        &mut self,
        _tag_name: &str,
        _fields: TagFields,
    ) -> Result<TagProperties> {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn get_data_current_value<I, T>(
        &mut self,
        _tag_names: I,
        _digital_as_string: bool,
    ) -> Result<Vec<Result<Data>>>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn data_subscribe<I, T>(
        &mut self,
        _tag_names: I,
        _min_elapsed_ms: u32,
        _sender: Option<flume::Sender<Result<DataRecord>>>,
    ) -> Result<Vec<Result<()>>>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn tag_subscribe<I, T>(
        &mut self,
        _tag_names: I,
        _sender: Option<flume::Sender<(ItemChangeType, TagProperties)>>,
    ) -> Result<Vec<Result<()>>>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn tag_group_get_children(&mut self, _group_id: u32) -> Result<Vec<u32>> {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn tag_group_get_tags(&mut self, _group_id: u32) -> Result<Vec<String>> {
        unimplemented!("KingDB not supported on unix platform");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i16)]
pub enum DataVersion {
    #[default]
    Original = 0,
    Modified = -3,
    Latest = -2,
    All = -1,
}

impl TryFrom<i16> for DataVersion {
    type Error = Error;
    fn try_from(_value: i16) -> Result<Self> {
        unimplemented!("KingDB not supported on unix platform");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum SamplingMode {
    #[default]
    Unknown = 0,
    CurrentValue = 1,
    Interpolated = 2,
    RawByTime = 3,
    RawByNumber = 4,
    Calculated = 5,
    Stepped = 6,
    Trend = 7,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum CalculationMode {
    #[default]
    Unknown = 0,
    Count = 1,
    Average = 2,
    Total = 3,
    Stdev = 4,
    RawTotal = 5,
    RawAverage = 6,
    RawStdev = 7,
    Minimum = 8,
    Maximum = 9,
    MinimumTime = 10,
    MaximumTime = 11,
    DurationGood = 12,
    DurationBad = 13,
    MaximumActualTime = 14,
    MinimumActualTime = 15,
    Start = 16,
    End = 17,
    Delta = 18,
    Range = 19,
    PercentGood = 20,
    PercentBad = 21,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum FilterMode {
    #[default]
    Unknown = 0,
    ExactTime = 1,
    BeforeTime = 2,
    AfterTime = 3,
    BeforeAndAfterTime = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum FilterComparisonMode {
    #[default]
    Unknown = 0,
    Equal = 1,
    NotEqual = 2,
    Less = 3,
    Greater = 4,
    LessEqual = 5,
    GreaterEqual = 6,
}

#[derive(Debug, Default)]
pub enum Value {
    #[default]
    Empty,
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    Str(String),
    WStr(Vec<String>),
    Blob(Vec<u8>),
    FileTime(FileTime),
    Timestamp(DateTime<Utc>),
    Var(Box<Value>),
    Dec(Dec),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ValueType {
    Empty = 0,
    Bool = 1,
    I8 = 2,
    I16 = 3,
    I32 = 4,
    I64 = 5,
    U8 = 6,
    U16 = 7,
    U32 = 8,
    U64 = 9,
    F32 = 10,
    F64 = 11,
    Str = 12,
    WStr = 13,
    Blob = 14,
    FileTime = 15,
    Timestamp = 16,
    Var = 17,
    Dec = 18,
}

impl From<u16> for ValueType {
    fn from(_value: u16) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }
}

#[derive(Debug)]
pub struct Dec;

#[derive(Debug)]
pub struct FileTime;

pub struct DataCriteriaBuilder<'a> {
    _s: &'a str,
}

impl<'a> DataCriteriaBuilder<'a> {
    pub fn start_time(self, _start_time: DateTime<Utc>) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn end_time(self, _end_time: DateTime<Utc>) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn data_version(self, _data_version: DataVersion) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn sampling_mode(self, _sampling_mode: SamplingMode) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn sampling_number(self, _sampling_number: u32) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn sampling_interval_ms(self, _sampling_interval_ms: u64) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn calculation_mode(self, _calculation_mode: CalculationMode) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn filter_tag(self, _filter_tag: &'a str) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn filter_mode(self, _filter_mode: FilterMode) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn filter_comparison_mode(self, _mode: FilterComparisonMode) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn filter_comparison_value(self, _value: Value) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn row_count(self, _row_count: u32) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn digital_as_string(self, _digital_as_string: bool) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    // 可选：提供一个 build() 方法返回最终的 DataCriteria（如果你有目标结构）
    pub fn build(self) -> DataCriteria {
        unimplemented!("KingDB not supported on unix platform");
    }
}

pub struct DataCriteria {}

impl DataCriteria {
    pub fn builder<'a>(_tag_names: &'a [&'a str]) -> DataCriteriaBuilder<'a> {
        unimplemented!("KingDB not supported on unix platform");
    }
}

#[derive(Debug)]
pub struct DataRecord {}

impl DataRecord {
    pub fn tag_name(&self) -> Option<String> {
        unimplemented!("KingDB not supported on unix platform");
    }
}

#[derive(Debug)]
pub struct Data {
    pub timestamp: Option<DateTime<Utc>>,
    pub version: Option<DataVersion>,
    pub quality: u32,
    pub value: Value,
}

#[derive(Debug, Default)]
pub struct TagCriteriaBuilder<'a> {
    _a: &'a str,
}

impl<'a> TagCriteriaBuilder<'a> {
    pub fn tag_name_mask(self, _mask: &'a str) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn tag_names(self, _names: &'a [&'a str]) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn push_tag_name(self, _name: &'a str) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn description_mask(self, _mask: &'a str) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn collector_name(self, _name: &'a str) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn source_address(self, _addr: &'a str) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn build(self) -> TagCriteria {
        unimplemented!("KingDB not supported on unix platform");
    }
}

pub struct TagCriteria;

impl TagCriteria {
    pub fn builder<'a>() -> TagCriteriaBuilder<'a> {
        unimplemented!("KingDB not supported on unix platform");
    }
}

#[derive(Debug, Default)]
pub struct TagFields;

#[derive(Debug, Default)]
pub struct TagFieldsBuilder;

impl TagFieldsBuilder {
    pub fn all_fields(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn tag_name(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn engineering_unit(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn description(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn tag_id(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn digital_set_id(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn collector_name(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn collector_type(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn source_address(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn data_type(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn data_length(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn collection_control(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn collection_mode(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn collection_interval(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn collection_offset(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn timestamp_type(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn time_zone_bias(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn time_adjustment(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn max_value(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn min_value(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn input_conversion(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn max_raw(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn min_raw(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn collector_compression(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn collector_compression_mode(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn collector_absolute_deadbanding(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn collector_deadband_percent(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn collector_absolute_deadband(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn collector_compression_timeout(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn collector_compression_timeout_min(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn archive_control(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn archive_version_support(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn archive_shutdown(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn archive_step_value(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn archive_store_mode(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn archive_compression(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn archive_absolute_deadbanding(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn archive_compression_mode(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn archive_deadband_percent(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn archive_absolute_deadband(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn archive_compression_timeout(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn archive_compression_timeout_min(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn security_read_role(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn security_write_role(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn security_admin_role(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn create_time(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn last_modified(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn create_user(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn last_modified_user(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn electronic_record(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn calculation(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn calculation_triggers(self) -> Self {
        unimplemented!("KingDB not supported on unix platform");
    }

    pub fn build(self) -> TagFields {
        unimplemented!("KingDB not supported on unix platform");
    }
}

impl TagFields {
    pub fn builder() -> TagFieldsBuilder {
        unimplemented!("KingDB not supported on unix platform");
    }
}

#[derive(Debug)]
pub struct TagProperties {
    pub tag_name: Option<String>,
    pub engineering_unit: Option<String>,
    pub description: Option<String>,
    pub tag_id: Option<i32>,
    pub digital_set_id: Option<i16>,
    pub collector_name: Option<String>,
    pub collector_type: Option<CollectorType>,
    pub source_address: Option<String>,
    pub data_type: Option<HistoryDataType>,
    pub data_length: Option<i32>,
    pub collection_control: Option<bool>,
    pub collection_mode: Option<CollectionMode>,
    pub collection_interval: Option<i32>,
    pub collection_offset: Option<i32>,
    pub timestamp_type: Option<TimestampType>,
    pub time_zone_bias: Option<i32>,
    pub time_adjustment: Option<i32>,
    pub max_value: Option<f64>,
    pub min_value: Option<f64>,
    pub input_conversion: Option<InputConversion>,
    pub max_raw: Option<f64>,
    pub min_raw: Option<f64>,
    pub collector_compression: Option<bool>,
    pub collector_compression_mode: Option<i8>,
    pub collector_absolute_deadbanding: Option<bool>,
    pub collector_deadband_percent: Option<f32>,
    pub collector_absolute_deadband: Option<f64>,
    pub collector_compression_timeout: Option<i32>,
    pub collector_compression_timeout_min: Option<i32>,
    pub archive_control: Option<bool>,
    pub archive_version_support: Option<bool>,
    pub archive_shutdown: Option<bool>,
    pub archive_step_value: Option<bool>,
    pub archive_store_mode: Option<i8>,
    pub archive_compression: Option<bool>,
    pub archive_absolute_deadbanding: Option<bool>,
    pub archive_compression_mode: Option<i8>,
    pub archive_deadband_percent: Option<f32>,
    pub archive_absolute_deadband: Option<f64>,
    pub archive_compression_timeout: Option<i32>,
    pub archive_compression_timeout_min: Option<i32>,
    pub security_read_role: Option<String>,
    pub security_write_role: Option<String>,
    pub security_admin_role: Option<String>,
    pub create_time: Option<DateTime<Utc>>,
    pub last_modified: Option<DateTime<Utc>>,
    pub create_user: Option<String>,
    pub last_modified_user: Option<String>,
    pub electronic_record: Option<i32>,
    pub calculation: Option<String>,
    pub calculation_triggers: Option<Vec<String>>,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectorType {
    Unknown = 0,
    CalculationEngine = 1,
    Opc = 2,
    AlarmServer = 3,
    Kingview = 4,
    File = 5,
    ServerToServer = 6,
    KingviewLab = 7,
    Simulation = 8,
    Manual = 9,
    Other = 10,
    PiCollector = 11,
    PiDistributor = 12,
    KingIoServer = 13,
    IfixCollector = 14,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryDataType {
    Empty = 0,
    Boolean = 1,
    Int8 = 2,
    Int16 = 3,
    Int32 = 4,
    Int64 = 5,
    Float32 = 6,
    Float64 = 7,
    Decimal = 9,
    Char = 10,
    Varchar = 11,
    Nchar = 13,
    Nvarchar = 14,
    Timestamp = 18,
    Binary = 19,
    Varbinary = 20,
    Digital = 101,
    Float16 = 102,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectionMode {
    Unknown = 0,
    Unsolicited = 1,
    Polled = 2,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampType {
    Source = 1,
    Collector = 2,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputConversion {
    NoConversion = 0,
    Linear = 1,
    Sqrt = 2,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemChangeType {
    NoChange = 0,
    Added = 1,
    Deleted = 2,
    Modified = 3,
}
