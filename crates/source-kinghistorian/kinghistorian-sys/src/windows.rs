#![allow(non_snake_case)]

use std::{collections::HashMap, sync::OnceLock};

use chrono::{DateTime, Utc};
use dlopen2::wrapper::{Container, WrapperApi};

#[allow(clippy::all)]
#[allow(warnings)]
mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}
pub mod error;

#[derive(Debug)]
pub enum Error {
    KDB {
        code: error::KDBError,
        description: Option<String>,
    },
    Dlopen {
        path: String,
        error: dlopen2::Error,
    },
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::KDB { code, description } => match description {
                Some(description) => {
                    write!(f, "[{code}]: {description}")
                }
                None => {
                    write!(f, "[{code}]")
                }
            },
            Error::Dlopen { path, error } => write!(f, "load dll {path} error: {error}"),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(WrapperApi)]
pub struct KDBFunctions {
    KDBAPIStartup: unsafe extern "system" fn(Flags: bindings::KDB_UINT32) -> bindings::KDB_RET,
    KDBAPICleanup: unsafe extern "system" fn() -> bindings::KDB_RET,
    KDBAPIVersion: unsafe extern "system" fn(Version: bindings::KDB_WSTR, Length: bindings::ULONG),
    KDBAPITrace: unsafe extern "system" fn(
        EnableTrace: bindings::KDB_BOOLEAN,
        LogFileName: bindings::KDB_CWSTR,
    ),
    KDBUtilGetErrorDescription: unsafe extern "system" fn(
        ErrorCode: bindings::KDB_RET,
        ErrorDescription: bindings::KDB_WSTR,
        Length: bindings::KDB_UINT32,
    ),
    KDBServerConnect: unsafe extern "system" fn(
        ConnectionOption: bindings::PKDB_CONNECTION_OPTION,
        DBHandle: *mut bindings::KDB_HANDLE,
    ) -> bindings::KDB_RET,
    KDBServerIsConnected:
        unsafe extern "system" fn(DBHandle: bindings::KDB_HANDLE) -> bindings::KDB_BOOLEAN,
    KDBServerGetTime: unsafe extern "system" fn(
        DBHandle: bindings::KDB_HANDLE,
        CurrentTime: *mut bindings::KDB_TIMESTAMP,
    ) -> bindings::KDB_RET,
    KDBDataOpenRecordset: unsafe extern "system" fn(
        DBHandle: bindings::KDB_HANDLE,
        DataCriteria: bindings::PKDB_DATA_CRITERIA,
        DataRecordsets: bindings::PKDB_DATA_RECORDSETS,
    ) -> bindings::KDB_RET,
    KDBDataCloseRecordset:
        unsafe extern "system" fn(DataRecordsets: bindings::PKDB_DATA_RECORDSETS),
    KDBTagExists: unsafe extern "system" fn(
        DBHandle: bindings::KDB_HANDLE,
        TagName: bindings::KDB_CWSTR,
    ) -> bindings::KDB_RET,
    KDBTagGetAllNames: unsafe extern "system" fn(
        DBHandle: bindings::KDB_HANDLE,
        TagNames: bindings::PKDB_STRING_ARRAY,
    ) -> bindings::KDB_RET,
    KDBUtilFreeStringArray: unsafe extern "system" fn(StringArray: bindings::PKDB_STRING_ARRAY),
    KDBTagGetNames: unsafe extern "system" fn(
        DBHandle: bindings::KDB_HANDLE,
        TagNameMask: bindings::KDB_CWSTR,
        DescriptionMask: bindings::KDB_CWSTR,
        CollectorName: bindings::KDB_CWSTR,
        SourceAddress: bindings::KDB_CWSTR,
        TagNames: bindings::PKDB_STRING_ARRAY,
    ) -> bindings::KDB_RET,
    KDBTagOpenRecordset: unsafe extern "system" fn(
        DBHandle: bindings::KDB_HANDLE,
        TagCriteria: bindings::PKDB_TAG_CRITERIA,
        TagFields: bindings::PKDB_TAG_FIELDS,
        TagRecordset: bindings::PKDB_TAG_RECORDSET,
    ) -> bindings::KDB_RET,
    KDBTagCloseRecordset:
        unsafe extern "system" fn(TagRecordset: bindings::PKDB_TAG_RECORDSET) -> bindings::KDB_RET,
    KDBTagGetProperties: unsafe extern "system" fn(
        DBHandle: bindings::KDB_HANDLE,
        TagName: bindings::KDB_CWSTR,
        TagFields: bindings::PKDB_TAG_FIELDS,
        TagProperties: bindings::PKDB_TAG_PROPERTIES,
    ) -> bindings::KDB_RET,
    KDBTagFreeProperties: unsafe extern "system" fn(TagProperties: bindings::PKDB_TAG_PROPERTIES),
    KDBDataGetCurrentValue: unsafe extern "system" fn(
        DBHandle: bindings::KDB_HANDLE,
        NumberOfTags: bindings::KDB_UINT32,
        TagNames: bindings::KDB_WSTR_ARRAY,
        DigitalAsString: bindings::KDB_BOOLEAN,
        DataProperties: bindings::PKDB_DATA_PROPERTIES,
        ErrorStatuses: *mut bindings::KDB_RET,
    ) -> bindings::KDB_RET,
    KDBDataFreeCurrentValue: unsafe extern "system" fn(
        NumberOfTags: bindings::KDB_UINT32,
        DataProperties: bindings::PKDB_DATA_PROPERTIES,
    ),
    KDBDataRegisterCallback: unsafe extern "system" fn(
        DBHandle: bindings::KDB_HANDLE,
        CallbackFunction: bindings::KDB_DATA_CALLBACK_FUNCTION,
        UserParameter: bindings::KDB_PTR,
    ) -> bindings::KDB_RET,
    KDBDataSubscribeEx: unsafe extern "system" fn(
        DBHandle: bindings::KDB_HANDLE,
        NumberOfTags: bindings::KDB_UINT32,
        TagNames: bindings::KDB_WSTR_ARRAY,
        MinimumElapsedTime: bindings::KDB_UINT32,
        ErrorStatuses: *mut bindings::KDB_RET,
        Subscribe: bindings::KDB_BOOLEAN,
    ) -> bindings::KDB_RET,
    KDBTagRegisterPropertiesCallback: unsafe extern "system" fn(
        DBHandle: bindings::KDB_HANDLE,
        CallbackFunction: bindings::KDB_TAG_PROPERTIES_CALLBACK_FUNCTION,
        UserParameter: bindings::KDB_PTR,
    ) -> bindings::KDB_RET,
    KDBTagSubscribePropertiesEx: unsafe extern "system" fn(
        DBHandle: bindings::KDB_HANDLE,
        NumberOfTags: bindings::KDB_UINT32,
        TagNames: bindings::KDB_WSTR_ARRAY,
        ErrorStatuses: *mut bindings::KDB_RET,
        Subscribe: bindings::KDB_BOOLEAN,
    ) -> bindings::KDB_RET,
    KDBTagGroupGetChildren: unsafe extern "system" fn(
        DBHandle: bindings::KDB_HANDLE,
        GroupID: bindings::KDB_UINT32,
        ChildrenIDs: bindings::PKDB_INT_ARRAY,
    ) -> bindings::KDB_RET,
    KDBUtilFreeIntArray: unsafe extern "system" fn(IntArray: bindings::PKDB_INT_ARRAY),
    KDBTagGroupGetTags: unsafe extern "system" fn(
        DBHandle: bindings::KDB_HANDLE,
        GroupID: bindings::KDB_UINT32,
        TagNames: bindings::PKDB_STRING_ARRAY,
    ) -> bindings::KDB_RET,
    KDBServerDisconnect:
        unsafe extern "system" fn(DBHandle: bindings::KDB_HANDLE) -> bindings::KDB_RET,
    KDBTagGroupGetProperties: unsafe extern "system" fn(
        DBHandle: bindings::KDB_HANDLE,
        GroupID: bindings::KDB_UINT32,
        GroupProperties: bindings::PKDB_TAG_GROUP_PROPERTIES,
    ) -> bindings::KDB_RET,
    KDBTagGroupFreeProperties:
        unsafe extern "system" fn(GroupProperties: bindings::PKDB_TAG_GROUP_PROPERTIES),
}

static KDB_API: OnceLock<KDBContainer> = OnceLock::new();

struct KDBContainer {
    container: Container<KDBFunctions>,
}

impl KDBContainer {
    fn open<'a>() -> Result<&'a Container<KDBFunctions>> {
        match KDB_API.get() {
            Some(this) => Ok(&this.container),
            None => {
                let path = std::env::var("KINGHISTORIAN_SDK_DLL_PATH")
                    .unwrap_or_else(|_| "KRTDBAPIx64.dll".to_string());
                let container = match unsafe { Container::load(&path) } {
                    Ok(container) => Self { container },
                    Err(error) => {
                        tracing::warn!(
                            "load KRTDBAPIx64.dll path error, use default C:\\Program Files\\KingHistorian\\SDK\\C\\KRTDBAPIx64.dll: {error}, "
                        );
                        let path =
                            "C:\\Program Files\\KingHistorian\\SDK\\C\\KRTDBAPIx64.dll".to_string();
                        match unsafe { Container::load(&path) } {
                            Ok(container) => Self { container },
                            Err(error) => return Err(Error::Dlopen { path, error }),
                        }
                    }
                };
                Ok(&KDB_API.get_or_init(|| container).container)
            }
        }
    }
}

fn ret_to_error<T>(ret: bindings::KDB_RET) -> Result<T> {
    let mut buf = [0u16; 256];
    unsafe {
        KDBContainer::open()?.KDBUtilGetErrorDescription(ret, &mut buf as *mut _, 256);
    }
    Err(Error::KDB {
        code: (ret as bindings::KDBErrorCode).into(),
        description: string_from_wptr(&mut buf as *mut _),
    })
}

pub fn api_start_up() -> Result<()> {
    let ret = unsafe { KDBContainer::open()?.KDBAPIStartup(0) };
    if ret == 0 {
        return Ok(());
    }
    ret_to_error(ret)
}

pub fn api_cleanup() -> Result<()> {
    let ret = unsafe { KDBContainer::open()?.KDBAPICleanup() };
    if ret == 0 {
        return Ok(());
    }
    ret_to_error(ret)
}

pub fn api_trace(filename: &str, enable: bool) -> Result<()> {
    let filename = string_to_wptr_vec(filename);
    unsafe {
        KDBContainer::open()?.KDBAPITrace(enable as _, filename.as_ptr());
    }
    Ok(())
}

pub fn api_version() -> Result<Option<String>> {
    let mut res = [0; 30];
    unsafe {
        KDBContainer::open()?.KDBAPIVersion(res.as_mut_ptr(), 30);
    }
    Ok(string_from_wptr(res.as_mut_ptr()))
}

pub struct ConnectionOptionsBuilder<'a> {
    server_name: &'a str,
    server_port: &'a str,
    username: &'a str,
    password: &'a str,
    application_name: Option<&'a str>,
    client_name: Option<&'a str>,
    collector_name: Option<&'a str>,
    network_timeout_ms: Option<u32>,
    session_id: Option<&'a str>,
}

impl<'a> ConnectionOptionsBuilder<'a> {
    pub fn application_name(mut self, application_name: &'a str) -> Self {
        self.application_name = Some(application_name);
        self
    }

    pub fn client_name(mut self, client_name: &'a str) -> Self {
        self.client_name = Some(client_name);
        self
    }

    pub fn collector_name(mut self, collector_name: &'a str) -> Self {
        self.collector_name = Some(collector_name);
        self
    }

    pub fn network_timeout_ms(mut self, network_timeout_ms: u32) -> Self {
        self.network_timeout_ms = Some(network_timeout_ms);
        self
    }

    pub fn session_id(mut self, session_id: &'a str) -> Self {
        self.session_id = Some(session_id);
        self
    }

    pub fn build(self) -> ConnectionOptions {
        ConnectionOptions {
            server_name: string_to_wptr_vec(self.server_name),
            server_port: string_to_wptr_vec(self.server_port),
            username: string_to_wptr_vec(self.username),
            password: string_to_wptr_vec(self.password),
            application_name: self.application_name.map(string_to_wptr_vec),
            client_name: self.client_name.map(string_to_wptr_vec),
            collector_name: self.collector_name.map(string_to_wptr_vec),
            network_timeout_ms: self.network_timeout_ms.unwrap_or_default(),
            session_id: self.session_id.map(string_to_wptr_vec),
        }
    }
}

pub struct ConnectionOptions {
    server_name: Vec<bindings::KDB_WCHAR>,
    server_port: Vec<bindings::KDB_WCHAR>,
    username: Vec<bindings::KDB_WCHAR>,
    password: Vec<bindings::KDB_WCHAR>,
    application_name: Option<Vec<bindings::KDB_WCHAR>>,
    client_name: Option<Vec<bindings::KDB_WCHAR>>,
    collector_name: Option<Vec<bindings::KDB_WCHAR>>,
    network_timeout_ms: u32,
    session_id: Option<Vec<bindings::KDB_WCHAR>>,
}

impl ConnectionOptions {
    pub fn builder<'a>(
        server_name: &'a str,
        server_port: &'a str,
        username: &'a str,
        password: &'a str,
    ) -> ConnectionOptionsBuilder<'a> {
        ConnectionOptionsBuilder {
            server_name,
            server_port,
            username,
            password,
            application_name: None,
            client_name: None,
            collector_name: None,
            network_timeout_ms: None,
            session_id: None,
        }
    }
}

fn string_to_wptr_vec(s: &str) -> Vec<bindings::KDB_WCHAR> {
    use std::ffi::OsStr;
    use std::os::windows::ffi::OsStrExt;
    OsStr::new(s)
        .encode_wide()
        .chain(std::iter::once(0))
        .collect()
}

fn string_from_wptr(wptr: *mut u16) -> Option<String> {
    let utf16_slice = unsafe { u16_ptr_to_vec(wptr)? };
    string_from_wptr_vec(&utf16_slice)
}

fn string_from_wptr_vec(utf16_slice: &[u16]) -> Option<String> {
    use std::{ffi::OsString, os::windows::ffi::OsStringExt};
    let s = OsString::from_wide(utf16_slice);
    Some(s.to_string_lossy().into_owned())
}

unsafe fn u16_ptr_to_vec(ptr: *mut u16) -> Option<Vec<bindings::WCHAR>> {
    if ptr.is_null() {
        return None;
    }

    let mut len = 0;
    unsafe {
        while *ptr.add(len) != 0 {
            len += 1;
        }
    }

    if len == 0 {
        return Some(vec![]);
    }

    let slice = unsafe { std::slice::from_raw_parts(ptr as _, len) };
    Some(slice.to_vec())
}

pub struct ServerConnection {
    handle: bindings::KDB_HANDLE,
    data_sender: Option<*mut flume::Sender<Result<DataRecord>>>,
    tag_sender: Option<*mut flume::Sender<(ItemChangeType, TagProperties)>>,
}

impl ServerConnection {
    pub fn new(mut options: ConnectionOptions) -> Result<Self> {
        let mut opts = bindings::KDBConnectionOption {
            ServerName: options.server_name.as_mut_ptr(),
            ServerPort: options.server_port.as_mut_ptr(),
            UserName: options.username.as_mut_ptr(),
            Password: options.password.as_mut_ptr(),
            ApplicationName: options
                .application_name
                .as_mut()
                .map(|v| v.as_mut_ptr())
                .unwrap_or_else(std::ptr::null_mut),
            ClientName: options
                .client_name
                .as_mut()
                .map(|v| v.as_mut_ptr())
                .unwrap_or_else(std::ptr::null_mut),
            CollectorName: options
                .collector_name
                .as_mut()
                .map(|v| v.as_mut_ptr())
                .unwrap_or_else(std::ptr::null_mut),
            NetworkTimeout: options.network_timeout_ms as _,
            ConnectionFlags: bindings::KDBConnectionFlags_KCOF_PROTOCOL_TCPIP as _,
            Reserved1: 0,
            Reserved2: 0,
            SessionId: options
                .session_id
                .as_mut()
                .map(|v| v.as_mut_ptr())
                .unwrap_or_else(std::ptr::null_mut),
            Reserved4: std::ptr::null_mut(),
        };
        let mut handle = std::ptr::null_mut();
        let ret = unsafe { KDBContainer::open()?.KDBServerConnect(&mut opts, &mut handle) };
        if ret != 0 {
            return ret_to_error(ret);
        }
        Ok(Self {
            handle,
            data_sender: None,
            tag_sender: None,
        })
    }

    pub fn is_connected(&mut self) -> Result<bool> {
        unsafe { Ok(KDBContainer::open()?.KDBServerIsConnected(self.handle) > 0) }
    }

    pub fn get_server_time(&mut self) -> Result<Option<DateTime<Utc>>> {
        let mut ts = bindings::KDBTimeStamp {
            Seconds: 0,
            Millisecs: 0,
        };
        let ret = unsafe { KDBContainer::open()?.KDBServerGetTime(self.handle, &mut ts as *mut _) };
        if ret == 0 {
            return Ok(DateTime::from_timestamp(
                ts.Seconds as _,
                (ts.Millisecs as u32) * 1000 * 1000,
            ));
        }
        ret_to_error(ret)
    }

    pub fn query_tag_values(
        &mut self,
        mut criteria: DataCriteria,
    ) -> Result<HashMap<String, Vec<Data>>> {
        let mut tag_names: Vec<_> = criteria
            .tag_names
            .iter_mut()
            .map(|p| p.as_mut_ptr())
            .collect();
        let filter_comparison_value = criteria
            .filter_comparison_value
            .as_mut()
            .map(|v| v.as_kdb_value());
        let mut criteria = bindings::KDBDataCriteria {
            NumberOfTags: criteria.tag_names.len() as _,
            TagNames: tag_names.as_mut_ptr(),
            StartTime: {
                let ts = criteria.start_time;
                bindings::KDBTimeStamp {
                    Seconds: ts.map(|ts| ts.timestamp() as _).unwrap_or_default(),
                    Millisecs: ts
                        .map(|ts| ts.timestamp_subsec_millis() as _)
                        .unwrap_or_default(),
                }
            },
            EndTime: {
                let ts = criteria.end_time;
                bindings::KDBTimeStamp {
                    Seconds: ts.map(|ts| ts.timestamp() as _).unwrap_or_default(),
                    Millisecs: ts
                        .map(|ts| ts.timestamp_subsec_millis() as _)
                        .unwrap_or_default(),
                }
            },
            DataVersion: criteria.data_version.unwrap_or_default() as _,
            SamplingMode: criteria.sampling_mode.unwrap_or_default() as _,
            SamplingNumber: criteria.sampling_number.unwrap_or_default() as _,
            SamplingInterval: criteria.sampling_interval_ms.unwrap_or_default(),
            CalculationMode: criteria.calculation_mode.unwrap_or_default() as _,
            FilterTag: criteria
                .filter_tag
                .as_mut()
                .map(|v| v.as_mut_ptr())
                .unwrap_or_else(std::ptr::null_mut),
            FilterMode: criteria.filter_mode.unwrap_or_default() as _,
            FilterComparisonMode: criteria.filter_comparison_mode.unwrap_or_default() as _,
            FilterComparisonValue: filter_comparison_value
                .unwrap_or_else(|| Value::Empty.as_kdb_value())
                .0,
            RowCount: criteria.row_count.unwrap_or_default() as _,
            DigitalAsString: criteria.digital_as_string.unwrap_or_default() as _,
        };
        let len = tag_names.len();
        let mut data_record_sets = bindings::KDBDataRecordsets {
            NumberOfTags: len as _,
            DataRecordset: std::ptr::null_mut(),
        };
        let ret = unsafe {
            KDBContainer::open()?.KDBDataOpenRecordset(
                self.handle,
                &mut criteria,
                &mut data_record_sets,
            )
        };
        if ret != 0 {
            return ret_to_error(ret);
        }

        let len = data_record_sets.NumberOfTags as usize;
        if len == 0 {
            return Ok(HashMap::new());
        }

        let sets = unsafe { std::slice::from_raw_parts(data_record_sets.DataRecordset, len) };
        let mut res = HashMap::with_capacity(sets.len());

        // 每个 tag 一个 set
        for set in sets {
            let Some(tag_name) = string_from_wptr(set.TagName) else {
                continue;
            };
            let len = set.NumberOfRecords as usize;
            if len == 0 {
                continue;
            }
            // 每个 set 里有一堆数据
            let records = unsafe { std::slice::from_raw_parts(set.DataRecords, len) };
            let mut tag_data = Vec::with_capacity(len);
            for record in records {
                let Some(value) = Value::from_kdb_value(&record.Value) else {
                    continue;
                };
                let data = Data {
                    timestamp: DateTime::from_timestamp(
                        record.TimeStamp.Seconds as _,
                        (record.TimeStamp.Millisecs as u32) * 1000 * 1000,
                    ),
                    version: DataVersion::from_i16(record.Version),
                    quality: record.Quality as _,
                    value,
                };
                tag_data.push(data);
            }
            res.insert(tag_name, tag_data);
        }

        unsafe {
            KDBContainer::open()?.KDBDataCloseRecordset(&mut data_record_sets);
        }
        Ok(res)
    }

    pub fn tag_exists(&mut self, tag_name: &str) -> Result<bool> {
        let tag_name_v = string_to_wptr_vec(tag_name);
        let ret = unsafe { KDBContainer::open()?.KDBTagExists(self.handle, tag_name_v.as_ptr()) };
        if ret == 0 {
            return Ok(true);
        }
        if ret == (error::KDBError::NotFound as _) {
            return Ok(false);
        }

        return ret_to_error(ret);
    }

    pub fn all_tags(&mut self) -> Result<Vec<String>> {
        let mut buf = bindings::KDBStringArray {
            SizeOfArray: 0,
            StringArray: std::ptr::null_mut(),
        };
        let ret = unsafe { KDBContainer::open()?.KDBTagGetAllNames(self.handle, &mut buf) };
        if ret != 0 {
            return ret_to_error(ret);
        }

        let size = buf.SizeOfArray;

        let mut res = Vec::with_capacity(size as _);
        if buf.StringArray.is_null() || size == 0 {
            return Ok(res);
        }

        for i in 0..size {
            let p = unsafe { *buf.StringArray.add(i as _) };
            if p.is_null() {
                continue;
            }

            let Some(s) = string_from_wptr(p) else {
                continue;
            };
            res.push(s);
        }

        unsafe {
            KDBContainer::open()?.KDBUtilFreeStringArray(&mut buf as _);
        }

        Ok(res)
    }

    pub fn get_tag_names_by_filter(
        &mut self,
        tag_name_mask: Option<String>,
        description_mask: Option<String>,
        collector_name: Option<String>,
        source_address: Option<String>,
    ) -> Result<Vec<String>> {
        let mut buf = bindings::KDBStringArray {
            SizeOfArray: 0,
            StringArray: std::ptr::null_mut(),
        };
        let ret = unsafe {
            KDBContainer::open()?.KDBTagGetNames(
                self.handle,
                tag_name_mask
                    .map(|s| string_to_wptr_vec(&s))
                    .map(|v| v.as_ptr())
                    .unwrap_or_else(std::ptr::null),
                description_mask
                    .map(|s| string_to_wptr_vec(&s))
                    .map(|v| v.as_ptr())
                    .unwrap_or_else(std::ptr::null),
                collector_name
                    .map(|s| string_to_wptr_vec(&s))
                    .map(|v| v.as_ptr())
                    .unwrap_or_else(std::ptr::null),
                source_address
                    .map(|s| string_to_wptr_vec(&s))
                    .map(|v| v.as_ptr())
                    .unwrap_or_else(std::ptr::null),
                &mut buf,
            )
        };
        if ret != 0 {
            return ret_to_error(ret);
        }

        let size = buf.SizeOfArray;

        let mut res = Vec::with_capacity(size as _);
        if buf.StringArray.is_null() || size == 0 {
            return Ok(res);
        }

        for i in 0..size {
            let p = unsafe { *buf.StringArray.add(i as _) };
            if p.is_null() {
                continue;
            }

            let Some(s) = string_from_wptr(p) else {
                continue;
            };
            res.push(s);
        }

        unsafe {
            KDBContainer::open()?.KDBUtilFreeStringArray(&mut buf as _);
        }

        Ok(res)
    }

    pub fn query_tag_properties(
        &mut self,
        mut criteria: TagCriteria,
        fields: TagFields,
    ) -> Result<Vec<TagProperties>> {
        let mut tag_names = criteria
            .tag_names
            .as_mut()
            .map(|names| names.iter_mut().map(|p| p.as_mut_ptr()).collect::<Vec<_>>());
        let mut criteria = bindings::KDBTagCriteria {
            TagNameMask: criteria
                .tag_name_mask
                .as_mut()
                .map(|s| s.as_mut_ptr())
                .unwrap_or_else(std::ptr::null_mut),
            NumberOfTags: criteria.tag_names.map(|s| s.len()).unwrap_or_default() as _,
            TagNames: tag_names
                .as_mut()
                .map(|p| p.as_mut_ptr())
                .unwrap_or_else(std::ptr::null_mut),
            DescriptionMask: criteria
                .description_mask
                .as_mut()
                .map(|s| s.as_mut_ptr())
                .unwrap_or_else(std::ptr::null_mut),
            CollectorName: criteria
                .collector_name
                .as_mut()
                .map(|s| s.as_mut_ptr())
                .unwrap_or_else(std::ptr::null_mut),
            SourceAddress: criteria
                .source_address
                .as_mut()
                .map(|s| s.as_mut_ptr())
                .unwrap_or_else(std::ptr::null_mut),
        };
        let mut fields = fields.to_kdb_tag_fields();
        let mut recordset = bindings::KDBTagRecordset {
            NumberOfRecords: 0,
            TagRecords: std::ptr::null_mut(),
        };
        let ret = unsafe {
            KDBContainer::open()?.KDBTagOpenRecordset(
                self.handle,
                &mut criteria,
                &mut fields,
                &mut recordset,
            )
        };
        if ret != 0 {
            return ret_to_error(ret);
        }

        let size = recordset.NumberOfRecords as usize;
        if size == 0 {
            return Ok(vec![]);
        }

        let records = unsafe { std::slice::from_raw_parts(recordset.TagRecords, size) };

        let mut res = Vec::with_capacity(size);
        for prop in records {
            res.push(TagProperties::from_kdb(prop));
        }

        unsafe {
            KDBContainer::open()?.KDBTagCloseRecordset(&mut recordset);
        }

        return Ok(res);
    }

    pub fn get_tag_properties(
        &mut self,
        tag_name: &str,
        fields: TagFields,
    ) -> Result<TagProperties> {
        let tag_name = string_to_wptr_vec(tag_name);
        let mut prop = bindings::KDBTagProperties {
            TagName: std::ptr::null_mut(),
            EngineeringUnit: std::ptr::null_mut(),
            Description: std::ptr::null_mut(),
            TagId: 0,
            DigitalSetId: 0,
            CollectorName: std::ptr::null_mut(),
            CollectorType: 0,
            SourceAddress: std::ptr::null_mut(),
            DataType: 0,
            DataLength: 0,
            CollectionControl: 0,
            CollectionMode: 0,
            CollectionInterval: 0,
            CollectionOffset: 0,
            TimestampType: 0,
            TimeZoneBias: 0,
            TimeAdjustment: 0,
            MaxValue: 0.0,
            MinValue: 0.0,
            InputConversion: 0,
            MinRaw: 0.0,
            MaxRaw: 0.0,
            CollectorCompression: 0,
            CollectorCompressionMode: 0,
            CollectorAbsoluteDeadbanding: 0,
            CollectorDeadbandPercent: 0.0,
            CollectorAbsoluteDeadband: 0.0,
            CollectorCompressionTimeout: 0,
            CollectorCompressionTimeoutMin: 0,
            ArchiveControl: 0,
            ArchiveVersionSupport: 0,
            ArchiveShutdown: 0,
            ArchiveStepValue: 0,
            ArchiveStoreMode: 0,
            ArchiveCompression: 0,
            ArchiveAbsoluteDeadbanding: 0,
            ArchiveCompressionMode: 0,
            ArchiveDeadbandPercent: 0.0,
            ArchiveAbsoluteDeadband: 0.0,
            ArchiveCompressionTimeout: 0,
            ArchiveCompressionTimeoutMin: 0,
            SecurityReadRole: std::ptr::null_mut(),
            SecurityWriteRole: std::ptr::null_mut(),
            SecurityAdminRole: std::ptr::null_mut(),
            CreateTime: bindings::KDBTimeStamp {
                Seconds: 0,
                Millisecs: 0,
            },
            LastModified: bindings::KDBTimeStamp {
                Seconds: 0,
                Millisecs: 0,
            },
            CreateUser: std::ptr::null_mut(),
            LastModifiedUser: std::ptr::null_mut(),
            ElectronicRecord: 0,
            Calculation: std::ptr::null_mut(),
            NumberOfCalculationTriggers: 0,
            CalculationTriggers: std::ptr::null_mut(),
            TagGeneral1: 0,
            TagGeneral2: 0,
            TagGeneral3: 0,
            TagGeneral4: 0,
            TagGeneral5: std::ptr::null_mut(),
            TagGeneral6: std::ptr::null_mut(),
            TagGeneral7: std::ptr::null_mut(),
            TagGeneral8: std::ptr::null_mut(),
            TagGeneral9: 0.0,
            TagGeneral10: 0.0,
            TagGeneral11: 0.0,
            TagGeneral12: 0.0,
            TagGeneral13: 0,
            TagGeneral14: 0,
            TagGeneral15: 0,
            TagGeneral16: 0,
            TagGeneral17: std::ptr::null_mut(),
            TagGeneral18: std::ptr::null_mut(),
            TagGeneral19: std::ptr::null_mut(),
            TagGeneral20: std::ptr::null_mut(),
            SystemGeneral1: 0,
            SystemGeneral2: 0,
            SystemGeneral3: 0,
            SystemGeneral4: 0,
            SystemGeneral5: std::ptr::null_mut(),
            SystemGeneral6: std::ptr::null_mut(),
            SystemGeneral7: std::ptr::null_mut(),
            SystemGeneral8: std::ptr::null_mut(),
            SystemGeneral9: 0.0,
            SystemGeneral10: 0.0,
            SystemGeneral11: 0.0,
            SystemGeneral12: 0.0,
            SystemGeneral13: 0,
            SystemGeneral14: 0,
            SystemGeneral15: 0,
            SystemGeneral16: 0,
            SystemGeneral17: std::ptr::null_mut(),
            SystemGeneral18: std::ptr::null_mut(),
            SystemGeneral19: std::ptr::null_mut(),
            SystemGeneral20: std::ptr::null_mut(),
            UserGeneral1: 0,
            UserGeneral2: 0,
            UserGeneral3: 0,
            UserGeneral4: 0,
            UserGeneral5: std::ptr::null_mut(),
            UserGeneral6: std::ptr::null_mut(),
            UserGeneral7: std::ptr::null_mut(),
            UserGeneral8: std::ptr::null_mut(),
            UserGeneral9: 0.0,
            UserGeneral10: 0.0,
        };
        let mut fields = fields.to_kdb_tag_fields();
        let ret = unsafe {
            KDBContainer::open()?.KDBTagGetProperties(
                self.handle,
                tag_name.as_ptr(),
                &mut fields as _,
                &mut prop,
            )
        };
        if ret != 0 {
            return ret_to_error(ret);
        }
        let res = TagProperties::from_kdb(&prop);
        unsafe {
            KDBContainer::open()?.KDBTagFreeProperties(&mut prop);
        }
        Ok(res)
    }

    pub fn get_data_current_value<I, T>(
        &mut self,
        tag_names: I,
        digital_as_string: bool,
    ) -> Result<Vec<Result<Data>>>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        let mut tag_names_vec = tag_names
            .into_iter()
            .map(|s| string_to_wptr_vec(s.as_ref()))
            .collect::<Vec<_>>();
        let mut tags: Vec<_> = tag_names_vec.iter_mut().map(|v| v.as_mut_ptr()).collect();
        let len = tags.len();
        let mut properties = Vec::with_capacity(len);
        for _ in 0..len {
            properties.push(bindings::KDBDataProperties {
                TimeStamp: bindings::KDBTimeStamp {
                    Seconds: 0,
                    Millisecs: 0,
                },
                Version: 0,
                Quality: 0,
                Value: Value::Empty.as_kdb_value().0,
            });
        }
        let mut error_status_arr = vec![0; len];
        let ret = unsafe {
            KDBContainer::open()?.KDBDataGetCurrentValue(
                self.handle,
                len as _,
                tags.as_mut_ptr(),
                digital_as_string as _,
                properties.as_mut_ptr(),
                error_status_arr.as_mut_ptr(),
            )
        };
        if ret != 0 {
            return ret_to_error(ret);
        }

        if properties.is_empty() {
            return Ok(vec![]);
        }

        let mut res = Vec::with_capacity(len);
        for (idx, prop) in properties.iter().enumerate() {
            let err_code = error_status_arr[idx];
            if err_code != 0 {
                res.push(ret_to_error(err_code));
                continue;
            }
            let Some(value) = Value::from_kdb_value(&prop.Value) else {
                continue;
            };
            let data = Data {
                timestamp: DateTime::from_timestamp(
                    prop.TimeStamp.Seconds as _,
                    (prop.TimeStamp.Millisecs as u32) * 1000 * 1000,
                ),
                version: DataVersion::from_i16(prop.Version),
                quality: prop.Quality as _,
                value,
            };
            res.push(Ok(data));
        }

        unsafe {
            KDBContainer::open()?.KDBDataFreeCurrentValue(len as _, properties.as_mut_ptr());
        }

        Ok(res)
    }

    pub fn data_subscribe<I, T>(
        &mut self,
        tag_names: I,
        min_elapsed_ms: u32,
        sender: Option<flume::Sender<Result<DataRecord>>>,
    ) -> Result<Vec<Result<()>>>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        let enable = sender.is_some();
        match sender {
            Some(sender) => {
                let sender = Box::into_raw(Box::new(sender));
                let ret = unsafe {
                    KDBContainer::open()?.KDBDataRegisterCallback(
                        self.handle,
                        Some(data_change_callback),
                        sender as *mut _,
                    )
                };
                if ret != 0 {
                    let _ = unsafe { Box::from_raw(sender) };
                    return ret_to_error(ret);
                }
                self.data_sender = Some(sender);
            }
            None => {
                if let Some(sender) = self.data_sender.take()
                    && !sender.is_null()
                {
                    let _ = unsafe { Box::from_raw(sender) };
                }
                let ret = unsafe {
                    KDBContainer::open()?.KDBDataRegisterCallback(
                        self.handle,
                        None,
                        std::ptr::null_mut(),
                    )
                };
                if ret != 0 {
                    return ret_to_error(ret);
                }
            }
        }

        let mut tag_names = tag_names
            .into_iter()
            .map(|s| string_to_wptr_vec(s.as_ref()))
            .map(|mut s| s.as_mut_ptr())
            .collect::<Vec<_>>();
        let len = tag_names.len();
        let mut error_status_arr = vec![0; len];
        let ret = unsafe {
            KDBContainer::open()?.KDBDataSubscribeEx(
                self.handle,
                len as _,
                tag_names.as_mut_ptr(),
                min_elapsed_ms as _,
                error_status_arr.as_mut_ptr(),
                enable as _,
            )
        };
        if ret != 0 {
            unsafe {
                KDBContainer::open()?.KDBDataRegisterCallback(
                    self.handle,
                    None,
                    std::ptr::null_mut(),
                );
            }
            return ret_to_error(ret);
        }

        Ok(error_status_arr
            .iter()
            .map(|v| if *v == 0 { Ok(()) } else { ret_to_error(*v) })
            .collect())
    }

    pub fn tag_subscribe<I, T>(
        &mut self,
        tag_names: I,
        sender: Option<flume::Sender<(ItemChangeType, TagProperties)>>,
    ) -> Result<Vec<Result<()>>>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        let enable = sender.is_some();
        match sender {
            Some(sender) => {
                let sender = Box::into_raw(Box::new(sender));
                let ret = unsafe {
                    KDBContainer::open()?.KDBTagRegisterPropertiesCallback(
                        self.handle,
                        Some(tag_change_callback),
                        sender as *mut _,
                    )
                };
                if ret != 0 {
                    let _ = unsafe { Box::from_raw(sender) };
                    return ret_to_error(ret);
                }
                self.tag_sender = Some(sender);
            }
            None => {
                if let Some(sender) = self.tag_sender.take()
                    && !sender.is_null()
                {
                    let _ = unsafe { Box::from_raw(sender) };
                }
                let ret = unsafe {
                    KDBContainer::open()?.KDBTagRegisterPropertiesCallback(
                        self.handle,
                        None,
                        std::ptr::null_mut(),
                    )
                };
                if ret != 0 {
                    return ret_to_error(ret);
                }
            }
        }

        let mut tag_names = tag_names
            .into_iter()
            .map(|s| string_to_wptr_vec(s.as_ref()))
            .map(|mut s| s.as_mut_ptr())
            .collect::<Vec<_>>();

        let len = tag_names.len();
        let mut error_status_arr = vec![0; len];

        let ret = unsafe {
            KDBContainer::open()?.KDBTagSubscribePropertiesEx(
                self.handle,
                len as _,
                tag_names.as_mut_ptr(),
                error_status_arr.as_mut_ptr(),
                enable as _,
            )
        };
        if ret != 0 {
            unsafe {
                KDBContainer::open()?.KDBTagRegisterPropertiesCallback(
                    self.handle,
                    None,
                    std::ptr::null_mut(),
                );
            }
            return ret_to_error(ret);
        }

        Ok(error_status_arr
            .iter()
            .map(|v| if *v == 0 { Ok(()) } else { ret_to_error(*v) })
            .collect())
    }

    pub fn tag_group_get_children(&mut self, group_id: u32) -> Result<Vec<u32>> {
        let mut array = bindings::KDBIntArray {
            SizeOfArray: 0,
            IntArray: std::ptr::null_mut(),
        };
        let ret = unsafe {
            KDBContainer::open()?.KDBTagGroupGetChildren(self.handle, group_id as _, &mut array)
        };
        if ret != 0 {
            return ret_to_error(ret);
        }
        let len = array.SizeOfArray as usize;
        if len == 0 {
            return Ok(vec![]);
        }

        let mut res = Vec::with_capacity(len);
        let children = unsafe { std::slice::from_raw_parts(array.IntArray, len) };
        res.extend(children.iter().map(|v| *v as u32));

        unsafe {
            KDBContainer::open()?.KDBUtilFreeIntArray(&mut array);
        }
        Ok(res)
    }

    pub fn tag_group_get_tags(&mut self, group_id: u32) -> Result<Vec<String>> {
        let mut array = bindings::KDBStringArray {
            SizeOfArray: 0,
            StringArray: std::ptr::null_mut(),
        };
        let ret = unsafe {
            KDBContainer::open()?.KDBTagGroupGetTags(self.handle, group_id as _, &mut array)
        };
        if ret != 0 {
            return ret_to_error(ret);
        }
        let len = array.SizeOfArray as usize;
        if len == 0 {
            return Ok(vec![]);
        }
        let tag_names = unsafe { std::slice::from_raw_parts(array.StringArray, len) };
        let mut res = Vec::with_capacity(len);
        res.extend(tag_names.iter().filter_map(|s| string_from_wptr(*s)));

        unsafe {
            KDBContainer::open()?.KDBUtilFreeStringArray(&mut array);
        }

        Ok(res)
    }

    pub fn get_tag_group_properties(&mut self, group_id: u32) -> Result<TagGroupProperties> {
        let mut prop = bindings::KDBTagGroupProperties {
            GroupID: group_id as _,
            ParentID: 0,
            GroupName: std::ptr::null_mut(),
            Description: std::ptr::null_mut(),
        };
        let ret = unsafe {
            KDBContainer::open()?.KDBTagGroupGetProperties(self.handle, group_id as _, &mut prop)
        };
        if ret != 0 {
            return ret_to_error(ret);
        }
        let res = TagGroupProperties::from_kdb(&prop);
        unsafe {
            KDBContainer::open()?.KDBTagGroupFreeProperties(&mut prop);
        }
        Ok(res)
    }
}

unsafe extern "C" fn data_change_callback(
    _handle: bindings::KDB_HANDLE,
    sender: *mut std::os::raw::c_void,
    records: *mut bindings::KDBDataRecordset,
) -> bindings::KDB_RET {
    let sender_ptr = sender as *mut flume::Sender<Result<DataRecord>>;
    if sender.is_null() {
        return -1;
    }
    let sender = unsafe { &*sender_ptr };
    // keep the original pointer so we can pass it back to the KDB close API
    let records_ptr = records;
    let records = unsafe { records_ptr.read() };

    let ret = records.ErrorStatus;
    if ret != 0 {
        let _ = sender.send(ret_to_error(ret));
    }
    let len = records.NumberOfRecords as usize;
    if len == 0 {
        return 0;
    }

    let mut data = Vec::with_capacity(len);
    let data_records = unsafe { std::slice::from_raw_parts(records.DataRecords, len) };
    for data_record in data_records {
        let Some(value) = Value::from_kdb_value(&data_record.Value) else {
            continue;
        };
        data.push(Data {
            timestamp: DateTime::from_timestamp(
                data_record.TimeStamp.Seconds as _,
                (data_record.TimeStamp.Millisecs as u32) * 1000 * 1000,
            ),
            version: DataVersion::from_i16(data_record.Version),
            quality: data_record.Quality as _,
            value,
        });
    }
    let record = DataRecord {
        tag_name: unsafe { u16_ptr_to_vec(records.TagName) },
        digital_set_id: records.DigitalSetId as _,
        data_type: ValueType::from(records.DataType as u16),
        data,
    };
    let _ = sender.send(Ok(record));

    // NOTE: do not call KDBDataCloseRecordset here. The SDK may already manage
    // the lifetime of the passed-in recordset for callbacks; calling the
    // close function here caused heap corruption in tests. Let the SDK free
    // its memory or require the caller to explicitly close if they opened it.

    // unsafe {
    //     if let Ok(container) = KDBContainer::open() {
    //         container.KDBDataCloseRecordset(
    //             &mut (bindings::KDBDataRecordsets {
    //                 NumberOfTags: 1,
    //                 DataRecordset: &mut records,
    //             }),
    //         );
    //     }
    // }
    0
}

unsafe extern "C" fn tag_change_callback(
    _handle: bindings::KDB_HANDLE,
    sender: *mut std::os::raw::c_void,
    properties: *mut bindings::KDB_TAG_PROPERTIES,
    change_type: bindings::KDB_ITEM_CHANGE_TYPE,
) -> bindings::KDB_RET {
    let Some(change_type) = ItemChangeType::from_i32(change_type as _) else {
        return 0;
    };
    let properties = unsafe { properties.read() };
    let properties = TagProperties::from_kdb(&properties);

    let sender_ptr = sender as *mut flume::Sender<(ItemChangeType, TagProperties)>;
    if sender.is_null() {
        return -1;
    }
    let sender = unsafe { &*sender_ptr };
    let _ = sender.send((change_type, properties));

    0
}

impl Drop for ServerConnection {
    fn drop(&mut self) {
        if let Some(sender) = self.data_sender.take() {
            unsafe {
                if let Ok(container) = KDBContainer::open() {
                    container.KDBDataRegisterCallback(self.handle, None, std::ptr::null_mut());
                }
            }
            if !sender.is_null() {
                let _ = unsafe { Box::from_raw(sender) };
            }
        }

        if let Some(sender) = self.tag_sender.take() {
            unsafe {
                if let Ok(container) = KDBContainer::open() {
                    container.KDBTagRegisterPropertiesCallback(
                        self.handle,
                        None,
                        std::ptr::null_mut(),
                    );
                }
            }
            if !sender.is_null() {
                let _ = unsafe { Box::from_raw(sender) };
            }
        }

        if !self.handle.is_null() {
            unsafe {
                if let Ok(container) = KDBContainer::open() {
                    container.KDBServerDisconnect(self.handle);
                }
            }
            self.handle = std::ptr::null_mut();
        }
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

impl DataVersion {
    fn from_i16(value: i16) -> Option<Self> {
        match value {
            0 => Some(Self::Original),
            -3 => Some(Self::Modified),
            -2 => Some(Self::Latest),
            -1 => Some(Self::All),
            _ => None,
        }
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
    WStr(Vec<bindings::KDB_WCHAR>),
    Blob(Vec<u8>),
    FileTime(FileTime),
    Timestamp(DateTime<Utc>),
    Var(Box<Value>),
    Dec(Dec),
}

impl Value {
    fn as_kdb_value(&mut self) -> SafeVal {
        let val = bindings::KDBValue {
            DataType: self.value_type() as _,
            __bindgen_anon_1: match self {
                Value::Empty => bindings::KDBValue__bindgen_ty_1 { bitVal: 0 },
                Value::Bool(v) => bindings::KDBValue__bindgen_ty_1 { bitVal: *v as _ },
                Value::I8(v) => bindings::KDBValue__bindgen_ty_1 { i1Val: *v as _ },
                Value::I16(v) => bindings::KDBValue__bindgen_ty_1 { i2Val: *v as _ },
                Value::I32(v) => bindings::KDBValue__bindgen_ty_1 { i4Val: *v as _ },
                Value::I64(v) => bindings::KDBValue__bindgen_ty_1 { i8Val: *v as _ },
                Value::U8(v) => bindings::KDBValue__bindgen_ty_1 { ui1Val: *v as _ },
                Value::U16(v) => bindings::KDBValue__bindgen_ty_1 { ui2Val: *v as _ },
                Value::U32(v) => bindings::KDBValue__bindgen_ty_1 { ui4Val: *v as _ },
                Value::U64(v) => bindings::KDBValue__bindgen_ty_1 { ui8Val: *v as _ },
                Value::F32(v) => bindings::KDBValue__bindgen_ty_1 { r4Val: *v as _ },
                Value::F64(v) => bindings::KDBValue__bindgen_ty_1 { r8Val: *v as _ },
                Value::Str(v) => bindings::KDBValue__bindgen_ty_1 {
                    strVal: std::ffi::CString::new(v.clone())
                        .expect("unexpected trailing zero")
                        .into_raw(),
                },
                Value::WStr(v) => bindings::KDBValue__bindgen_ty_1 {
                    wstrVal: v.as_mut_ptr(),
                },
                Value::Blob(v) => bindings::KDBValue__bindgen_ty_1 {
                    blobVal: bindings::KDBBlob {
                        Len: v.len() as _,
                        Data: v.as_mut_ptr(),
                    },
                },
                Value::FileTime(v) => bindings::KDBValue__bindgen_ty_1 {
                    ftVal: bindings::_FILETIME {
                        dwLowDateTime: v.dw_low_date_time as _,
                        dwHighDateTime: v.dw_high_date_time as _,
                    },
                },
                Value::Timestamp(v) => bindings::KDBValue__bindgen_ty_1 {
                    tsVal: bindings::KDBTimeStamp {
                        Seconds: v.timestamp() as _,
                        Millisecs: v.timestamp_subsec_millis() as _,
                    },
                },
                Value::Var(v) => bindings::KDBValue__bindgen_ty_1 {
                    varVal: Box::into_raw(Box::new(v.as_kdb_value().0)),
                },
                Value::Dec(v) => bindings::KDBValue__bindgen_ty_1 {
                    decVal: &mut (bindings::tagDEC {
                        wReserved: 0,
                        __bindgen_anon_1: bindings::tagDEC__bindgen_ty_1 {
                            __bindgen_anon_1: bindings::tagDEC__bindgen_ty_1__bindgen_ty_1 {
                                scale: v.scale,
                                sign: v.sign,
                            },
                        },
                        Hi32: v.high_32 as _,
                        __bindgen_anon_2: bindings::tagDEC__bindgen_ty_2 {
                            __bindgen_anon_1: bindings::tagDEC__bindgen_ty_2__bindgen_ty_1 {
                                Lo32: v.low as _,
                                Mid32: v.mid_32 as _,
                            },
                        },
                    }),
                },
            },
        };
        SafeVal(val)
    }

    fn from_kdb_value(value: &bindings::KDBValue) -> Option<Self> {
        unsafe {
            match ValueType::from(value.DataType) {
                ValueType::Empty => None,
                ValueType::Bool => Some(Value::Bool(value.__bindgen_anon_1.bitVal != 0)),
                ValueType::I8 => Some(Value::I8(value.__bindgen_anon_1.i1Val)),
                ValueType::I16 => Some(Value::I16(value.__bindgen_anon_1.i2Val)),
                ValueType::I32 => Some(Value::I32(value.__bindgen_anon_1.i4Val as _)),
                ValueType::I64 => Some(Value::I64(value.__bindgen_anon_1.i8Val)),
                ValueType::U8 => Some(Value::U8(value.__bindgen_anon_1.ui1Val)),
                ValueType::U16 => Some(Value::U16(value.__bindgen_anon_1.ui2Val)),
                ValueType::U32 => Some(Value::U32(value.__bindgen_anon_1.ui4Val as _)),
                ValueType::U64 => Some(Value::U64(value.__bindgen_anon_1.ui8Val)),
                ValueType::F32 => Some(Value::F32(value.__bindgen_anon_1.r4Val)),
                ValueType::F64 => Some(Value::F64(value.__bindgen_anon_1.r8Val)),
                ValueType::Str => Some(Value::Str(
                    std::ffi::CStr::from_ptr(value.__bindgen_anon_1.strVal)
                        .to_string_lossy()
                        .into_owned(),
                )),
                ValueType::WStr => {
                    Some(Value::WStr(u16_ptr_to_vec(value.__bindgen_anon_1.wstrVal)?))
                }
                ValueType::Blob => Some(Value::Blob({
                    let v = value.__bindgen_anon_1.blobVal;
                    let len = v.Len as usize;
                    if len == 0 {
                        vec![]
                    } else {
                        let buf = std::slice::from_raw_parts(v.Data, len);
                        buf.to_vec()
                    }
                })),
                ValueType::FileTime => {
                    let v = value.__bindgen_anon_1.ftVal;
                    Some(Value::FileTime(FileTime {
                        dw_low_date_time: v.dwLowDateTime as _,
                        dw_high_date_time: v.dwHighDateTime as _,
                    }))
                }
                ValueType::Timestamp => {
                    let v = value.__bindgen_anon_1.tsVal;
                    Some(Value::Timestamp(DateTime::from_timestamp(
                        v.Seconds as _,
                        (v.Millisecs as u32) * 1000 * 1000,
                    )?))
                }
                ValueType::Var => {
                    let v = value.__bindgen_anon_1.varVal;
                    if v.is_null() {
                        return None;
                    }
                    Some(Value::Var(Box::new(Value::from_kdb_value(&mut *v)?)))
                }
                ValueType::Dec => {
                    let v = value.__bindgen_anon_1.decVal;
                    let dec_v = v.read();
                    Some(Value::Dec(Dec {
                        scale: dec_v.__bindgen_anon_1.__bindgen_anon_1.scale,
                        sign: dec_v.__bindgen_anon_1.__bindgen_anon_1.sign,
                        high_32: dec_v.Hi32 as _,
                        low: dec_v.__bindgen_anon_2.__bindgen_anon_1.Lo32 as _,
                        mid_32: dec_v.__bindgen_anon_2.__bindgen_anon_1.Mid32 as _,
                    }))
                }
            }
        }
    }
}

struct SafeVal(bindings::KDBValue);

impl Drop for SafeVal {
    fn drop(&mut self) {
        free_val(self);
    }
}

fn free_val(val: &mut SafeVal) {
    if ValueType::from(val.0.DataType) == ValueType::Str
        && (unsafe { !val.0.__bindgen_anon_1.strVal.is_null() })
    {
        let _ = unsafe { std::ffi::CString::from_raw(val.0.__bindgen_anon_1.strVal) };
        val.0.__bindgen_anon_1.strVal = std::ptr::null_mut();
    }

    if ValueType::from(val.0.DataType) == ValueType::Var
        && (unsafe { !val.0.__bindgen_anon_1.varVal.is_null() })
    {
        let boxed = unsafe { Box::from_raw(val.0.__bindgen_anon_1.varVal) };
        free_val(&mut SafeVal(*boxed));
        val.0.__bindgen_anon_1.varVal = std::ptr::null_mut();
    }
}

impl Value {
    fn value_type(&self) -> u32 {
        match self {
            Value::Empty => 0,
            Value::Bool(_) => 1,
            Value::I8(_) => 2,
            Value::U8(_) => 3,
            Value::I16(_) => 4,
            Value::U16(_) => 5,
            Value::I32(_) => 6,
            Value::U32(_) => 7,
            Value::I64(_) => 8,
            Value::U64(_) => 9,
            Value::F32(_) => 10,
            Value::F64(_) => 11,
            Value::Str(_) => 12,
            Value::WStr(_) => 13,
            Value::Timestamp(_) => 14,
            Value::Blob(_) => 15,
            Value::Var(_) => 16,
            Value::FileTime(_) => 17,
            Value::Dec(_) => 18,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ValueType {
    Empty = 0,
    Bool = 1,
    I8 = 2,
    U8 = 3,
    I16 = 4,
    U16 = 5,
    I32 = 6,
    U32 = 7,
    I64 = 8,
    U64 = 9,
    F32 = 10,
    F64 = 11,
    Str = 12,
    WStr = 13,
    Timestamp = 14,
    Blob = 15,
    Var = 16,
    FileTime = 17,
    Dec = 18,
}

impl From<u16> for ValueType {
    fn from(value: u16) -> Self {
        match value {
            1 => ValueType::Bool,
            2 => ValueType::I8,
            3 => ValueType::U8,
            4 => ValueType::I16,
            5 => ValueType::U16,
            6 => ValueType::I32,
            7 => ValueType::U32,
            8 => ValueType::I64,
            9 => ValueType::U64,
            10 => ValueType::F32,
            11 => ValueType::F64,
            12 => ValueType::Str,
            13 => ValueType::WStr,
            14 => ValueType::Timestamp,
            15 => ValueType::Blob,
            16 => ValueType::Var,
            17 => ValueType::FileTime,
            18 => ValueType::Dec,
            _ => ValueType::Empty,
        }
    }
}

#[derive(Debug)]
pub struct Dec {
    scale: u8,
    sign: u8,
    high_32: u32,
    low: u32,
    mid_32: u32,
}

#[derive(Debug)]
pub struct FileTime {
    dw_low_date_time: u32,
    dw_high_date_time: u32,
}
pub struct DataCriteriaBuilder<'a> {
    tag_names: &'a [String],
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    data_version: Option<DataVersion>,
    sampling_mode: Option<SamplingMode>,
    sampling_number: Option<u32>,
    sampling_interval_ms: Option<u64>,
    calculation_mode: Option<CalculationMode>,
    filter_tag: Option<&'a str>,
    filter_mode: Option<FilterMode>,
    filter_comparison_mode: Option<FilterComparisonMode>,
    filter_comparison_value: Option<Value>,
    row_count: Option<u32>,
    digital_as_string: Option<bool>,
}

impl<'a> DataCriteriaBuilder<'a> {
    pub fn start_time(mut self, start_time: DateTime<Utc>) -> Self {
        self.start_time = Some(start_time);
        self
    }

    pub fn end_time(mut self, end_time: DateTime<Utc>) -> Self {
        self.end_time = Some(end_time);
        self
    }

    pub fn data_version(mut self, data_version: DataVersion) -> Self {
        self.data_version = Some(data_version);
        self
    }

    pub fn sampling_mode(mut self, sampling_mode: SamplingMode) -> Self {
        self.sampling_mode = Some(sampling_mode);
        self
    }

    pub fn sampling_number(mut self, sampling_number: u32) -> Self {
        self.sampling_number = Some(sampling_number);
        self
    }

    pub fn sampling_interval_ms(mut self, sampling_interval_ms: u64) -> Self {
        self.sampling_interval_ms = Some(sampling_interval_ms);
        self
    }

    pub fn calculation_mode(mut self, calculation_mode: CalculationMode) -> Self {
        self.calculation_mode = Some(calculation_mode);
        self
    }

    pub fn filter_tag(mut self, filter_tag: &'a str) -> Self {
        self.filter_tag = Some(filter_tag);
        self
    }

    pub fn filter_mode(mut self, filter_mode: FilterMode) -> Self {
        self.filter_mode = Some(filter_mode);
        self
    }

    pub fn filter_comparison_mode(mut self, mode: FilterComparisonMode) -> Self {
        self.filter_comparison_mode = Some(mode);
        self
    }

    pub fn filter_comparison_value(mut self, value: Value) -> Self {
        self.filter_comparison_value = Some(value);
        self
    }

    pub fn row_count(mut self, row_count: u32) -> Self {
        self.row_count = Some(row_count);
        self
    }

    pub fn digital_as_string(mut self, digital_as_string: bool) -> Self {
        self.digital_as_string = Some(digital_as_string);
        self
    }

    // 可选：提供一个 build() 方法返回最终的 DataCriteria（如果你有目标结构）
    pub fn build(self) -> DataCriteria {
        DataCriteria {
            tag_names: self
                .tag_names
                .iter()
                .map(|s| string_to_wptr_vec(s))
                .collect(),
            start_time: self.start_time,
            end_time: self.end_time,
            data_version: self.data_version,
            sampling_mode: self.sampling_mode,
            sampling_number: self.sampling_number,
            sampling_interval_ms: self.sampling_interval_ms,
            calculation_mode: self.calculation_mode,
            filter_tag: self.filter_tag.map(string_to_wptr_vec),
            filter_mode: self.filter_mode,
            filter_comparison_mode: self.filter_comparison_mode,
            filter_comparison_value: self.filter_comparison_value,
            row_count: self.row_count,
            digital_as_string: self.digital_as_string,
        }
    }
}

pub struct DataCriteria {
    tag_names: Vec<Vec<bindings::KDB_WCHAR>>,
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    data_version: Option<DataVersion>,
    sampling_mode: Option<SamplingMode>,
    sampling_number: Option<u32>,
    sampling_interval_ms: Option<u64>,
    calculation_mode: Option<CalculationMode>,
    filter_tag: Option<Vec<bindings::KDB_WCHAR>>,
    filter_mode: Option<FilterMode>,
    filter_comparison_mode: Option<FilterComparisonMode>,
    filter_comparison_value: Option<Value>,
    row_count: Option<u32>,
    digital_as_string: Option<bool>,
}

impl DataCriteria {
    pub fn builder<'a>(tag_names: &'a [String]) -> DataCriteriaBuilder<'a> {
        DataCriteriaBuilder {
            tag_names,
            start_time: None,
            end_time: None,
            data_version: None,
            sampling_mode: None,
            sampling_number: None,
            sampling_interval_ms: None,
            calculation_mode: None,
            filter_tag: None,
            filter_mode: None,
            filter_comparison_mode: None,
            filter_comparison_value: None,
            row_count: None,
            digital_as_string: None,
        }
    }
}

#[derive(Debug)]
pub struct DataRecord {
    tag_name: Option<Vec<bindings::KDB_WCHAR>>,
    pub digital_set_id: i16,
    pub data_type: ValueType,
    pub data: Vec<Data>,
}

impl DataRecord {
    pub fn tag_name(&self) -> Option<String> {
        self.tag_name
            .as_ref()
            .map(|v| string_from_wptr_vec(&v))
            .flatten()
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
    tag_name_mask: Option<&'a str>,
    tag_names: Option<&'a [String]>,
    description_mask: Option<&'a str>,
    collector_name: Option<&'a str>,
    source_address: Option<&'a str>,
}

impl<'a> TagCriteriaBuilder<'a> {
    pub fn tag_name_mask(mut self, mask: &'a str) -> Self {
        self.tag_name_mask = Some(mask);
        self
    }

    pub fn tag_names(mut self, names: &'a [String]) -> Self {
        self.tag_names = Some(names);
        self
    }

    pub fn description_mask(mut self, mask: &'a str) -> Self {
        self.description_mask = Some(mask);
        self
    }

    pub fn collector_name(mut self, name: &'a str) -> Self {
        self.collector_name = Some(name);
        self
    }

    pub fn source_address(mut self, addr: &'a str) -> Self {
        self.source_address = Some(addr);
        self
    }

    pub fn build(self) -> TagCriteria {
        TagCriteria {
            tag_name_mask: self.tag_name_mask.map(|s| string_to_wptr_vec(&s)),
            tag_names: self
                .tag_names
                .map(|s| s.iter().map(|a| string_to_wptr_vec(a)).collect()),
            description_mask: self.description_mask.map(|s| string_to_wptr_vec(&s)),
            collector_name: self.collector_name.map(|s| string_to_wptr_vec(&s)),
            source_address: self.source_address.map(|s| string_to_wptr_vec(&s)),
        }
    }
}

pub struct TagCriteria {
    tag_name_mask: Option<Vec<bindings::KDB_WCHAR>>,
    tag_names: Option<Vec<Vec<bindings::KDB_WCHAR>>>,
    description_mask: Option<Vec<bindings::KDB_WCHAR>>,
    collector_name: Option<Vec<bindings::KDB_WCHAR>>,
    source_address: Option<Vec<bindings::KDB_WCHAR>>,
}

impl TagCriteria {
    pub fn builder<'a>() -> TagCriteriaBuilder<'a> {
        TagCriteriaBuilder::default()
    }
}

#[derive(Debug, Default)]
pub struct TagFields {
    pub all_fields: bool,
    pub tag_name: bool,
    pub engineering_unit: bool,
    pub description: bool,
    pub tag_id: bool,
    pub digital_set_id: bool,
    pub collector_name: bool,
    pub collector_type: bool,
    pub source_address: bool,
    pub data_type: bool,
    pub data_length: bool,
    pub collection_control: bool,
    pub collection_mode: bool,
    pub collection_interval: bool,
    pub collection_offset: bool,
    pub timestamp_type: bool,
    pub time_zone_bias: bool,
    pub time_adjustment: bool,
    pub max_value: bool,
    pub min_value: bool,
    pub input_conversion: bool,
    pub max_raw: bool,
    pub min_raw: bool,
    pub collector_compression: bool,
    pub collector_compression_mode: bool,
    pub collector_absolute_deadbanding: bool,
    pub collector_deadband_percent: bool,
    pub collector_absolute_deadband: bool,
    pub collector_compression_timeout: bool,
    pub collector_compression_timeout_min: bool,
    pub archive_control: bool,
    pub archive_version_support: bool,
    pub archive_shutdown: bool,
    pub archive_step_value: bool,
    pub archive_store_mode: bool,
    pub archive_compression: bool,
    pub archive_absolute_deadbanding: bool,
    pub archive_compression_mode: bool,
    pub archive_deadband_percent: bool,
    pub archive_absolute_deadband: bool,
    pub archive_compression_timeout: bool,
    pub archive_compression_timeout_min: bool,
    pub security_read_role: bool,
    pub security_write_role: bool,
    pub security_admin_role: bool,
    pub create_time: bool,
    pub last_modified: bool,
    pub create_user: bool,
    pub last_modified_user: bool,
    pub electronic_record: bool,
    pub calculation: bool,
    pub calculation_triggers: bool,
}

impl TagFields {
    pub fn to_kdb_tag_fields(self) -> bindings::KDBTagFields {
        bindings::KDBTagFields {
            AllFields: self.all_fields as _,
            TagName: self.tag_name as _,
            EngineeringUnit: self.engineering_unit as _,
            Description: self.description as _,
            TagId: self.tag_id as _,
            DigitalSetId: self.digital_set_id as _,
            CollectorName: self.collector_name as _,
            CollectorType: self.collector_type as _,
            SourceAddress: self.source_address as _,
            DataType: self.data_type as _,
            DataLength: self.data_length as _,
            CollectionControl: self.collection_control as _,
            CollectionMode: self.collection_mode as _,
            CollectionInterval: self.collection_interval as _,
            CollectionOffset: self.collection_offset as _,
            TimestampType: self.timestamp_type as _,
            TimeZoneBias: self.time_zone_bias as _,
            TimeAdjustment: self.time_adjustment as _,
            MaxValue: self.max_value as _,
            MinValue: self.min_value as _,
            InputConversion: self.input_conversion as _,
            MaxRaw: self.max_raw as _,
            MinRaw: self.min_raw as _,
            CollectorCompression: self.collector_compression as _,
            CollectorCompressionMode: self.collector_compression_mode as _,
            CollectorAbsoluteDeadbanding: self.collector_absolute_deadbanding as _,
            CollectorDeadbandPercent: self.collector_deadband_percent as _,
            CollectorAbsoluteDeadband: self.collector_absolute_deadband as _,
            CollectorCompressionTimeout: self.collector_compression_timeout as _,
            CollectorCompressionTimeoutMin: self.collector_compression_timeout_min as _,
            ArchiveControl: self.archive_control as _,
            ArchiveVersionSupport: self.archive_version_support as _,
            ArchiveShutdown: self.archive_shutdown as _,
            ArchiveStepValue: self.archive_step_value as _,
            ArchiveStoreMode: self.archive_store_mode as _,
            ArchiveCompression: self.archive_compression as _,
            ArchiveAbsoluteDeadbanding: self.archive_absolute_deadbanding as _,
            ArchiveCompressionMode: self.archive_compression_mode as _,
            ArchiveDeadbandPercent: self.archive_deadband_percent as _,
            ArchiveAbsoluteDeadband: self.archive_absolute_deadband as _,
            ArchiveCompressionTimeout: self.archive_compression_timeout as _,
            ArchiveCompressionTimeoutMin: self.archive_compression_timeout_min as _,
            SecurityReadRole: self.security_read_role as _,
            SecurityWriteRole: self.security_write_role as _,
            SecurityAdminRole: self.security_admin_role as _,
            CreateTime: self.create_time as _,
            LastModified: self.last_modified as _,
            CreateUser: self.create_user as _,
            LastModifiedUser: self.last_modified_user as _,
            ElectronicRecord: self.electronic_record as _,
            Calculation: self.calculation as _,
            CalculationTriggers: self.calculation_triggers as _,
            TagGeneral1: false as _,
            TagGeneral2: false as _,
            TagGeneral3: false as _,
            TagGeneral4: false as _,
            TagGeneral5: false as _,
            TagGeneral6: false as _,
            TagGeneral7: false as _,
            TagGeneral8: false as _,
            TagGeneral9: false as _,
            TagGeneral10: false as _,
            TagGeneral11: false as _,
            TagGeneral12: false as _,
            TagGeneral13: false as _,
            TagGeneral14: false as _,
            TagGeneral15: false as _,
            TagGeneral16: false as _,
            TagGeneral17: false as _,
            TagGeneral18: false as _,
            TagGeneral19: false as _,
            TagGeneral20: false as _,
            SystemGeneral1: false as _,
            SystemGeneral2: false as _,
            SystemGeneral3: false as _,
            SystemGeneral4: false as _,
            SystemGeneral5: false as _,
            SystemGeneral6: false as _,
            SystemGeneral7: false as _,
            SystemGeneral8: false as _,
            SystemGeneral9: false as _,
            SystemGeneral10: false as _,
            SystemGeneral11: false as _,
            SystemGeneral12: false as _,
            SystemGeneral13: false as _,
            SystemGeneral14: false as _,
            SystemGeneral15: false as _,
            SystemGeneral16: false as _,
            SystemGeneral17: false as _,
            SystemGeneral18: false as _,
            SystemGeneral19: false as _,
            SystemGeneral20: false as _,
            UserGeneral1: false as _,
            UserGeneral2: false as _,
            UserGeneral3: false as _,
            UserGeneral4: false as _,
            UserGeneral5: false as _,
            UserGeneral6: false as _,
            UserGeneral7: false as _,
            UserGeneral8: false as _,
            UserGeneral9: false as _,
            UserGeneral10: false as _,
        }
    }
}

#[derive(Debug, Default)]
pub struct TagFieldsBuilder {
    all_fields: bool,
    tag_name: bool,
    engineering_unit: bool,
    description: bool,
    tag_id: bool,
    digital_set_id: bool,
    collector_name: bool,
    collector_type: bool,
    source_address: bool,
    data_type: bool,
    data_length: bool,
    collection_control: bool,
    collection_mode: bool,
    collection_interval: bool,
    collection_offset: bool,
    timestamp_type: bool,
    time_zone_bias: bool,
    time_adjustment: bool,
    max_value: bool,
    min_value: bool,
    input_conversion: bool,
    max_raw: bool,
    min_raw: bool,
    collector_compression: bool,
    collector_compression_mode: bool,
    collector_absolute_deadbanding: bool,
    collector_deadband_percent: bool,
    collector_absolute_deadband: bool,
    collector_compression_timeout: bool,
    collector_compression_timeout_min: bool,
    archive_control: bool,
    archive_version_support: bool,
    archive_shutdown: bool,
    archive_step_value: bool,
    archive_store_mode: bool,
    archive_compression: bool,
    archive_absolute_deadbanding: bool,
    archive_compression_mode: bool,
    archive_deadband_percent: bool,
    archive_absolute_deadband: bool,
    archive_compression_timeout: bool,
    archive_compression_timeout_min: bool,
    security_read_role: bool,
    security_write_role: bool,
    security_admin_role: bool,
    create_time: bool,
    last_modified: bool,
    create_user: bool,
    last_modified_user: bool,
    electronic_record: bool,
    calculation: bool,
    calculation_triggers: bool,
}

impl TagFieldsBuilder {
    pub fn all_fields(mut self) -> Self {
        self.all_fields = true;
        self
    }

    pub fn tag_name(mut self) -> Self {
        self.tag_name = true;
        self
    }

    pub fn engineering_unit(mut self) -> Self {
        self.engineering_unit = true;
        self
    }

    pub fn description(mut self) -> Self {
        self.description = true;
        self
    }

    pub fn tag_id(mut self) -> Self {
        self.tag_id = true;
        self
    }

    pub fn digital_set_id(mut self) -> Self {
        self.digital_set_id = true;
        self
    }

    pub fn collector_name(mut self) -> Self {
        self.collector_name = true;
        self
    }

    pub fn collector_type(mut self) -> Self {
        self.collector_type = true;
        self
    }

    pub fn source_address(mut self) -> Self {
        self.source_address = true;
        self
    }

    pub fn data_type(mut self) -> Self {
        self.data_type = true;
        self
    }

    pub fn data_length(mut self) -> Self {
        self.data_length = true;
        self
    }

    pub fn collection_control(mut self) -> Self {
        self.collection_control = true;
        self
    }

    pub fn collection_mode(mut self) -> Self {
        self.collection_mode = true;
        self
    }

    pub fn collection_interval(mut self) -> Self {
        self.collection_interval = true;
        self
    }

    pub fn collection_offset(mut self) -> Self {
        self.collection_offset = true;
        self
    }

    pub fn timestamp_type(mut self) -> Self {
        self.timestamp_type = true;
        self
    }

    pub fn time_zone_bias(mut self) -> Self {
        self.time_zone_bias = true;
        self
    }

    pub fn time_adjustment(mut self) -> Self {
        self.time_adjustment = true;
        self
    }

    pub fn max_value(mut self) -> Self {
        self.max_value = true;
        self
    }

    pub fn min_value(mut self) -> Self {
        self.min_value = true;
        self
    }

    pub fn input_conversion(mut self) -> Self {
        self.input_conversion = true;
        self
    }

    pub fn max_raw(mut self) -> Self {
        self.max_raw = true;
        self
    }

    pub fn min_raw(mut self) -> Self {
        self.min_raw = true;
        self
    }

    pub fn collector_compression(mut self) -> Self {
        self.collector_compression = true;
        self
    }

    pub fn collector_compression_mode(mut self) -> Self {
        self.collector_compression_mode = true;
        self
    }

    pub fn collector_absolute_deadbanding(mut self) -> Self {
        self.collector_absolute_deadbanding = true;
        self
    }

    pub fn collector_deadband_percent(mut self) -> Self {
        self.collector_deadband_percent = true;
        self
    }

    pub fn collector_absolute_deadband(mut self) -> Self {
        self.collector_absolute_deadband = true;
        self
    }

    pub fn collector_compression_timeout(mut self) -> Self {
        self.collector_compression_timeout = true;
        self
    }

    pub fn collector_compression_timeout_min(mut self) -> Self {
        self.collector_compression_timeout_min = true;
        self
    }

    pub fn archive_control(mut self) -> Self {
        self.archive_control = true;
        self
    }

    pub fn archive_version_support(mut self) -> Self {
        self.archive_version_support = true;
        self
    }

    pub fn archive_shutdown(mut self) -> Self {
        self.archive_shutdown = true;
        self
    }

    pub fn archive_step_value(mut self) -> Self {
        self.archive_step_value = true;
        self
    }

    pub fn archive_store_mode(mut self) -> Self {
        self.archive_store_mode = true;
        self
    }

    pub fn archive_compression(mut self) -> Self {
        self.archive_compression = true;
        self
    }

    pub fn archive_absolute_deadbanding(mut self) -> Self {
        self.archive_absolute_deadbanding = true;
        self
    }

    pub fn archive_compression_mode(mut self) -> Self {
        self.archive_compression_mode = true;
        self
    }

    pub fn archive_deadband_percent(mut self) -> Self {
        self.archive_deadband_percent = true;
        self
    }

    pub fn archive_absolute_deadband(mut self) -> Self {
        self.archive_absolute_deadband = true;
        self
    }

    pub fn archive_compression_timeout(mut self) -> Self {
        self.archive_compression_timeout = true;
        self
    }

    pub fn archive_compression_timeout_min(mut self) -> Self {
        self.archive_compression_timeout_min = true;
        self
    }

    pub fn security_read_role(mut self) -> Self {
        self.security_read_role = true;
        self
    }

    pub fn security_write_role(mut self) -> Self {
        self.security_write_role = true;
        self
    }

    pub fn security_admin_role(mut self) -> Self {
        self.security_admin_role = true;
        self
    }

    pub fn create_time(mut self) -> Self {
        self.create_time = true;
        self
    }

    pub fn last_modified(mut self) -> Self {
        self.last_modified = true;
        self
    }

    pub fn create_user(mut self) -> Self {
        self.create_user = true;
        self
    }

    pub fn last_modified_user(mut self) -> Self {
        self.last_modified_user = true;
        self
    }

    pub fn electronic_record(mut self) -> Self {
        self.electronic_record = true;
        self
    }

    pub fn calculation(mut self) -> Self {
        self.calculation = true;
        self
    }

    pub fn calculation_triggers(mut self) -> Self {
        self.calculation_triggers = true;
        self
    }

    pub fn build(self) -> TagFields {
        TagFields {
            all_fields: self.all_fields,
            tag_name: self.tag_name,
            engineering_unit: self.engineering_unit,
            description: self.description,
            tag_id: self.tag_id,
            digital_set_id: self.digital_set_id,
            collector_name: self.collector_name,
            collector_type: self.collector_type,
            source_address: self.source_address,
            data_type: self.data_type,
            data_length: self.data_length,
            collection_control: self.collection_control,
            collection_mode: self.collection_mode,
            collection_interval: self.collection_interval,
            collection_offset: self.collection_offset,
            timestamp_type: self.timestamp_type,
            time_zone_bias: self.time_zone_bias,
            time_adjustment: self.time_adjustment,
            max_value: self.max_value,
            min_value: self.min_value,
            input_conversion: self.input_conversion,
            max_raw: self.max_raw,
            min_raw: self.min_raw,
            collector_compression: self.collector_compression,
            collector_compression_mode: self.collector_compression_mode,
            collector_absolute_deadbanding: self.collector_absolute_deadbanding,
            collector_deadband_percent: self.collector_deadband_percent,
            collector_absolute_deadband: self.collector_absolute_deadband,
            collector_compression_timeout: self.collector_compression_timeout,
            collector_compression_timeout_min: self.collector_compression_timeout_min,
            archive_control: self.archive_control,
            archive_version_support: self.archive_version_support,
            archive_shutdown: self.archive_shutdown,
            archive_step_value: self.archive_step_value,
            archive_store_mode: self.archive_store_mode,
            archive_compression: self.archive_compression,
            archive_absolute_deadbanding: self.archive_absolute_deadbanding,
            archive_compression_mode: self.archive_compression_mode,
            archive_deadband_percent: self.archive_deadband_percent,
            archive_absolute_deadband: self.archive_absolute_deadband,
            archive_compression_timeout: self.archive_compression_timeout,
            archive_compression_timeout_min: self.archive_compression_timeout_min,
            security_read_role: self.security_read_role,
            security_write_role: self.security_write_role,
            security_admin_role: self.security_admin_role,
            create_time: self.create_time,
            last_modified: self.last_modified,
            create_user: self.create_user,
            last_modified_user: self.last_modified_user,
            electronic_record: self.electronic_record,
            calculation: self.calculation,
            calculation_triggers: self.calculation_triggers,
        }
    }
}

impl TagFields {
    pub fn builder() -> TagFieldsBuilder {
        TagFieldsBuilder::default()
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

impl TagProperties {
    fn from_kdb(prop: &bindings::KDBTagProperties) -> Self {
        TagProperties {
            tag_name: string_from_wptr(prop.TagName),
            engineering_unit: string_from_wptr(prop.EngineeringUnit),
            description: string_from_wptr(prop.Description),
            tag_id: Some(prop.TagId as _),
            digital_set_id: Some(prop.DigitalSetId as _),
            collector_name: string_from_wptr(prop.CollectorName),
            collector_type: CollectorType::from_i32(prop.CollectorType as _),
            source_address: string_from_wptr(prop.SourceAddress),
            data_type: HistoryDataType::from_i32(prop.DataType as _),
            data_length: Some(prop.DataLength as _),
            collection_control: Some(prop.CollectionControl != 0),
            collection_mode: CollectionMode::from_i32(prop.CollectionMode as _),
            collection_interval: Some(prop.CollectionInterval as _),
            collection_offset: Some(prop.CollectionOffset as _),
            timestamp_type: TimestampType::from_i32(prop.TimestampType as _),
            time_zone_bias: Some(prop.TimeZoneBias as _),
            time_adjustment: Some(prop.TimeAdjustment as _),
            max_value: Some(prop.MaxValue as _),
            min_value: Some(prop.MinValue as _),
            input_conversion: InputConversion::from_i32(prop.InputConversion as _),
            max_raw: Some(prop.MaxRaw as _),
            min_raw: Some(prop.MinRaw as _),
            collector_compression: Some(prop.CollectorCompression != 0),
            collector_compression_mode: Some(prop.CollectorCompressionMode as _),
            collector_absolute_deadbanding: Some(prop.CollectorAbsoluteDeadbanding != 0),
            collector_deadband_percent: Some(prop.CollectorDeadbandPercent as _),
            collector_absolute_deadband: Some(prop.CollectorAbsoluteDeadband as _),
            collector_compression_timeout: Some(prop.CollectorCompressionTimeout as _),
            collector_compression_timeout_min: Some(prop.CollectorCompressionTimeoutMin as _),
            archive_control: Some(prop.ArchiveControl != 0),
            archive_version_support: Some(prop.ArchiveVersionSupport != 0),
            archive_shutdown: Some(prop.ArchiveShutdown != 0),
            archive_step_value: Some(prop.ArchiveStepValue != 0),
            archive_store_mode: Some(prop.ArchiveStoreMode as _),
            archive_compression: Some(prop.ArchiveCompression != 0),
            archive_absolute_deadbanding: Some(prop.ArchiveAbsoluteDeadbanding != 0),
            archive_compression_mode: Some(prop.ArchiveCompressionMode as _),
            archive_deadband_percent: Some(prop.ArchiveDeadbandPercent as _),
            archive_absolute_deadband: Some(prop.ArchiveAbsoluteDeadband as _),
            archive_compression_timeout: Some(prop.ArchiveCompressionTimeout as _),
            archive_compression_timeout_min: Some(prop.ArchiveCompressionTimeoutMin as _),
            security_read_role: string_from_wptr(prop.SecurityReadRole),
            security_write_role: string_from_wptr(prop.SecurityWriteRole),
            security_admin_role: string_from_wptr(prop.SecurityAdminRole),
            create_time: {
                let ts = prop.CreateTime;
                if ts.Seconds == 0 && ts.Millisecs == 0 {
                    None
                } else {
                    DateTime::from_timestamp(ts.Seconds as _, (ts.Millisecs as u32) * 1000 * 1000)
                }
            },
            last_modified: {
                let ts = prop.LastModified;
                if ts.Seconds == 0 && ts.Millisecs == 0 {
                    None
                } else {
                    DateTime::from_timestamp(ts.Seconds as _, (ts.Millisecs as u32) * 1000 * 1000)
                }
            },
            create_user: string_from_wptr(prop.CreateUser),
            last_modified_user: string_from_wptr(prop.LastModifiedUser),
            electronic_record: Some(prop.ElectronicRecord as _),
            calculation: string_from_wptr(prop.Calculation),
            calculation_triggers: {
                if prop.NumberOfCalculationTriggers == 0 {
                    None
                } else {
                    let len = prop.NumberOfCalculationTriggers as usize;
                    if len == 0 {
                        Some(vec![])
                    } else {
                        let mut res = Vec::with_capacity(len);
                        let triggers =
                            unsafe { std::slice::from_raw_parts(prop.CalculationTriggers, len) };
                        for &trigger in triggers {
                            if let Some(trigger) = string_from_wptr(trigger) {
                                res.push(trigger);
                            }
                        }
                        Some(res)
                    }
                }
            },
        }
    }
}

#[repr(i32)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum CollectorType {
    #[default]
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

impl CollectorType {
    pub fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(CollectorType::Unknown),
            1 => Some(CollectorType::CalculationEngine),
            2 => Some(CollectorType::Opc),
            3 => Some(CollectorType::AlarmServer),
            4 => Some(CollectorType::Kingview),
            5 => Some(CollectorType::File),
            6 => Some(CollectorType::ServerToServer),
            7 => Some(CollectorType::KingviewLab),
            8 => Some(CollectorType::Simulation),
            9 => Some(CollectorType::Manual),
            10 => Some(CollectorType::Other),
            11 => Some(CollectorType::PiCollector),
            12 => Some(CollectorType::PiDistributor),
            13 => Some(CollectorType::KingIoServer),
            14 => Some(CollectorType::IfixCollector),
            _ => None,
        }
    }
}

#[repr(i32)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum HistoryDataType {
    #[default]
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

impl HistoryDataType {
    pub fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(HistoryDataType::Empty),
            1 => Some(HistoryDataType::Boolean),
            2 => Some(HistoryDataType::Int8),
            3 => Some(HistoryDataType::Int16),
            4 => Some(HistoryDataType::Int32),
            5 => Some(HistoryDataType::Int64),
            6 => Some(HistoryDataType::Float32),
            7 => Some(HistoryDataType::Float64),
            9 => Some(HistoryDataType::Decimal),
            10 => Some(HistoryDataType::Char),
            11 => Some(HistoryDataType::Varchar),
            13 => Some(HistoryDataType::Nchar),
            14 => Some(HistoryDataType::Nvarchar),
            18 => Some(HistoryDataType::Timestamp),
            19 => Some(HistoryDataType::Binary),
            20 => Some(HistoryDataType::Varbinary),
            101 => Some(HistoryDataType::Digital),
            102 => Some(HistoryDataType::Float16),
            _ => None,
        }
    }
}

#[repr(i32)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum CollectionMode {
    #[default]
    Unknown = 0,
    Unsolicited = 1,
    Polled = 2,
}

impl CollectionMode {
    pub fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(CollectionMode::Unknown),
            1 => Some(CollectionMode::Unsolicited),
            2 => Some(CollectionMode::Polled),
            _ => None,
        }
    }
}

#[repr(i32)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum TimestampType {
    #[default]
    Source = 1,
    Collector = 2,
}

impl TimestampType {
    pub fn from_i32(value: i32) -> Option<Self> {
        match value {
            1 => Some(TimestampType::Source),
            2 => Some(TimestampType::Collector),
            _ => None,
        }
    }
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputConversion {
    NoConversion = 0,
    Linear = 1,
    Sqrt = 2,
}

impl Default for InputConversion {
    fn default() -> Self {
        InputConversion::NoConversion
    }
}

impl InputConversion {
    pub fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(InputConversion::NoConversion),
            1 => Some(InputConversion::Linear),
            2 => Some(InputConversion::Sqrt),
            _ => None,
        }
    }
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemChangeType {
    NoChange = 0,
    Added = 1,
    Deleted = 2,
    Modified = 3,
}

impl ItemChangeType {
    fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(Self::NoChange),
            1 => Some(Self::Added),
            2 => Some(Self::Deleted),
            3 => Some(Self::Modified),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct TagGroupProperties {
    pub group_id: u32,
    pub parent_id: u32,
    pub group_name: Option<String>,
    pub description: Option<String>,
}

impl TagGroupProperties {
    fn from_kdb(prop: &bindings::KDBTagGroupProperties) -> Self {
        TagGroupProperties {
            group_id: prop.GroupID,
            parent_id: prop.ParentID,
            group_name: string_from_wptr(prop.GroupName),
            description: string_from_wptr(prop.Description),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_type_from_codes_should_match_header() {
        // Integer family
        assert_eq!(ValueType::from(2u16), ValueType::I8);
        assert_eq!(ValueType::from(3u16), ValueType::U8);
        assert_eq!(ValueType::from(4u16), ValueType::I16);
        assert_eq!(ValueType::from(5u16), ValueType::U16);
        assert_eq!(ValueType::from(6u16), ValueType::I32);
        assert_eq!(ValueType::from(7u16), ValueType::U32);
        assert_eq!(ValueType::from(8u16), ValueType::I64);
        assert_eq!(ValueType::from(9u16), ValueType::U64);

        // Float & string
        assert_eq!(ValueType::from(10u16), ValueType::F32);
        assert_eq!(ValueType::from(11u16), ValueType::F64);
        assert_eq!(ValueType::from(12u16), ValueType::Str);
        assert_eq!(ValueType::from(13u16), ValueType::WStr);

        // Others
        assert_eq!(ValueType::from(14u16), ValueType::Timestamp);
        assert_eq!(ValueType::from(15u16), ValueType::Blob);
        assert_eq!(ValueType::from(16u16), ValueType::Var);
        assert_eq!(ValueType::from(17u16), ValueType::FileTime);
        assert_eq!(ValueType::from(18u16), ValueType::Dec);
    }

    #[test]
    #[ignore]
    fn connect_test() {
        api_start_up().unwrap();
        let opts = ConnectionOptions::builder("127.0.0.1", "5678", "sa", "sa").build();
        let mut conn = match ServerConnection::new(opts) {
            Ok(conn) => conn,
            Err(e) => {
                api_cleanup().unwrap();
                println!("connect error: {e:?}");
                return;
            }
        };
        assert!(conn.is_connected().unwrap());
        api_cleanup().unwrap();
    }

    #[test]
    #[ignore]
    fn tag_search_test() {
        api_start_up().unwrap();
        let opts = ConnectionOptions::builder("127.0.0.1", "5678", "sa", "sa").build();
        let mut conn = match ServerConnection::new(opts) {
            Ok(conn) => conn,
            Err(e) => {
                api_cleanup().unwrap();
                println!("connect error: {e:?}");
                return;
            }
        };
        assert!(conn.is_connected().unwrap());
        let tags = conn.all_tags().unwrap();
        for tag in tags {
            println!("{tag}");
        }
        api_cleanup().unwrap();
    }

    #[test]
    #[ignore]
    fn current_value_test() {
        api_start_up().unwrap();
        let opts = ConnectionOptions::builder("127.0.0.1", "5678", "sa", "sa").build();
        let mut conn = match ServerConnection::new(opts) {
            Ok(conn) => conn,
            Err(e) => {
                api_cleanup().unwrap();
                println!("connect error: {e:?}");
                return;
            }
        };
        assert!(conn.is_connected().unwrap());
        match conn.get_data_current_value(["Tag000020"], false) {
            Ok(values) => {
                println!("len: {}", values.len());
                for v in values {
                    println!("{v:?}");
                }
            }
            Err(e) => {
                println!("error: {e}");
            }
        }
        api_cleanup().unwrap();
    }

    #[test]
    #[ignore]
    fn tag_exists_test() {
        api_start_up().unwrap();
        let opts = ConnectionOptions::builder("127.0.0.1", "5678", "sa", "sa").build();
        let mut conn = match ServerConnection::new(opts) {
            Ok(conn) => conn,
            Err(e) => {
                api_cleanup().unwrap();
                println!("connect error: {e:?}");
                return;
            }
        };
        assert!(conn.is_connected().unwrap());
        match conn.tag_exists("Tag000020") {
            Ok(v) => {
                println!("tag exists: {v}");
            }
            Err(e) => {
                println!("error: {e:?}");
            }
        }
        api_cleanup().unwrap();
    }

    #[test]
    #[ignore]
    fn tag_name_filter_test() {
        api_start_up().unwrap();
        let opts = ConnectionOptions::builder("127.0.0.1", "5678", "sa", "sa").build();
        let mut conn = match ServerConnection::new(opts) {
            Ok(conn) => conn,
            Err(e) => {
                api_cleanup().unwrap();
                println!("connect error: {e:?}");
                return;
            }
        };
        assert!(conn.is_connected().unwrap());
        match conn.get_tag_names_by_filter(Some("OPC".into()), None, None, None) {
            Ok(tags) => {
                for tag in tags {
                    println!("{tag}");
                }
            }
            Err(e) => {
                println!("filter error: {e}");
            }
        }
        api_cleanup().unwrap();
    }

    #[test]
    #[ignore]
    fn data_subscribe_test() {
        api_start_up().unwrap();
        let opts = ConnectionOptions::builder("127.0.0.1", "5678", "sa", "sa").build();
        let mut conn = match ServerConnection::new(opts) {
            Ok(conn) => conn,
            Err(e) => {
                api_cleanup().unwrap();
                println!("connect error: {e:?}");
                return;
            }
        };
        assert!(conn.is_connected().unwrap());
        let (sender, receiver) = flume::bounded(1);
        match conn.data_subscribe(
            // ["OPC_数据类型示例.16 位设备.R 寄存器.Short1"],
            ["OPC_数据类型示例.16 位设备.R 寄存器.Double1"],
            1000,
            Some(sender),
        ) {
            Ok(res) => {
                for (idx, res) in res.iter().enumerate() {
                    if let Err(e) = res {
                        println!("idx {idx} subscribe error: {e}");
                    }
                }
            }
            Err(e) => {
                api_cleanup().unwrap();
                println!("subscribe error: {e:?}");
                return;
            }
        }
        let new_data = receiver.recv().unwrap();
        match new_data {
            Ok(v) => {
                println!(
                    "new data: {:?}, {:?}, {:?}",
                    v.tag_name(),
                    v.data_type,
                    v.data
                );
            }
            Err(e) => {
                println!("error data: {e:?}");
            }
        }
        println!("before api_cleanup");
        api_cleanup().unwrap();
    }

    #[test]
    #[ignore]
    fn query_data_test() {
        api_start_up().unwrap();
        let opts = ConnectionOptions::builder("127.0.0.1", "5678", "sa", "sa").build();
        let mut conn = match ServerConnection::new(opts) {
            Ok(conn) => conn,
            Err(e) => {
                api_cleanup().unwrap();
                println!("connect error: {e:?}");
                return;
            }
        };
        assert!(conn.is_connected().unwrap());

        let filter =
            DataCriteria::builder(&["OPC_数据类型示例.16 位设备.R 寄存器.Double1".to_string()])
                .build();
        match conn.query_tag_values(filter) {
            Ok(tags) => {
                for (key, value) in tags {
                    println!("len: {}, {key}: {:?}", value.len(), value[0]);
                }
            }
            Err(e) => {
                println!("filter error: {e}");
            }
        }
        api_cleanup().unwrap();
    }

    #[test]
    fn test_query_multi_tags() {
        api_start_up().unwrap();
        let opts = ConnectionOptions::builder("127.0.0.1", "5678", "sa", "sa").build();
        let mut conn = match ServerConnection::new(opts) {
            Ok(conn) => conn,
            Err(e) => {
                api_cleanup().unwrap();
                println!("connect error: {e:?}");
                return;
            }
        };
        assert!(conn.is_connected().unwrap());

        let start = DateTime::parse_from_rfc3339("2025-10-15T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339("2025-10-16T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        use std::io::Write;
        use std::time::Instant;

        let file = std::fs::File::create("test_query_multi_tags.txt").unwrap();
        let mut file = std::io::BufWriter::new(file);
        let elapse = Instant::now();
        let filter = DataCriteria::builder(&[
            "OPC_数据类型示例.16 位设备.R 寄存器.Double1".to_string(),
            "OPC_数据类型示例.16 位设备.R 寄存器.Double2".to_string(),
            "OPC_数据类型示例.16 位设备.R 寄存器.Double3".to_string(),
            "OPC_数据类型示例.16 位设备.R 寄存器.Double4".to_string(),
        ])
        .start_time(start)
        .end_time(end)
        .row_count(86400000)
        .build();
        match conn.query_tag_values(filter) {
            Ok(tags) => {
                let elapsed = elapse.elapsed();
                for (key, value) in tags {
                    println!("key: {}, len: {}, elapsed: {:?}", key, value.len(), elapsed);
                    for v in &value {
                        writeln!(file, "{key}, {:?}", v).unwrap();
                    }
                    file.flush().unwrap();
                }
            }
            Err(e) => {
                println!("filter error: {e}");
            }
        }
    }

    #[test]
    #[ignore]
    fn query_tag_metadata_test() {
        api_start_up().unwrap();
        let opts = ConnectionOptions::builder("127.0.0.1", "5678", "sa", "sa").build();
        let mut conn = match ServerConnection::new(opts) {
            Ok(conn) => conn,
            Err(e) => {
                api_cleanup().unwrap();
                println!("connect error: {e:?}");
                return;
            }
        };
        assert!(conn.is_connected().unwrap());

        let filter = TagCriteria::builder()
            .tag_names(&["OPC_数据类型示例.8 位设备.K 寄存器.Double1".to_string()])
            .build();
        let fields = TagFields::builder().all_fields().build();
        match conn.query_tag_properties(filter, fields) {
            Ok(props) => {
                for prop in props {
                    println!("{prop:?}");
                }
            }
            Err(e) => {
                println!("filter error: {e}");
            }
        }
        api_cleanup().unwrap();
    }

    #[test]
    #[ignore]
    fn get_tag_properties_test() {
        api_start_up().unwrap();
        let opts = ConnectionOptions::builder("127.0.0.1", "5678", "sa", "sa").build();
        let mut conn = match ServerConnection::new(opts) {
            Ok(conn) => conn,
            Err(e) => {
                api_cleanup().unwrap();
                println!("connect error: {e:?}");
                return;
            }
        };
        assert!(conn.is_connected().unwrap());

        let fields = TagFields::builder().all_fields().build();
        match conn.get_tag_properties("OPC_数据类型示例.8 位设备.K 寄存器.Double1", fields)
        {
            Ok(props) => {
                println!("{props:?}");
            }
            Err(e) => {
                println!("filter error: {e}");
            }
        }
        api_cleanup().unwrap();
    }

    #[test]
    #[ignore]
    fn tag_subscribe_test() {
        api_start_up().unwrap();
        let opts = ConnectionOptions::builder("127.0.0.1", "5678", "sa", "sa").build();
        let mut conn = match ServerConnection::new(opts) {
            Ok(conn) => conn,
            Err(e) => {
                api_cleanup().unwrap();
                println!("connect error: {e:?}");
                return;
            }
        };
        assert!(conn.is_connected().unwrap());
        let (sender, receiver) = flume::bounded(1);
        match conn.tag_subscribe(["Tag0"], Some(sender)) {
            Ok(res) => {
                for (idx, res) in res.iter().enumerate() {
                    if let Err(e) = res {
                        println!("idx {idx} subscribe error: {e}");
                    }
                }
            }
            Err(e) => {
                api_cleanup().unwrap();
                println!("subscribe error: {e:?}");
                return;
            }
        }

        let (change_type, properties) = receiver.recv().unwrap();
        println!("change type: {change_type:?}, properties: {properties:?}");
        api_cleanup().unwrap();
    }

    #[test]
    #[ignore]
    fn tag_group_get_children_test() {
        api_start_up().unwrap();
        let opts = ConnectionOptions::builder("127.0.0.1", "5678", "sa", "sa").build();
        let mut conn = match ServerConnection::new(opts) {
            Ok(conn) => conn,
            Err(e) => {
                api_cleanup().unwrap();
                println!("connect error: {e:?}");
                return;
            }
        };
        assert!(conn.is_connected().unwrap());
        match conn.tag_group_get_children(1) {
            Ok(res) => {
                println!("{res:?}");
            }
            Err(e) => {
                println!("tag group get children error: {e:?}");
            }
        }

        api_cleanup().unwrap();
    }

    #[test]
    #[ignore]
    fn tag_group_get_tags_test() {
        api_start_up().unwrap();
        let opts = ConnectionOptions::builder("127.0.0.1", "5678", "sa", "sa").build();
        let mut conn = match ServerConnection::new(opts) {
            Ok(conn) => conn,
            Err(e) => {
                api_cleanup().unwrap();
                println!("connect error: {e:?}");
                return;
            }
        };
        assert!(conn.is_connected().unwrap());
        match conn.tag_group_get_tags(1) {
            Ok(res) => {
                println!("{res:?}");
            }
            Err(e) => {
                println!("tag group get tags error: {e:?}");
            }
        }

        api_cleanup().unwrap();
    }

    #[test]
    #[ignore]
    fn get_tag_group_properties_test() {
        api_start_up().unwrap();
        let opts = ConnectionOptions::builder("127.0.0.1", "5678", "sa", "sa").build();
        let mut conn = match ServerConnection::new(opts) {
            Ok(conn) => conn,
            Err(e) => {
                api_cleanup().unwrap();
                println!("connect error: {e:?}");
                return;
            }
        };
        assert!(conn.is_connected().unwrap());
        match conn.get_tag_group_properties(1) {
            Ok(res) => {
                println!("{res:?}");
            }
            Err(e) => {
                println!("get tag group properties error: {e:?}");
            }
        }

        api_cleanup().unwrap();
    }
}
