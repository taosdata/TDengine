use taos_sys::{tmq_topic_vgroup_list_t, tmq_topic_vgroup_t};

#[derive(Debug)]
pub struct Offsets(pub(crate) *const tmq_topic_vgroup_list_t);

impl Offsets {
    pub fn new() -> Self {
        Offsets(std::ptr::null())
    }

    pub(super) fn as_ptr(&self) -> *const tmq_topic_vgroup_list_t {
        self.0
    }
}

impl From<()> for Offsets {
    fn from(_: ()) -> Self {
        Offsets(std::ptr::null())
    }
}

impl From<*const tmq_topic_vgroup_list_t> for Offsets {
    fn from(v: *const tmq_topic_vgroup_list_t) -> Self {
        Offsets(v)
    }
}

pub struct Offset(pub(crate) *const tmq_topic_vgroup_t);
