use taos_sys::{tmq_topic_vgroup_list_t, tmq_topic_vgroup_t};

pub struct Offsets(pub(crate) *const tmq_topic_vgroup_list_t);

pub struct Offset(pub(crate) *const tmq_topic_vgroup_t);
