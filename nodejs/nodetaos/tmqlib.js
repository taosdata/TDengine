const ref = require('ref-napi');
const ffi = require('ffi-napi');
const ArrayType = require('ref-array-di')(ref);
const Struct = require('ref-struct-di')(ref);
const os = require('os');

if ('win32' == os.platform()) {
    taoslibname = 'taos';
} else {
    taoslibname = 'libtaos';
}

ref.types.char_ptr = ref.refType(ref.types.char);
ref.types.void_ptr = ref.refType(ref.types.void);
ref.types.void_ptr2 = ref.refType(ref.types.void_ptr);

var libtmq = ffi.Library(taoslibname,{
     /* --------------------------TMQ INTERFACE------------------------------- */
    // tmq_list_t *tmq_list_new();
    'tmq_list_new':[ref.types.void_ptr,[]]

    // int32_t tmq_list_append(tmq_list_t *, const char *);
    ,'tmq_list_append':[ref.types.int32,[ref.types.void_ptr,ref.types.char_ptr]]

    // void tmq_list_destroy(tmq_list_t *);
    ,'tmq_list_destroy':[ref.types.void,[ref.types.void_ptr]]

    // int32_t tmq_list_get_size(const tmq_list_t *);
    ,'tmq_list_get_size':[ref.types.int32,[ref.types.void_ptr]]

    // char **tmq_list_to_c_array(const tmq_list_t *);
    ,'tmq_list_to_c_array':[ref.refType(ref.types.char_ptr),[ref.types.void_ptr]]
    
    //const char *tmq_err2str(int32_t code);
    ,'tmq_err2str':[ref.types.char_ptr,[ref.types.int32]]

    /* ------------------------TMQ CONSUMER INTERFACE------------------------ */
    // tmq_t *tmq_consumer_new(tmq_conf_t *conf, char *errstr, int32_t errstrLen);
    ,'tmq_consumer_new':[ref.types.void_ptr,[ref.types.void_ptr,ref.types.char_ptr,ref.types.int32]]

    // int32_t tmq_subscribe(tmq_t *tmq, const tmq_list_t *topic_list);
    ,'tmq_subscribe':[ref.types.int32,[ref.types.void_ptr,ref.types.void_ptr]]

    // int32_t tmq_unsubscribe(tmq_t *tmq);
    ,'tmq_unsubscribe':[ref.types.int32,[ref.types.void_ptr]]

    // int32_t tmq_subscription(tmq_t *tmq, tmq_list_t **topics);
    ,'tmq_subscription':[ref.types.int32,[ref.types.void_ptr,ref.types.void_ptr2]]

    // TAOS_RES *tmq_consumer_poll(tmq_t *tmq, int64_t timeout);
    ,'tmq_consumer_poll':[ref.types.void_ptr,[ref.types.void_ptr,ref.types.int64]]

    // int32_t tmq_consumer_close(tmq_t *tmq);
    ,'tmq_consumer_close':[ref.types.int32,[ref.types.void_ptr]]

    // int32_t tmq_commit_sync(tmq_t *tmq, const TAOS_RES *msg);
    ,'tmq_commit_sync':[ref.types.int32,[ref.types.void_ptr,ref.types.void_ptr]]

    // void tmq_commit_async(tmq_t *tmq, const TAOS_RES *msg, tmq_commit_cb *cb, void *param);
    ,'tmq_commit_async':[ref.types.void,[ref.types.void_ptr,ref.types.void_ptr,ref.types.void_ptr,ref.types.void_ptr]]
    
    /* ----------------------TMQ CONFIGURATION INTERFACE---------------------- */
    // tmq_conf_t *tmq_conf_new();
    ,'tmq_conf_new':[ref.types.void_ptr,[]]

    // tmq_conf_res_t tmq_conf_set(tmq_conf_t *conf, const char *key, const char *value);
    ,'tmq_conf_set':[ref.types.int,[ref.types.void_ptr,ref.types.char_ptr,ref.types.char_ptr]]
    // ,'tmq_conf_set':[ref.types.int,[ref.types.void_ptr,'char *','char *']]

    // void tmq_conf_destroy(tmq_conf_t *conf);
    ,'tmq_conf_destroy':[ref.types.void,[ref.types.void_ptr]]

    // void tmq_conf_set_auto_commit_cb(tmq_conf_t *conf, tmq_commit_cb *cb, void *param);
    ,'tmq_conf_set_auto_commit_cb':[ref.types.void,[ref.types.void_ptr,ref.types.void_ptr,ref.types.void_ptr]]
    
    /* -------------------------TMQ MSG HANDLE INTERFACE---------------------- */
    // tmq_res_t tmq_get_res_type(TAOS_RES *res);
    ,'tmq_get_res_type':[ref.types.int,[ref.types.void_ptr]]
    
    // int32_t tmq_get_raw_meta(TAOS_RES *res, void **raw_meta, int32_t *raw_meta_len);
    // ,'tmq_get_raw_meta':[ref.types.int32,[ref.types.void_ptr,ref.types.void_ptr2,'int *']]
    
    // const char *tmq_get_topic_name(TAOS_RES *res);
    ,'tmq_get_topic_name':[ref.types.char_ptr,[ref.types.void_ptr]]
    
    // const char *tmq_get_db_name(TAOS_RES *res);
    ,'tmq_get_db_name':[ref.types.char_ptr,[ref.types.void_ptr]]

    // int32_t tmq_get_vgroup_id(TAOS_RES *res);
    ,'tmq_get_vgroup_id':[ref.types.int32,[ref.types.void_ptr]]

    // const char *tmq_get_table_name(TAOS_RES *res);
    ,'tmq_get_table_name':[ref.types.char_ptr,[ref.types.void_ptr]] 
});
module.exports = libtmq;