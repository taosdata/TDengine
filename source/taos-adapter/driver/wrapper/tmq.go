package wrapper

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
extern void TMQCommitCB(tmq_t *, int32_t,  void *param);
extern void TMQAutoCommitCB(tmq_t *, int32_t,  void *param);
extern void TMQCommitOffsetCB(tmq_t *, int32_t,  void *param);
*/
import "C"
import (
	"sync"
	"unsafe"

	"github.com/taosdata/taosadapter/v3/driver/common/tmq"
	"github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/tools"
)

var tmqCommitCallbackResultPool = sync.Pool{}

type TMQCommitCallbackResult struct {
	ErrCode  int32
	Consumer unsafe.Pointer
}

func (t *TMQCommitCallbackResult) GetError() error {
	if t.ErrCode == 0 {
		return nil
	}
	errStr := TMQErr2Str(t.ErrCode)
	return errors.NewError(int(t.ErrCode), errStr)
}

func GetTMQCommitCallbackResult(errCode int32, consumer unsafe.Pointer) *TMQCommitCallbackResult {
	t, ok := tmqCommitCallbackResultPool.Get().(*TMQCommitCallbackResult)
	if ok {
		t.ErrCode = errCode
		t.Consumer = consumer
		return t
	}
	return &TMQCommitCallbackResult{ErrCode: errCode, Consumer: consumer}
}

func PutTMQCommitCallbackResult(result *TMQCommitCallbackResult) {
	tmqCommitCallbackResultPool.Put(result)
}

// TMQConfNew  tmq_conf_t    *tmq_conf_new();
func TMQConfNew() unsafe.Pointer {
	return unsafe.Pointer(C.tmq_conf_new())
}

// TMQConfSet  tmq_conf_res_t tmq_conf_set(tmq_conf_t *conf, const char *key, const char *value);
func TMQConfSet(conf unsafe.Pointer, key string, value string) int32 {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cValue))
	return int32(C.tmq_conf_set((*C.struct_tmq_conf_t)(conf), cKey, cValue))
}

// TMQConfDestroy void           tmq_conf_destroy(tmq_conf_t *conf);
func TMQConfDestroy(conf unsafe.Pointer) {
	C.tmq_conf_destroy((*C.struct_tmq_conf_t)(conf))
}

// TMQConfSetAutoCommitCB DLL_EXPORT void           tmq_conf_set_auto_commit_cb(tmq_conf_t *conf, tmq_commit_cb *cb, void *param);
func TMQConfSetAutoCommitCB(conf unsafe.Pointer, h cgo.Handle) {
	C.tmq_conf_set_auto_commit_cb((*C.struct_tmq_conf_t)(conf), (*C.tmq_commit_cb)(C.TMQAutoCommitCB), h.Pointer())
}

// TMQCommitAsync DLL_EXPORT void    tmq_commit_async(tmq_t *tmq, const TAOS_RES *msg, tmq_commit_cb *cb, void *param);
func TMQCommitAsync(consumer unsafe.Pointer, message unsafe.Pointer, h cgo.Handle) {
	C.tmq_commit_async((*C.tmq_t)(consumer), message, (*C.tmq_commit_cb)(C.TMQCommitCB), h.Pointer())
}

// TMQCommitSync DLL_EXPORT int32_t tmq_commit_sync(tmq_t *tmq, const TAOS_RES *msg);
func TMQCommitSync(consumer unsafe.Pointer, message unsafe.Pointer) int32 {
	return int32(C.tmq_commit_sync((*C.tmq_t)(consumer), message))
}

// TMQListNew tmq_list_t *tmq_list_new();
func TMQListNew() unsafe.Pointer {
	return unsafe.Pointer(C.tmq_list_new())
}

// TMQListAppend int32_t     tmq_list_append(tmq_list_t *, const char *);
func TMQListAppend(list unsafe.Pointer, str string) int32 {
	cStr := C.CString(str)
	defer C.free(unsafe.Pointer(cStr))
	return int32(C.tmq_list_append((*C.tmq_list_t)(list), cStr))
}

// TMQListDestroy void        tmq_list_destroy(tmq_list_t *);
func TMQListDestroy(list unsafe.Pointer) {
	C.tmq_list_destroy((*C.tmq_list_t)(list))
}

// TMQListGetSize int32_t     tmq_list_get_size(const tmq_list_t *);
func TMQListGetSize(list unsafe.Pointer) int32 {
	return int32(C.tmq_list_get_size((*C.tmq_list_t)(list)))
}

// TMQListToCArray char      **tmq_list_to_c_array(const tmq_list_t *);
func TMQListToCArray(list unsafe.Pointer, size int) []string {
	head := unsafe.Pointer(C.tmq_list_to_c_array((*C.tmq_list_t)(list)))
	result := make([]string, size)
	for i := 0; i < size; i++ {
		result[i] = C.GoString(*(**C.char)(tools.AddPointer(head, PointerSize*uintptr(i))))
	}
	return result
}

// TMQConsumerNew tmq_t *tmq_consumer_new1(tmq_conf_t *conf, char *errstr, int32_t errstrLen);
func TMQConsumerNew(conf unsafe.Pointer) (unsafe.Pointer, error) {
	p := (*C.char)(C.calloc(C.size_t(C.uint(1024)), C.size_t(C.uint(1024))))
	defer C.free(unsafe.Pointer(p))
	tmq := unsafe.Pointer(C.tmq_consumer_new((*C.struct_tmq_conf_t)(conf), p, C.int32_t(1024)))
	errStr := C.GoString(p)
	if len(errStr) > 0 {
		return nil, errors.NewError(-1, errStr)
	}
	if tmq == nil {
		return nil, errors.NewError(-1, "new consumer return nil")
	}
	return tmq, nil
}

// TMQErr2Str const char *tmq_err2str(int32_t);
func TMQErr2Str(code int32) string {
	return C.GoString(C.tmq_err2str((C.int32_t)(code)))
}

// TMQSubscribe tmq_resp_err_t tmq_subscribe(tmq_t *tmq, tmq_list_t *topic_list);
func TMQSubscribe(consumer unsafe.Pointer, topicList unsafe.Pointer) int32 {
	return int32(C.tmq_subscribe((*C.tmq_t)(consumer), (*C.tmq_list_t)(topicList)))
}

// TMQUnsubscribe tmq_resp_err_t tmq_unsubscribe(tmq_t *tmq);
func TMQUnsubscribe(consumer unsafe.Pointer) int32 {
	return int32(C.tmq_unsubscribe((*C.tmq_t)(consumer)))
}

// TMQSubscription tmq_resp_err_t tmq_subscription(tmq_t *tmq, tmq_list_t **topics);
func TMQSubscription(consumer unsafe.Pointer) (int32, unsafe.Pointer) {
	list := C.tmq_list_new()
	code := int32(C.tmq_subscription(
		(*C.tmq_t)(consumer),
		(**C.tmq_list_t)(&list),
	))
	return code, unsafe.Pointer(list)
}

// TMQConsumerPoll TAOS_RES *tmq_consumer_poll(tmq_t *tmq, int64_t blocking_time);
func TMQConsumerPoll(consumer unsafe.Pointer, blockingTime int64) unsafe.Pointer {
	return unsafe.Pointer(C.tmq_consumer_poll((*C.tmq_t)(consumer), (C.int64_t)(blockingTime)))
}

// TMQConsumerClose tmq_resp_err_t tmq_consumer_close(tmq_t *tmq);
func TMQConsumerClose(consumer unsafe.Pointer) int32 {
	return int32(C.tmq_consumer_close((*C.tmq_t)(consumer)))
}

// TMQGetTopicName char       *tmq_get_topic_name(tmq_message_t *message);
func TMQGetTopicName(message unsafe.Pointer) string {
	return C.GoString(C.tmq_get_topic_name(message))
}

// TMQGetVgroupID int32_t     tmq_get_vgroup_id(tmq_message_t *message);
func TMQGetVgroupID(message unsafe.Pointer) int32 {
	return int32(C.tmq_get_vgroup_id(message))
}

// TMQGetTableName DLL_EXPORT const char *tmq_get_table_name(TAOS_RES *res);
func TMQGetTableName(message unsafe.Pointer) string {
	return C.GoString(C.tmq_get_table_name(message))
}

// TMQGetDBName const char *tmq_get_db_name(TAOS_RES *res);
func TMQGetDBName(message unsafe.Pointer) string {
	return C.GoString(C.tmq_get_db_name(message))
}

// TMQGetResType DLL_EXPORT tmq_res_t     tmq_get_res_type(TAOS_RES *res);
func TMQGetResType(message unsafe.Pointer) int32 {
	return int32(C.tmq_get_res_type(message))
}

// TMQGetRaw DLL_EXPORT int32_t       tmq_get_raw(TAOS_RES *res, tmq_raw_data *raw);
func TMQGetRaw(message unsafe.Pointer) (int32, unsafe.Pointer) {
	var cRawMeta C.struct_tmq_raw_data
	m := unsafe.Pointer(&cRawMeta)
	code := int32(C.tmq_get_raw(message, (*C.tmq_raw_data)(m)))
	return code, m
}

// TMQWriteRaw DLL_EXPORT int32_t       tmq_write_raw(TAOS *taos, tmq_raw_data raw);
func TMQWriteRaw(conn unsafe.Pointer, length uint32, metaType uint16, data unsafe.Pointer) int32 {
	meta := C.struct_tmq_raw_data{}
	meta.raw = data
	meta.raw_len = (C.uint32_t)(length)
	meta.raw_type = (C.uint16_t)(metaType)
	return int32(C.tmq_write_raw(conn, meta))
}

// TMQFreeRaw DLL_EXPORT void          tmq_free_raw(tmq_raw_data raw);
func TMQFreeRaw(raw unsafe.Pointer) {
	C.tmq_free_raw((C.struct_tmq_raw_data)(*(*C.struct_tmq_raw_data)(raw)))
}

// TMQGetJsonMeta DLL_EXPORT char         *tmq_get_json_meta(TAOS_RES *res);   // Returning null means error. Returned result need to be freed by tmq_free_json_meta
func TMQGetJsonMeta(message unsafe.Pointer) unsafe.Pointer {
	p := unsafe.Pointer(C.tmq_get_json_meta(message))
	return p
}

// TMQFreeJsonMeta DLL_EXPORT void          tmq_free_json_meta(char* jsonMeta);
func TMQFreeJsonMeta(jsonMeta unsafe.Pointer) {
	C.tmq_free_json_meta((*C.char)(jsonMeta))
}

func ParseRawMeta(rawMeta unsafe.Pointer) (length uint32, metaType uint16, data unsafe.Pointer) {
	meta := *(*C.tmq_raw_data)(rawMeta)
	length = uint32(meta.raw_len)
	metaType = uint16(meta.raw_type)
	data = meta.raw
	return
}

func ParseJsonMeta(jsonMeta unsafe.Pointer) []byte {
	var binaryVal []byte
	if jsonMeta != nil {
		i := 0
		c := byte(0)
		for {
			c = *((*byte)(unsafe.Pointer(uintptr(jsonMeta) + uintptr(i))))
			if c != 0 {
				binaryVal = append(binaryVal, c)
				i += 1
			} else {
				break
			}
		}
	}
	return binaryVal
}

// TMQGetTopicAssignment DLL_EXPORT int32_t   tmq_get_topic_assignment(tmq_t *tmq, const char* pTopicName, tmq_topic_assignment **assignment, int32_t *numOfAssignment)
func TMQGetTopicAssignment(consumer unsafe.Pointer, topic string) (int32, []*tmq.Assignment) {
	var assignment *C.tmq_topic_assignment
	var numOfAssignment int32
	topicName := C.CString(topic)
	defer C.free(unsafe.Pointer(topicName))
	code := int32(C.tmq_get_topic_assignment((*C.tmq_t)(consumer), topicName, (**C.tmq_topic_assignment)(unsafe.Pointer(&assignment)), (*C.int32_t)(&numOfAssignment)))
	if code != 0 {
		return code, nil
	}
	if assignment == nil {
		return 0, nil
	}
	defer TMQFreeAssignment(unsafe.Pointer(assignment))
	result := make([]*tmq.Assignment, numOfAssignment)
	for i := 0; i < int(numOfAssignment); i++ {
		item := *(*C.tmq_topic_assignment)(unsafe.Pointer(uintptr(unsafe.Pointer(assignment)) + uintptr(C.sizeof_struct_tmq_topic_assignment*C.int(i))))
		result[i] = &tmq.Assignment{
			VGroupID: int32(item.vgId),
			Offset:   int64(item.currentOffset),
			Begin:    int64(item.begin),
			End:      int64(item.end),
		}
	}
	return 0, result
}

// TMQOffsetSeek DLL_EXPORT int32_t   tmq_offset_seek(tmq_t* tmq, const char* pTopicName, int32_t vgroupHandle, int64_t offset);
func TMQOffsetSeek(consumer unsafe.Pointer, topic string, vGroupID int32, offset int64) int32 {
	topicName := C.CString(topic)
	defer C.free(unsafe.Pointer(topicName))
	return int32(C.tmq_offset_seek((*C.tmq_t)(consumer), topicName, (C.int32_t)(vGroupID), (C.int64_t)(offset)))
}

// TMQGetVgroupOffset DLL_EXPORT int64_t     tmq_get_vgroup_offset(TAOS_RES* res, int32_t vgroupId);
func TMQGetVgroupOffset(message unsafe.Pointer) int64 {
	return int64(C.tmq_get_vgroup_offset(message))
}

// TMQFreeAssignment DLL_EXPORT void      tmq_free_assignment(tmq_topic_assignment* pAssignment);
func TMQFreeAssignment(assignment unsafe.Pointer) {
	if assignment == nil {
		return
	}
	C.tmq_free_assignment((*C.tmq_topic_assignment)(assignment))
}

// TMQPosition DLL_EXPORT int64_t     tmq_position(tmq_t *tmq, const char *pTopicName, int32_t vgId);
func TMQPosition(consumer unsafe.Pointer, topic string, vGroupID int32) int64 {
	topicName := C.CString(topic)
	defer C.free(unsafe.Pointer(topicName))
	return int64(C.tmq_position((*C.tmq_t)(consumer), topicName, (C.int32_t)(vGroupID)))
}

// TMQCommitted DLL_EXPORT int64_t     tmq_committed(tmq_t *tmq, const char *pTopicName, int32_t vgId);
func TMQCommitted(consumer unsafe.Pointer, topic string, vGroupID int32) int64 {
	topicName := C.CString(topic)
	defer C.free(unsafe.Pointer(topicName))
	return int64(C.tmq_committed((*C.tmq_t)(consumer), topicName, (C.int32_t)(vGroupID)))
}

// TMQCommitOffsetSync DLL_EXPORT int32_t   tmq_commit_offset_sync(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset);
func TMQCommitOffsetSync(consumer unsafe.Pointer, topic string, vGroupID int32, offset int64) int32 {
	topicName := C.CString(topic)
	defer C.free(unsafe.Pointer(topicName))
	return int32(C.tmq_commit_offset_sync((*C.tmq_t)(consumer), topicName, (C.int32_t)(vGroupID), (C.int64_t)(offset)))
}

// TMQCommitOffsetAsync DLL_EXPORT void      tmq_commit_offset_async(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset, tmq_commit_cb *cb, void *param);
func TMQCommitOffsetAsync(consumer unsafe.Pointer, topic string, vGroupID int32, offset int64, h cgo.Handle) {
	topicName := C.CString(topic)
	defer C.free(unsafe.Pointer(topicName))
	C.tmq_commit_offset_async((*C.tmq_t)(consumer), topicName, (C.int32_t)(vGroupID), (C.int64_t)(offset), (*C.tmq_commit_cb)(C.TMQCommitOffsetCB), h.Pointer())
}

// TMQGetConnect TAOS *tmq_get_connect(tmq_t *tmq)
func TMQGetConnect(consumer unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer(C.tmq_get_connect((*C.tmq_t)(consumer)))
}
