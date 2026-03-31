using System;
using System.Runtime.InteropServices;

namespace TDengine.Driver.Impl.NativeMethods
{
    public static partial class NativeMethods
    {
        /* --------------------------TMQ INTERFACE------------------------------- */

        [DllImport(DLLName, EntryPoint = "tmq_err2str", CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr TmqErr2Str(int errorCode);
        //const char *tmq_err2str(int32_t code);

        /* --------------------------TMQ Config---------------------------------- */
        [DllImport(DLLName, EntryPoint = "tmq_conf_new", CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr TmqConfNew();
        //tmq_conf_t    *tmq_conf_new();

        [DllImport(DLLName, EntryPoint = "tmq_conf_set", CallingConvention = CallingConvention.Cdecl)]
        public static extern int TmqConfSet(IntPtr conf, string key, string value);
        //tmq_conf_res_t tmq_conf_set(tmq_conf_t* conf, const char* key, const char* value);

        [DllImport(DLLName, EntryPoint = "tmq_conf_destroy", CallingConvention = CallingConvention.Cdecl)]
        public static extern void TmqConfDestroy(IntPtr conf);
        //void tmq_conf_destroy(tmq_conf_t* conf);

        [DllImport(DLLName, EntryPoint = "tmq_conf_set_auto_commit_cb", CallingConvention = CallingConvention.Cdecl)]
        public static extern void TmqConfSetAutoCommitCB(IntPtr conf, TmqCommitCallback callback, IntPtr param);
        //void tmq_conf_set_auto_commit_cb(tmq_conf_t* conf, tmq_commit_cb* cb, void* param);

        /* --------------------------TMQ Consumer-------------------------------- */
        [DllImport(DLLName, EntryPoint = "tmq_consumer_new", CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr TmqConsumerNew(IntPtr tmqConf, IntPtr errStr, int errStrLength);
        //tmq_t *tmq_consumer_new(tmq_conf_t* conf, char* errstr, int32_t errstrLen);

        [DllImport(DLLName, EntryPoint = "tmq_subscribe", CallingConvention = CallingConvention.Cdecl)]
        public static extern int TmqSubscribe(IntPtr tmq, IntPtr topicList);
        //int32_t   tmq_subscribe(tmq_t *tmq, const tmq_list_t *topic_list);

        [DllImport(DLLName, EntryPoint = "tmq_unsubscribe", CallingConvention = CallingConvention.Cdecl)]
        public static extern int TmqUnsubscribe(IntPtr tmq);
        //int32_t   tmq_unsubscribe(tmq_t *tmq);

        [DllImport(DLLName, EntryPoint = "tmq_subscription", CallingConvention = CallingConvention.Cdecl)]
        public static extern int TmqSubscription(IntPtr tmq, IntPtr topicListPtr);
        //int32_t   tmq_subscription(tmq_t *tmq, tmq_list_t **topics);

        [DllImport(DLLName, EntryPoint = "tmq_consumer_poll", CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr TmqConsumerPoll(IntPtr tmq, Int64 timeout);
        //TAOS_RES *tmq_consumer_poll(tmq_t *tmq, int64_t timeout);

        [DllImport(DLLName, EntryPoint = "tmq_consumer_close", CallingConvention = CallingConvention.Cdecl)]
        public static extern int TmqConsumerClose(IntPtr tmq);
        //int32_t   tmq_consumer_close(tmq_t *tmq);

        [DllImport(DLLName, EntryPoint = "tmq_commit_sync", CallingConvention = CallingConvention.Cdecl)]
        public static extern int TmqCommitSync(IntPtr tmq, IntPtr msg);
        //int32_t   tmq_commit_sync(tmq_t *tmq, const TAOS_RES *msg);

        [DllImport(DLLName, EntryPoint = "tmq_commit_async", CallingConvention = CallingConvention.Cdecl)]
        public static extern void TmqCommitAsync(IntPtr tmq, IntPtr msg, TmqCommitCallback callback, IntPtr param);
        //void tmq_commit_async(tmq_t* tmq, const TAOS_RES* msg, tmq_commit_cb *cb, void* param);

        /* --------------------------TMQ Message-------------------------------- */

        [DllImport(DLLName, EntryPoint = "tmq_get_res_type", CallingConvention = CallingConvention.Cdecl)]
        public static extern int TmqGetResType(IntPtr taosRes);
        //tmq_res_t tmq_get_res_type(TAOS_RES* res);

        [DllImport(DLLName, EntryPoint = "tmq_get_raw", CallingConvention = CallingConvention.Cdecl)]
        public static extern int TmqGetRaw(IntPtr taosRes, IntPtr raw);
        //int32_t     tmq_get_raw(TAOS_RES *res, tmq_raw_data *raw);

        [DllImport(DLLName, EntryPoint = "tmq_get_topic_name", CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr TmqGetTopicName(IntPtr taosRes);
        //const char* tmq_get_topic_name(TAOS_RES * res);

        [DllImport(DLLName, EntryPoint = "tmq_get_db_name", CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr TmqGetDbName(IntPtr taosRes);
        //const char* tmq_get_db_name(TAOS_RES * res);

        [DllImport(DLLName, EntryPoint = "tmq_get_vgroup_id", CallingConvention = CallingConvention.Cdecl)]
        public static extern int TmqGetVgroupId(IntPtr taosRes);
        //int32_t tmq_get_vgroup_id(TAOS_RES* res);

        [DllImport(DLLName, EntryPoint = "tmq_get_table_name", CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr TmqGetTableName(IntPtr taosRes);
        //const char* tmq_get_table_name(TAOS_RES * res);

        /* --------------------------TMQ Topic List-------------------------------- */
        [DllImport(DLLName, EntryPoint = "tmq_list_new", CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr TmqListNew();
        // tmq_list_t *tmq_list_new();

        [DllImport(DLLName, EntryPoint = "tmq_list_append", CallingConvention = CallingConvention.Cdecl)]
        public static extern int TmqListAppend(IntPtr tmqList, string str);
        // int32_t tmq_list_append(tmq_list_t *, const char *);


        [DllImport(DLLName, EntryPoint = "tmq_list_destroy", CallingConvention = CallingConvention.Cdecl)]
        public static extern void TmqListDestroy(IntPtr tmqList);
        // void tmq_list_destroy(tmq_list_t *);

        [DllImport(DLLName, EntryPoint = "tmq_list_get_size", CallingConvention = CallingConvention.Cdecl)]
        public static extern int TmqListGetSize(IntPtr tmqList);
        // int32_t tmq_list_get_size(const tmq_list_t *);

        [DllImport(DLLName, EntryPoint = "tmq_list_to_c_array", CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr TmqListToCArray(IntPtr tmqList);
        // char **tmq_list_to_c_array(const tmq_list_t *);

        [DllImport(DLLName, EntryPoint = "tmq_get_topic_assignment", CallingConvention = CallingConvention.Cdecl)]
        private static extern int TmqGetTopicAssignment(IntPtr tmq, string pTopicName, out IntPtr assignment,
            out int numOfAssignment);

        [DllImport(DLLName, EntryPoint = "tmq_free_assignment", CallingConvention = CallingConvention.Cdecl)]
        public static extern void TmqFreeAssignment(IntPtr assignment);

        public static TMQTopicAssignment[] GetTopicAssignments(IntPtr tmqPtr, string topicName)
        {
            IntPtr assignmentPtr = IntPtr.Zero;
            int numOfAssignment;
            try
            {
                int result = TmqGetTopicAssignment(tmqPtr, topicName, out assignmentPtr, out numOfAssignment);

                if (result == 0)
                {
                    var assignments = new TMQTopicAssignment[numOfAssignment];
                    int structSize = Marshal.SizeOf(typeof(TMQTopicAssignment));

                    for (int i = 0; i < numOfAssignment; i++)
                    {
                        IntPtr currentAssignmentPtr = new IntPtr(assignmentPtr.ToInt64() + i * structSize);
                        assignments[i] =
                            (TMQTopicAssignment)Marshal.PtrToStructure(currentAssignmentPtr,
                                typeof(TMQTopicAssignment));
                    }

                    return assignments;
                }
                else
                {
                    throw new Exception(
                        $"Error calling tmq_get_topic_assignment code:{result},msg:{TmqErr2Str(result)}");
                }
            }
            finally
            {
                if (assignmentPtr != IntPtr.Zero)
                {
                    TmqFreeAssignment(assignmentPtr);
                }
            }
        }

        [DllImport(DLLName, EntryPoint = "tmq_committed", CallingConvention = CallingConvention.Cdecl)]
        public static extern long TmqCommitted(IntPtr tmq, string pTopicName, int vgId);

        [DllImport(DLLName, EntryPoint = "tmq_commit_offset_sync", CallingConvention = CallingConvention.Cdecl)]
        public static extern int TmqCommitOffsetSync(IntPtr tmq, string pTopicName, int vgId, long offset);

        [DllImport(DLLName, EntryPoint = "tmq_offset_seek", CallingConvention = CallingConvention.Cdecl)]
        public static extern int TmqOffsetSeek(IntPtr tmq, string pTopicName, int vgId, long offset);

        [DllImport(DLLName, EntryPoint = "tmq_get_vgroup_offset", CallingConvention = CallingConvention.Cdecl)]
        public static extern long TmqGetVgroupOffset(IntPtr res);

        [DllImport(DLLName, EntryPoint = "tmq_position", CallingConvention = CallingConvention.Cdecl)]
        public static extern long TmqPosition(IntPtr tmq, string pTopicName, int vgId);
    }
}