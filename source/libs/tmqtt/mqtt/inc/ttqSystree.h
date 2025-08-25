/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef SYS_TREE_H
#define SYS_TREE_H

#if defined(WITH_SYS_TREE) && defined(WITH_BROKER)
extern uint64_t      g_bytes_received;
extern uint64_t      g_bytes_sent;
extern uint64_t      g_pub_bytes_received;
extern uint64_t      g_pub_bytes_sent;
extern unsigned long g_msgs_received;
extern unsigned long g_msgs_sent;
extern unsigned long g_pub_msgs_received;
extern unsigned long g_pub_msgs_sent;
extern unsigned long g_msgs_dropped;
extern int           g_clients_expired;
extern unsigned int  g_socket_connections;
extern unsigned int  g_connection_count;

#define G_BYTES_RECEIVED_INC(A)     (g_bytes_received += (uint64_t)(A))
#define G_BYTES_SENT_INC(A)         (g_bytes_sent += (uint64_t)(A))
#define G_PUB_BYTES_RECEIVED_INC(A) (g_pub_bytes_received += (A))
#define G_PUB_BYTES_SENT_INC(A)     (g_pub_bytes_sent += (A))
#define G_MSGS_RECEIVED_INC(A)      (g_msgs_received += (A))
#define G_MSGS_SENT_INC(A)          (g_msgs_sent += (A))
#define G_PUB_MSGS_RECEIVED_INC(A)  (g_pub_msgs_received += (A))
#define G_PUB_MSGS_SENT_INC(A)      (g_pub_msgs_sent += (A))
#define G_MSGS_DROPPED_INC()        (g_msgs_dropped++)
#define G_CLIENTS_EXPIRED_INC()     (g_clients_expired++)
#define G_SOCKET_CONNECTIONS_INC()  (g_socket_connections++)
#define G_CONNECTION_COUNT_INC()    (g_connection_count++)

#else

#define G_BYTES_RECEIVED_INC(A)
#define G_BYTES_SENT_INC(A)
#define G_PUB_BYTES_RECEIVED_INC(A)
#define G_PUB_BYTES_SENT_INC(A)
#define G_MSGS_RECEIVED_INC(A)
#define G_MSGS_SENT_INC(A)
#define G_PUB_MSGS_RECEIVED_INC(A)
#define G_PUB_MSGS_SENT_INC(A)
#define G_MSGS_DROPPED_INC()
#define G_CLIENTS_EXPIRED_INC()
#define G_SOCKET_CONNECTIONS_INC()
#define G_CONNECTION_COUNT_INC()

#endif

#endif
