/*******************************************************************************
 * Copyright (c) 2009, 2014 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution. 
 *
 * The Eclipse Public License is available at 
 *    https://www.eclipse.org/legal/epl-2.0/
 * and the Eclipse Distribution License is available at 
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Ian Craggs - initial API and implementation and/or initial documentation
 *    Ian Craggs - MQTT 3.1.1 updates
 *******************************************************************************/

#if !defined(MQTTPROTOCOL_H)
#define MQTTPROTOCOL_H

#include "LinkedList.h"
#include "MQTTPacket.h"
#include "Clients.h"

#define MAX_MSG_ID 65535
#define MAX_CLIENTID_LEN 65535

typedef struct
{
	int socket;
	Publications* p;
} pending_write;


typedef struct
{
	List publications;
	unsigned int msgs_received;
	unsigned int msgs_sent;
	List pending_writes; /* for qos 0 writes not complete */
} MQTTProtocol;


#include "MQTTProtocolOut.h"

#endif
