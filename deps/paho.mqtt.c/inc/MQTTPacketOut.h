/*******************************************************************************
 * Copyright (c) 2009, 2018 IBM Corp.
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
 *    Ian Craggs, Allan Stockdill-Mander - SSL updates
 *    Ian Craggs - MQTT 3.1.1 support
 *    Ian Craggs - MQTT 5.0 support
 *******************************************************************************/

#if !defined(MQTTPACKETOUT_H)
#define MQTTPACKETOUT_H

#include "MQTTPacket.h"

int MQTTPacket_send_connect(Clients* client, int MQTTVersion,
		MQTTProperties* connectProperties, MQTTProperties* willProperties);
void* MQTTPacket_connack(int MQTTVersion, unsigned char aHeader, char* data, size_t datalen);
void MQTTPacket_freeConnack(Connack* pack);

int MQTTPacket_send_pingreq(networkHandles* net, const char* clientID);

int MQTTPacket_send_subscribe(List* topics, List* qoss, MQTTSubscribe_options* opts, MQTTProperties* props,
		int msgid, int dup, Clients* client);
void* MQTTPacket_suback(int MQTTVersion, unsigned char aHeader, char* data, size_t datalen);

int MQTTPacket_send_unsubscribe(List* topics, MQTTProperties* props, int msgid, int dup, Clients* client);
void* MQTTPacket_unsuback(int MQTTVersion, unsigned char aHeader, char* data, size_t datalen);

#endif
