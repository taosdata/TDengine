/*******************************************************************************
 * Copyright (c) 2018 IBM Corp.
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
 *******************************************************************************/

#if !defined(SUBOPTS_H)
#define SUBOPTS_H

/** The MQTT V5 subscribe options, apart from QoS which existed before V5. */
typedef struct MQTTSubscribe_options
{
	/** The eyecatcher for this structure. Must be MQSO. */
	char struct_id[4];
	/** The version number of this structure.  Must be 0.
	 */
	int struct_version;
	/** To not receive our own publications, set to 1.
	 *  0 is the original MQTT behaviour - all messages matching the subscription are received.
	 */
	unsigned char noLocal;
	/** To keep the retain flag as on the original publish message, set to 1.
	 *  If 0, defaults to the original MQTT behaviour where the retain flag is only set on
	 *  publications sent by a broker if in response to a subscribe request.
	 */
	unsigned char retainAsPublished;
	/** 0 - send retained messages at the time of the subscribe (original MQTT behaviour)
	 *  1 - send retained messages on subscribe only if the subscription is new
	 *  2 - do not send retained messages at all
	 */
	unsigned char retainHandling;
} MQTTSubscribe_options;

#define MQTTSubscribe_options_initializer { {'M', 'Q', 'S', 'O'}, 0, 0, 0, 0 }

#endif
