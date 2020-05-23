/*******************************************************************************
 * Copyright (c) 2020, 2020 Andreas Walter
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
 *    Andreas Walter - initially moved export declarations into separate fle
 *******************************************************************************/

#if !defined(EXPORTDECLARATIONS_H)
#define EXPORTDECLARATIONS_H

#if defined(_WIN32) || defined(_WIN64)
#   if defined(PAHO_MQTT_EXPORTS)
#       define LIBMQTT_API __declspec(dllexport)
#   elif defined(PAHO_MQTT_IMPORTS)
#       define LIBMQTT_API __declspec(dllimport)
#   else
#       define LIBMQTT_API
#   endif
#else
#    if defined(PAHO_MQTT_EXPORTS)
#       define LIBMQTT_API  __attribute__ ((visibility ("default")))
#    else
#       define LIBMQTT_API extern
#    endif
#endif

#endif
