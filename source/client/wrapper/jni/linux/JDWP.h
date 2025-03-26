/*
 * @(#)JDWP.h	1.33 05/11/17
 *
 * Copyright 2006 Sun Microsystems, Inc. All rights reserved.
 * SUN PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

#ifndef JDWP_JDWP_H
#define JDWP_JDWP_H

#include "JDWPCommands.h"

/*                         
 * JDWPCommands.h is the javah'ed version of all the constants defined
 * com.sun.tools.jdi.JDWP and all its nested classes. Since the names are
 * very long, the macros below are provided for convenience.
 */
 
#define JDWP_COMMAND_SET(name) JDWP_ ## name 
#define JDWP_COMMAND(set, name) JDWP_ ## set ## _ ## name
#define JDWP_REQUEST_MODIFIER(name) \
           JDWP_EventRequest_Set_Out_modifiers_Modifier_ ## name
#define JDWP_EVENT(name) \
           JDWP_EventKind_ ## name 
#define JDWP_THREAD_STATUS(name) \
           JDWP_ThreadStatus_ ## name 
#define JDWP_SUSPEND_STATUS(name) \
           JDWP_SuspendStatus_SUSPEND_STATUS_ ## name 
#define JDWP_CLASS_STATUS(name) \
           JDWP_ClassStatus_ ## name 
#define JDWP_TYPE_TAG(name) \
           JDWP_TypeTag_ ## name 
#define JDWP_TAG(name) \
           JDWP_Tag_ ## name 
#define JDWP_STEP_DEPTH(name) \
           JDWP_StepDepth_ ## name 
#define JDWP_STEP_SIZE(name) \
           JDWP_StepSize_ ## name 
#define JDWP_SUSPEND_POLICY(name) \
           JDWP_SuspendPolicy_ ## name 
#define JDWP_INVOKE_OPTIONS(name) \
           JDWP_InvokeOptions_INVOKE_ ## name 
#define JDWP_ERROR(name) \
           JDWP_Error_ ## name
#define JDWP_HIGHEST_COMMAND_SET 17
#define JDWP_REQUEST_NONE        -1

/* This typedef helps keep the event and error types straight. */
typedef unsigned short jdwpError;
typedef unsigned char  jdwpEvent;
typedef jint           jdwpThreadStatus;

#endif
