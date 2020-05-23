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
 *******************************************************************************/

#if !defined(MQTTPERSISTENCEDEFAULT_H)
#define MQTTPERSISTENCEDEFAULT_H

/** 8.3 filesystem */
#define MESSAGE_FILENAME_LENGTH 8    
/** Extension of the filename */
#define MESSAGE_FILENAME_EXTENSION ".msg"

/* prototypes of the functions for the default file system persistence */
int pstopen(void** handle, const char* clientID, const char* serverURI, void* context); 
int pstclose(void* handle); 
int pstput(void* handle, char* key, int bufcount, char* buffers[], int buflens[]); 
int pstget(void* handle, char* key, char** buffer, int* buflen); 
int pstremove(void* handle, char* key); 
int pstkeys(void* handle, char*** keys, int* nkeys); 
int pstclear(void* handle); 
int pstcontainskey(void* handle, char* key);

int pstmkdir(char *pPathname);

#endif

