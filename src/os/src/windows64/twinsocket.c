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

#include <WS2tcpip.h>  
#include <IPHlpApi.h>  
#include <winsock2.h>  
#include <stdio.h>
#include <string.h>
#include <ws2def.h>
#include <tchar.h>

void taosWinSocketInit() {
    static char flag = 0;
    if (flag == 0) {
        WORD wVersionRequested;
        WSADATA wsaData;
        wVersionRequested = MAKEWORD(1, 1);
        if (WSAStartup(wVersionRequested, &wsaData) == 0) {
            flag = 1;
        }
    }
}

int taosSetNonblocking(SOCKET sock, int on) {
    u_long mode;
    if (on) {
        mode = 1;
        ioctlsocket(sock, FIONBIO, &mode);
    }
    else {
        mode = 0;
        ioctlsocket(sock, FIONBIO, &mode);
    }
    return 0;
}

int taosGetPrivateIp(char *const ip) {
    PIP_ADAPTER_ADDRESSES pAddresses = 0;
    IP_ADAPTER_DNS_SERVER_ADDRESS *pDnServer = 0;
    ULONG outBufLen = 0;
    DWORD dwRetVal = 0;
    char buff[100];
    DWORD bufflen = 100;
    int i;
    int flag = -1;

    taosWinSocketInit();
    GetAdaptersAddresses(AF_UNSPEC, 0, NULL, pAddresses, &outBufLen);
    pAddresses = (IP_ADAPTER_ADDRESSES *)malloc(outBufLen);
    if ((dwRetVal = GetAdaptersAddresses(AF_INET, GAA_FLAG_SKIP_ANYCAST, NULL, pAddresses, &outBufLen)) == NO_ERROR) {
        while (pAddresses) {
            if (wcsstr(pAddresses->FriendlyName, L"Loopback") != 0) {
                pAddresses = pAddresses->Next;
                continue;
            }
            if (pAddresses->OperStatus == IfOperStatusUp) {
                //printf("%s, Status: active\n", pAddresses->FriendlyName);
            }
            else {
                //printf("%s, Status: deactive\n", pAddresses->FriendlyName);
                pAddresses = pAddresses->Next;
                continue;
            }

            PIP_ADAPTER_UNICAST_ADDRESS pUnicast = pAddresses->FirstUnicastAddress;
            for (i = 0; pUnicast != NULL; i++) {
                if (pUnicast->Address.lpSockaddr->sa_family == AF_INET) {
                    struct sockaddr_in *sa_in = (struct sockaddr_in *)pUnicast->Address.lpSockaddr;
                    strcpy(ip, inet_ntop(AF_INET, &(sa_in->sin_addr), buff, bufflen));
                    flag = 0;
                    //printf("%s\n", ip);
                }
                else if (pUnicast->Address.lpSockaddr->sa_family == AF_INET6) {
                    struct sockaddr_in6 *sa_in6 = (struct sockaddr_in6 *)pUnicast->Address.lpSockaddr;
                    strcpy(ip, inet_ntop(AF_INET6, &(sa_in6->sin6_addr), buff, bufflen));
                    flag = 0;
                    //printf("%s\n", ip);
                }
                else {
                }
                pUnicast = pUnicast->Next;
            }
            pAddresses = pAddresses->Next;
        }
    }
    else {
        LPVOID lpMsgBuf;
        printf("Call to GetAdaptersAddresses failed.\n");
        if (FormatMessage(
          FORMAT_MESSAGE_ALLOCATE_BUFFER |
          FORMAT_MESSAGE_FROM_SYSTEM |
          FORMAT_MESSAGE_IGNORE_INSERTS,
          NULL,
          dwRetVal,
          MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
          (LPTSTR) & lpMsgBuf,
          0,
          NULL)) {
            printf("\tError: %s", lpMsgBuf);
        }
        LocalFree(lpMsgBuf);
    }
    free(pAddresses);
    return flag;
}
