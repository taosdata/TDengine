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

#define _DEFAULT_SOURCE
#include "os.h"

#include "vnode.h"

int vnodeCheckHeaderFile(int fd, int dfd, SVnodeCfg cfg, int mode) {
    SCompHeader *pHeaders = NULL;
    SVnodeCfg *pCfg = &cfg;
    SCompInfo compInfo;
    SCompBlock *pBlocks = NULL;
    int blockSize = 0;
    SField *pFields = NULL;
    char *pBuf = NULL;
    int size = 0;
    int ret = 0;

    if (fd < 0 || dfd < 0) return -1;

    lseek(fd, TSDB_FILE_HEADER_LEN, SEEK_SET);
    size = pCfg->maxSessions*sizeof(SCompHeader)+sizeof(TSCKSUM);
    pHeaders = calloc(1, size);
    if (pHeaders == NULL) {
        return -1;
    }

    read(fd, pHeaders, size);
    if (!taosCheckChecksumWhole((uint8_t *)pHeaders, size)) {
        return -1;
    }

    for (int i = 0; i < pCfg->maxSessions; i++) {
        if (pHeaders[i].compInfoOffset == 0) continue;
        if (pHeaders[i].compInfoOffset < 0) {
            // TODO : report error here
            ret = -1;
            continue;
        }
        lseek(fd, pHeaders[i].compInfoOffset, SEEK_SET);
        read(fd, &compInfo, sizeof(SCompInfo));
        if (!taosCheckChecksumWhole((uint8_t *)&compInfo, sizeof(SCompInfo))) {
            // TODO : report error
            ret = -1;
            continue;
        }

        int tsize = sizeof(SCompBlock) * compInfo.numOfBlocks + sizeof(TSCKSUM);
        if (tsize > blockSize) {
            if (pBlocks == NULL) {
                pBlocks = calloc(1, tsize);
            } else {
                pBlocks = realloc(pBlocks, tsize);
            }
            blockSize = tsize;
        }

        read(fd, tsize);
        if (!taosCheckChecksumWhole(pBlocks, tsize)) {
            // TODO: Report error
            ret = -1;
            continue;
        }

        TSKEY keyLast = 0;
        for (int j = 0; j < compInfo.numOfBlocks; j++) {
            SCompBlock *pBlock = pBlocks + j;
            if (pBlock->last != 0 && j < compInfo.numOfBlocks-1) {
                // TODO: report error
                ret = -1;
                break;
            }

            if (pBlock->offset < TSDB_FILE_HEADER_LEN) {
                // TODO : report erro
                ret = -1;
                break;
            }
            
            if (pBlock->keyLast < pBlock->keyFirst) {
                // TODO : report error
                ret = -1;
                break;
            }

            if (pBlock->keyFirst <= keyLast) {
                // TODO : report error
                ret = -1;
                break;
            }
            keyLast = pBlock->keyLast;

            // Check block in data
            lseek(dfd, pBlock->offset, SEEK_SET);
            tsize = sizeof(SField) * pBlock->numOfCols + sizeof(TSCKSUM);
            pFields = realloc(pFields, tsize);

            read(dfd, pFields, tsize);
            if (!taosCheckChecksumWhole((uint8_t*)pFields, tsize)) {
                // TODO : report error
                ret = -1;
                continue;
            }

            for (int k = 0; k < pBlock->numOfCols; k++) {
                // TODO: Check pFields[k] content

                pBuf = realloc(pBuf, pFields[k].len);

                if (!taosCheckChecksumWhole((uint8_t *)pBuf, pFields[k].len)) {
                    // TODO : report error;
                    ret = -1;
                    continue;
                }
            }
        }
    }

    tfree(pBuf);
    tfree(pFields);
    tfree(pBlocks);
    tfree(pHeaders);
    return ret;
}

int vnodePackDataFile(int vnode, int fileId) {
    // TODO: check if it is able to pack current file

    // TODO: assign value to headerFile and dataFile
    char *headerFile = NULL;
    char *dataFile = NULL;
    char *lastFile = NULL;
    SVnodeObj *pVnode = vnodeList+vnode;
    SCompHeader *pHeaders = NULL;
    SCompBlock *pBlocks = NULL;
    int blockSize = 0;
    char *pBuff = 0;
    int buffSize = 0;
    SCompInfo compInfo;
    int size = 0;

    int hfd = open(headerFile, O_RDONLY);
    if (hfd < 0) {
        dError("vid: %d, failed to open header file:%s\n", vnode, headerFile);
        return -1;
    }
    int dfd = open(dataFile, O_RDONLY);
    if (dfd < 0) {
        dError("vid: %d, failed to open data file:%s\n", vnode, dataFile);
        return -1;
    }
    int lfd = open(lastFile, O_RDONLY);
    if (lfd < 0) {
        dError("vid: %d, failed to open data file:%s\n", vnode, lastFile);
        return -1;
    }

    lseek(hfd, TSDB_FILE_HEADER_LEN, SEEK_SET);
    size = sizeof(SCompHeader)*pVnode->cfg.maxSessions+sizeof(TSCKSUM);
    pHeaders = malloc(size);
    if (pHeaders == NULL) goto _exit_failure;
    read(hfd, pHeaders, size);
    if (!taosCheckChecksumWhole((uint8_t *)pHeaders, size)) {
        dError("vid: %d, header file %s is broken", vnode, headerFile);
        goto _exit_failure;
    }

    for (size_t i = 0; i < pVnode->cfg.maxSessions; i++)
    {
        if (pHeaders[i].compInfoOffset <= 0) continue;
        SMeterObj *pObj = (SMeterObj *)pVnode->meterList[i];
        // read compInfo part
        lseek(hfd, pHeaders[i].compInfoOffset, SEEK_SET);
        read(hfd, &compInfo, sizeof(SCompInfo));
        if (!taosCheckChecksumWhole((uint8_t *)&compInfo, sizeof(SCompInfo))) {
            dError("vid: %d sid:%d fileId:%d compInfo is broken", vnode, i, fileId);
            goto _exit_failure;
        }

        // read compBlock part
        int tsize = compInfo.numOfBlocks * sizeof(SCompBlock) + sizeof(TSCKSUM);
        if (tsize >  blockSize) {
            if (blockSize == 0) {
                pBlocks = malloc(tsize);
            } else {
                pBlocks = realloc(pBlocks, tsize);
            }
            blockSize = tsize;
        }
        read(hfd, pBlocks, tsize);
        if (!taosCheckChecksumWhole((uint8_t *)pBlocks, tsize)) {
            dError("vid:%d sid:%d fileId:%d block part is broken", vnode, i, fileId);
            goto _exit_failure;
        }

        assert(compInfo.numOfBlocks > 0);
        // Loop to scan the blocks and merge block when neccessary.
        tsize = sizeof(SCompInfo) + compInfo.numOfBlocks *sizeof(SCompBlock) + sizeof(TSCKSUM);
        pBuff = realloc(pBuff, tsize);
        SCompInfo *pInfo = (SCompInfo *)pBuff;
        SCompBlock *pNBlocks = pBuff + sizeof(SCompInfo);
        int nCounter = 0;
        for (int j; j < compInfo.numOfBlocks; j++) {
            // TODO : Check if it is the last block
            // if (j == compInfo.numOfBlocks - 1) {}
            if (pBlocks[j].numOfPoints + pNBlocks[nCounter].numOfPoints <= pObj->pointsPerFileBlock) {
                // Merge current block to current new block
            } else {
                // Write new block to new data file
                // pNBlocks[nCounter].
                nCounter++;
            }
        }
    }

    return 0;

_exit_failure:
    tfree(pHeaders);
    if (hfd > 0) close(hfd);
    if (dfd > 0) close(dfd);
    if (lfd > 0) close(lfd);
    return -1;
}