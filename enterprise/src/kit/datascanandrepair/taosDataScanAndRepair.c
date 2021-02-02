#include <argp.h>
#include <assert.h>
#include <dirent.h>
#include <errno.h>

#ifndef _ALPINE
#include <error.h>
#endif

#include <fcntl.h>
#include <libgen.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "tchecksum.h"
#include "taosdef.h"
#include "tutil.h"
#include "vnode.h"
#include "vnodeFile.h"

// #define KEEP_ALL

#ifdef KEEP_ALL
#define REMOVE(f)
#else
#define REMOVE(f) remove(f)
#endif

#define REPORT_DIR "report"
#define REPAIR_DIR "repair"

typedef struct {
  int64_t     lastCreate;
  int64_t     lastRemove;
  uint64_t    version;
  int64_t     lastKeyOnFile;
  int         fileId;
  int         numOfFiles;
  SVnodeCfg   cfg;
  SMeterObj **pTable;
} SVnodeInfo;

struct arguments {
  char dataDir[TSDB_FILENAME_LEN];
  int  reportOnly;
  int  inPlace;
  int  keepAll;
  int  abort;
};

struct arguments gArg;

const char *argp_program_version = version;
const char *argp_program_bug_address = "<support@taosdata.com>";
static char doc[] = "";
static char args_doc[] = "[dataDir]";

#define OPT_ABORT 1 /* –abort */

static struct argp_option options[] = {{"report-only", 'R', 0, 0, "Only report the scan results."},
                                       {"in-place", 'I', 0, 0, "Modify in place"},
                                       {"keep-all", 'K', 0, 0, "Keep all reports. Default only keep bad files"},
                                       {0}};

static error_t parse_opt(int key, char *arg, struct argp_state *state) {
  /* Get the input argument from argp_parse, which we
     know is a pointer to our arguments structure. */
  struct arguments *tArg = state->input;

  switch (key) {
    // connection option
    case 'R':
      tArg->reportOnly = 1;
      break;
    case 'I':
      tArg->inPlace = 1;
      break;
    case 'K':
      tArg->keepAll = 1;
      break;
    case OPT_ABORT:
      tArg->abort = 1;
      break;
    case ARGP_KEY_ARG:
      if (state->argc - state->next + 1 != 1) {
        fprintf(stderr, "at most 1 parameter is allowed\n");
        tArg->abort = 1;
      } else {
        strcpy(tArg->dataDir, state->argv[state->next - 1]);
      }
      break;

    default:
      return ARGP_ERR_UNKNOWN;
  }
  return 0;
}

/* Our argp parser. */
static struct argp argp = {options, parse_opt, args_doc, doc};

void        printVnodeCfg(FILE *fp, SVnodeCfg *pCfg);
void        printHeader(SCompHeader *pHeader, int maxSessions);
int         printCompInfo(SBlockInfo *pCompInfo, uint64_t uid, FILE *fp);
void        printCompBlock(SBlock *pBlock, FILE *fp);
void        checkFile(char *headFile, SVnodeInfo *pInfo, char *vnodeDirName, char *headName, int *err);
SVnodeInfo *loadVnodeInfo(char *meterObjFile, char *vnodeDir);
int         createDir(const char *dirName);
ssize_t     taosSendFile(int dfd, int sfd, off_t *offset, size_t size);
ssize_t     taosWrite(int fd, void *buf, size_t n);
void        scanDir(const char *dbDir, SVnodeInfo *pInfo);
void        vnodeCreateFileHeaderFd(int fd);

int checkParams() {
  if (gArg.reportOnly && gArg.inPlace) {
    fprintf(stderr, "read-only option and modify in-place option cannot coexists\n");
    return -1;
  }
  return 0;
}

// #define REPARE_FILE
int main(int argc, char **argv) {
  taosResolveCRC();
  char tsdbDir[133] = "\0";
  char dbDir[400] = "\0";
  char meterObjFile[408] = "\0";
  char headFile[658] = "\0";

  gArg = (struct arguments){"/var/lib/taos", 0, 0, 0, 0};

  argp_parse(&argp, argc, argv, 0, 0, &gArg);

  if (gArg.abort) error(10, 0, "ABORTED");
  if (checkParams() < 0) return -1;

  sprintf(tsdbDir, "%s/tsdb", gArg.dataDir);

  DIR *          dir1 = opendir(tsdbDir);
  struct dirent *dent1 = NULL;
  if (dir1) {
    if (createDir(REPORT_DIR) < 0) exit(EXIT_FAILURE);
    if (!(gArg.reportOnly)) {
      if (createDir(REPAIR_DIR) < 0) exit(EXIT_FAILURE);
    }

    while ((dent1 = readdir(dir1)) != NULL) {  // Loop over tsdb, for vnodexx directories
      if (strcmp(dent1->d_name, ".") == 0 || strcmp(dent1->d_name, "..") == 0) continue;
      if (strncmp(dent1->d_name, "vnode", 5) != 0) continue;
      int vnodeHasError = 0;

      // Start to processing vnode
      printf("Processing directory: %s/%s...\n", tsdbDir, dent1->d_name);
      {  // Create report and repair vnode directory
        char tmpDirName[FILENAME_MAX] = "\0";
        sprintf(tmpDirName, "%s/%s", REPORT_DIR, dent1->d_name);
        if (createDir(tmpDirName) < 0) exit(EXIT_FAILURE);
        if (!(gArg.reportOnly)) {
          sprintf(tmpDirName, "%s/%s", REPAIR_DIR, dent1->d_name);
          if (createDir(tmpDirName) < 0) exit(EXIT_FAILURE);
        }
      }

      int vnode = 0;
      sscanf(dent1->d_name, "vnode%d", &vnode);
      sprintf(meterObjFile, "%s/%s/meterObj.v%d", tsdbDir, dent1->d_name, vnode);

      SVnodeInfo *pInfo = loadVnodeInfo(meterObjFile, dent1->d_name);
      if (pInfo == NULL) {
        vnodeHasError = 1;
        continue;
      }

      sprintf(dbDir, "%s/%s/db", tsdbDir, dent1->d_name);
      scanDir(dbDir, pInfo);

      DIR *          dir2 = opendir(dbDir);
      struct dirent *dent2 = NULL;
      if (dir2) {
        while ((dent2 = readdir(dir2)) != NULL) {  // Loop over head files
          if (strcmp(dent2->d_name, ".") == 0 || strcmp(dent2->d_name, "..") == 0) continue;
          if (strcmp(dent2->d_name + strlen(dent2->d_name) - 5, ".head") != 0) continue;

          memset((void *)headFile, 0, 128);
          sprintf(headFile, "%s/%s", dbDir, dent2->d_name);

          // printf("Processing file: %s...\n", headFile);
          // TODO : modify vnodeHas ERROR part
          int err = 0;
          checkFile(headFile, pInfo, dent1->d_name, dent2->d_name, &err);
          if (err != 0) vnodeHasError = 1;
        }

        if (vnodeHasError == 0) {
          char tmpfname[1024] = "\0";
          if (!(gArg.keepAll)) {
            sprintf(tmpfname, "%s/%s/%s.info", REPORT_DIR, dent1->d_name, dent1->d_name);
            REMOVE(tmpfname);
            sprintf(tmpfname, "%s/%s", REPORT_DIR, dent1->d_name);
            rmdir(tmpfname);
          }
          if (!(gArg.reportOnly)) {
            sprintf(tmpfname, "%s/%s", REPAIR_DIR, dent1->d_name);
            rmdir(tmpfname);
          }
        }
      } else {
        fprintf(stderr, "failed to open directory:%s\n", tsdbDir);
        continue;
      }
      closedir(dir2);

      for (int i = 0; i < pInfo->cfg.maxSessions; i++)
        if (pInfo->pTable[i] != NULL) free(pInfo->pTable[i]);
      free(pInfo);
    }
  } else {
    fprintf(stderr, "failed to open directory:%s\n", tsdbDir);
    exit(EXIT_FAILURE);
  }

  closedir(dir1);

  return 0;
}

SVnodeInfo *loadVnodeInfo(char *meterObjFile, char *vnodeDir) {  // Open meterObj file and read from it
  int       size = 0;
  SMeterObjHeader *meterIndex = NULL;
  FILE *    fp = NULL;
  char *    buff = NULL;
  char      reportInfoFname[128] = "\0";

  FILE *    reportFP = NULL;

  SVnodeInfo *pInfo = (SVnodeInfo *)calloc(1, sizeof(SVnodeInfo));
  if (pInfo == NULL) {
    fprintf(stderr, "ERROR! failed to allocate memory , size:%zu\n", sizeof(SVnodeInfo));
    return NULL;
  }

  // Open meterObj file
  fp = fopen(meterObjFile, "r");
  if (fp == NULL) {
    fprintf(stderr, "ERROR! failed to open file %s\n", meterObjFile);
    goto _error_meterObj;
  }
  sprintf(reportInfoFname, "%s/%s/%s.info", REPORT_DIR, vnodeDir, vnodeDir);
  reportFP = fopen(reportInfoFname, "w");
  if (reportFP == NULL) {
    fprintf(stderr, "ERROR! failed to open file %s\n", reportInfoFname);
    goto _error_meterObj;
  }

  fseek(fp, TSDB_FILE_HEADER_LEN * 1 / 4, SEEK_SET);
  fscanf(fp, "%" PRId64 " %" PRId64 " %" PRId64, &(pInfo->lastCreate), &(pInfo->lastRemove), &(pInfo->version));
  fscanf(fp, "%" PRId64 " %d %d ", &(pInfo->lastKeyOnFile), &(pInfo->fileId), &(pInfo->numOfFiles));
#endif

  fseek(fp, TSDB_FILE_HEADER_LEN * 2 / 4, SEEK_SET);
  fread(&(pInfo->cfg), sizeof(SVnodeCfg), 1, fp);

  printVnodeCfg(reportFP, &(pInfo->cfg));

  // Read meterIndex
  fseek(fp, TSDB_FILE_HEADER_LEN, SEEK_SET);
  size = sizeof(SMeterObjHeader) * pInfo->cfg.maxSessions + sizeof(TSCKSUM);
  meterIndex = malloc(size);
  if (meterIndex == NULL) {
    goto _error_meterObj;
  }
  memset(meterIndex, 0, size);
  fread(meterIndex, size, 1, fp);

  // Read SMeterObj
  size = sizeof(SMeterObj) + 256 * sizeof(SColumn) + 256 * 16 + sizeof(TSCKSUM);
  buff = malloc(size);

  pInfo->pTable = (SMeterObj **)calloc(pInfo->cfg.maxSessions, sizeof(SMeterObj *));
  if (pInfo->pTable == NULL) {
    goto _error_meterObj;
  }

  for (int sid = 0; sid < pInfo->cfg.maxSessions; sid++) {
    int64_t offset = meterIndex[sid].offset;
    int64_t length = meterIndex[sid].length;
    if (offset <= 0 || length <= 0) continue;

    fseek(fp, offset, SEEK_SET);
    fread(buff, length, 1, fp);
    if (taosCheckChecksumWhole((uint8_t *)buff, length)) {
      SMeterObj *pSaveObj = (SMeterObj *)buff;

      if (pSaveObj->vnode < 0 || pSaveObj->vnode >= 256) {
        fprintf(reportFP, "ERROR vnode number: %d\n", pSaveObj->vnode);
        continue;
      }

      if (pSaveObj->tableId[0] == 0) continue;
      pInfo->pTable[sid] = (SMeterObj *)malloc(sizeof(SMeterObj) + pSaveObj->sqlLen + 1);
      memcpy(pInfo->pTable[sid], pSaveObj, offsetof(SMeterObj, reserved));
      fprintf(reportFP, "sid: %d, uid: %" PRIu64 ", numOfColumns:%d tableId:%s\n", sid, pInfo->pTable[sid]->uid,
              pInfo->pTable[sid]->numOfColumns, pInfo->pTable[sid]->tableId);

    } else {
      fprintf(reportFP, "ERROR in meterobj file record, sid:%d\n", sid);
    }
  }

  if (meterIndex != NULL) free(meterIndex);
  if (buff != NULL) free(buff);

  if (fp != NULL) fclose(fp);
  if (reportFP != NULL) fclose(reportFP);

  return pInfo;

_error_meterObj:
  if (meterIndex != NULL) free(meterIndex);
  if (buff != NULL) free(buff);
  free(pInfo);
  if (fp != NULL) fclose(fp);
  if (reportFP != NULL) fclose(reportFP);
  return NULL;
}

void checkFile(char *headFile, SVnodeInfo *pInfo, char *vnodeDirName, char *headName, int *err) {
  int          drift = 0;
  int          size = 0;
  SCompHeader *pHeader = NULL;
  SVnodeCfg *  pCfg = &(pInfo->cfg);
  SBlockInfo    compInfo;
  char         reportFname[128] = "\0";
  FILE *       reportFP = NULL;
  int          fileHasError = 0;
  SBlock * pBlocks = NULL;
  SBlock * pRepairBlocks = NULL;
  SField      *pFields = NULL;
  size_t       fields_size = 0;
  char        *pCol = NULL;
  size_t       col_size = 0;
  char         dataFile[128] = "\0";
  char         lastFile[128] = "\0";
  struct stat  tstat;
  size_t       hfsize = 0, dfsize = 0, lfsize = 0;

  // REPAIR
  int  repairFd = -1;
  char repairFname[128] = "\0";
  sprintf(repairFname, "%s/%s/%s", REPAIR_DIR, vnodeDirName, headName);

  sprintf(reportFname, "%s/%s/%s", REPORT_DIR, vnodeDirName, headName);

  size_t tlen = strlen(headFile);
  strcpy(dataFile, headFile);
  sprintf(dataFile + tlen - strlen("head"), "%s", "data");
  strcpy(lastFile, headFile);
  sprintf(lastFile + tlen - strlen("head"), "%s", "last");

  int fd = open(headFile, O_RDONLY);
  if (fd < 0) {
    fprintf(stderr, "failed to open file %s, reason:%s\n", headFile, strerror(errno));
    return;
  } else {
    fstat(fd, &tstat);
    hfsize = tstat.st_size;
  }

  int dfd = open(dataFile, O_RDONLY);
  if (dfd < 0) {
    fprintf(stderr, "failed to open data file %s, reason:%s, will not check data/last file\n", dataFile, strerror(errno));
  } else {
    fstat(dfd, &tstat);
    dfsize = tstat.st_size;
  }

  int lfd = open(lastFile, O_RDONLY);
  if (lfd < 0) {
    fprintf(stderr, "failed to open last file %s, reason:%s, will not check data/last file\n", lastFile, strerror(errno));
  } else {
    fstat(lfd, &tstat);
    lfsize = tstat.st_size;
  }

  reportFP = fopen(reportFname, "w");
  if (reportFP == NULL) {
    fprintf(stderr, "failed to open report file %s, reason:%s, will report to stdout\n", reportFname, strerror(errno));
    reportFP = stdout;
  }

  if (hfsize < TSDB_FILE_HEADER_LEN + sizeof(SCompHeader)*pCfg->maxSessions+sizeof(TSCKSUM)) {
    fileHasError = 1;
    *err =  1;
    fprintf(reportFP, "ERROR header file %s size is too small, size:%zu, expected minimum size:%" PRId64 "\n", headFile, hfsize, TSDB_FILE_HEADER_LEN + sizeof(SCompHeader)*pCfg->maxSessions+sizeof(TSCKSUM));
  }

  if (dfsize < TSDB_FILE_HEADER_LEN) {
    // fileHasError = 1;
    // *err =  1;
    fprintf(reportFP, "ERROR data file %s size is too small, size:%zu\n", dataFile, dfsize);
    // TODO: deal with the error here
  }

  if (lfsize < TSDB_FILE_HEADER_LEN) {
    // fileHasError = 1;
    // *err =  1;
    fprintf(reportFP, "ERROR last file %s size is too small, size:%zu\n", lastFile, lfsize);
    // TODO: deal with the error here
  }

  // REPAIR
  if (!(gArg.reportOnly)) {
    repairFd = open(repairFname, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
    if (repairFd < 0) {
      fprintf(stderr, "failed to open file: %s, reason:%s\n", repairFname, strerror(errno));
    }
  }

  int offset_size = sizeof(SCompHeader) * pCfg->maxSessions + sizeof(TSCKSUM);
  pHeader = malloc(offset_size);
  if (pHeader == NULL) {
    fprintf(stderr, "failed to alloc memory\n");
    goto __exit_error;
  }

  if (fileHasError) {
    if (repairFd > 0) {
      vnodeCreateFileHeaderFd(repairFd);
      goto __exit;
    }
    goto __exit_error;
  } 

  // REPAIR
  lseek(fd, 0, SEEK_SET);
  if (repairFd > 0) {
    lseek(repairFd, 0, SEEK_SET);
    taosSendFile(repairFd, fd, NULL, TSDB_FILE_HEADER_LEN);
  } else {
    lseek(fd, TSDB_FILE_HEADER_LEN, SEEK_SET);
  }
  if (read(fd, pHeader, offset_size) < offset_size) {
    fprintf(stderr, "failed to read the SCompHeader part from header file:%s, reason:%s\n", headFile, strerror(errno));
    goto __exit_error;
  }

  // 1. Check the SCompHeader part
  if (!taosCheckChecksumWhole((uint8_t *)pHeader, offset_size)) {
    fileHasError = 1;
    *err = 1;
    fprintf(reportFP, "ERROR in offset part\n\n");
    // REPAIR
    if (repairFd > 0) {
      memset(pHeader, 0, offset_size);
    }
    goto __exit;
  } else {
    if (repairFd > 0) {
      taosWrite(repairFd, (void *)pHeader, offset_size);
    }
    fprintf(reportFP, "Offset part is correct\n\n");
  }

  // printHeader(pHeader, pCfg->maxSessions);

  size = 0;
  for (int i = 0; i < pCfg->maxSessions; i++) {// loop over table
    if (pInfo->pTable[i] == NULL || pHeader[i].compInfoOffset == 0) continue;
    if (pHeader[i].compInfoOffset < 0 || pHeader[i].compInfoOffset > hfsize) {
      fprintf(reportFP, "ERROR!!! i:%d compInfoOffset:%" PRId64 "\n", i, pHeader[i].compInfoOffset);
      fileHasError = 1;
      *err = 1;
      if (repairFd > 0) {
        pHeader[i].compInfoOffset = 0;
      }
      continue;
    }
    // Read the compInfo Block
    fprintf(reportFP, "------------------------\n");
    fprintf(reportFP, "meter sid: %d offset:%" PRId64 "\n\n", i, pHeader[i].compInfoOffset + drift);
    if (lseek(fd, pHeader[i].compInfoOffset + drift, SEEK_SET) < 0) {
      fprintf(stderr, "failed to seek head file:%s, reason:%s\n", headFile, strerror(errno));
      continue;
    }
    if (read(fd, &compInfo, sizeof(SBlockInfo)) < sizeof(SBlockInfo)) {
      fprintf(stderr, "failed to read SBlockInfo part of meter sid:%d from head file:%s, reason:%s\n", i, headFile, strerror(errno));
      continue;
    }
    if (!printCompInfo(&compInfo, pInfo->pTable[i]->uid, reportFP)) {// check the SBlockInfo part
      fileHasError = 1;
      *err = 1;
      if (repairFd > 0) {
        pHeader[i].compInfoOffset = 0;
      }
      continue;
    }

    int numOfBlocks = compInfo.numOfBlocks;
    if (numOfBlocks <= 0) {
      fprintf(reportFP, "Number of blocks is zero\n");
      fileHasError = 1;
      *err = 1;
      if (repairFd > 0) {
        pHeader[i].compInfoOffset = 0;
      }
      continue;
    }
    int tsize = numOfBlocks * sizeof(SBlock) + sizeof(TSCKSUM);
    if (tsize > size) {
      size = tsize;
      if (pBlocks == NULL) {
        pBlocks = malloc(size);
        if (pBlocks == NULL) {
          fprintf(stderr, "failed to allocate memory\n");
          abort();
        }
      } else {
        pBlocks = realloc(pBlocks, size);
        if(pBlocks == NULL) {
          fprintf(stderr, "failed to allocate memory\n");
          abort();
        }
      }
      if (repairFd > 0) pRepairBlocks = (SBlock *)realloc((void *)pRepairBlocks, size);
    }

    lseek(fd, pHeader[i].compInfoOffset + sizeof(SBlockInfo) + drift, SEEK_SET);
    read(fd, pBlocks, tsize);
    if (!taosCheckChecksumWhole((uint8_t *)pBlocks, tsize)) {
      fprintf(reportFP, "> ERROR in SBlocks\n\n");
      fileHasError = 1;
      *err = 1;
      if (repairFd > 0) {
        pHeader[i].compInfoOffset = 0;
      }
      continue;
    } else {
      fprintf(reportFP, "> Blocks part is correct\n\n");
    }

    TSKEY keyLast = 0;
    int   numOfCorrectBlocks = numOfBlocks;
    int   blockCounter = 0;
    for (int j = 0; j < numOfBlocks; j++) {
      // Check the SBlock context
      SBlock *pBlock = &pBlocks[j];
      if (pBlock->last != 0 && j < numOfBlocks - 1) {
        fprintf(reportFP, ">> ERROR in block %d: last block in middle\n", j);
        printCompBlock(pBlock, reportFP);
        fileHasError = 1;
        *err = 1;
        if (repairFd > 0) {
          pHeader[i].compInfoOffset = 0;
        }
        break;
      }

      if (pBlock->keyFirst > pBlock->keyLast) {
        fprintf(reportFP, ">> ERROR in block %d: keyFirst is larger than keyLast\n", j);
        printCompBlock(pBlock, reportFP);
        fileHasError = 1;
        *err = 1;
        if (repairFd > 0) {
          pHeader[i].compInfoOffset = 0;
        }
        break;
      }

      if (pBlock->offset < 512) {
        fprintf(reportFP, ">> ERROR in block %d: offset %" PRId64 " is smaller than 512\n", j, (int64_t)(pBlock->offset));
        printCompBlock(pBlock, reportFP);
        fileHasError = 1;
        *err = 1;
        if (repairFd > 0) {
          pHeader[i].compInfoOffset = 0;
        }
        break;
      }

      if (pBlock->keyFirst <= keyLast) {
        fprintf(reportFP, ">> ERROR in block %d: block keyFirst %" PRId64 " is not larger than last block keyLast %" PRId64 "\n", j, pBlock->keyFirst, keyLast);
        printCompBlock(pBlock, reportFP);
        fileHasError = 1;
        *err = 1;
        if (repairFd > 0) {
          pHeader[i].compInfoOffset = 0;
        }
        break;
      }
      keyLast = pBlock->keyLast;

      
      if (dfd > 0 && lfd > 0) {// Check the data file
        int tfd = -1;
        size_t toffset = 0;
        if (pBlock->last) {
          tfd = lfd;
          toffset = lfsize;
        } else {
          tfd = dfd;
          toffset = dfsize;
        }

        if (pBlock->offset > toffset) {
          fprintf(reportFP, ">> ERROR in block %d: offset %" PRId64 " is larger than file size %zu", j, (int64_t)(pBlock->offset), toffset);
          printCompBlock(pBlock, reportFP);
          numOfCorrectBlocks--;
          continue;
        }

        // Read the SField part
        if (lseek(tfd, pBlock->offset, SEEK_SET) < 0) {
          fprintf(stderr, "failed to seek, reason:%s\n", strerror(errno));
          continue;
        }

        // TODO: allocate the field part
        size_t tfsize = sizeof(SField) * pBlock->numOfCols + sizeof(TSCKSUM);
        if (fields_size < tfsize) {
          pFields = (SField *)realloc((void *)pFields, tfsize);
          if (pFields == NULL) {
            fprintf(stderr, "Failed to allocate memory\n");
            abort();
          }
          fields_size = tfsize;
        }

        if (read(tfd, pFields, tfsize) < tfsize) {
          fprintf(stderr, "failed to read SField part, reason:%s\n", strerror(errno));
          continue;
        }

        if (!taosCheckChecksumWhole((uint8_t *)pFields, tfsize)) {
          fprintf(reportFP, ">> ERROR in block %d: SField part checksum is error\n", j);
          printCompBlock(pBlock, reportFP);
          fileHasError = 1;
          *err = 1;
          numOfCorrectBlocks--;
          continue;
        } else {
          fprintf(reportFP, ">> block %d: SField part is correct\n", j);
        }

        int colHasError = 0;
        for (int ti = 0; ti < pBlock->numOfCols; ti++) {
          SField *pField = pFields + ti;
          if (lseek(tfd, pBlock->offset+pField->offset, SEEK_SET) < 0) {
            fprintf(stderr, "Failed to seek, reason:%s\n", strerror(errno));
            continue;
          }


          int32_t ttlen = pField->len + sizeof(TSCKSUM);
          if (col_size < ttlen) {
            pCol = (char *)realloc((void *)pCol, ttlen);
            col_size = ttlen;
          }

          if (read(tfd, pCol, ttlen) < ttlen) {
            fprintf(stderr, "Failed read %d bytes, reason:%s\n", ttlen, strerror(errno));
            continue;
          }

          if (!taosCheckChecksumWhole((uint8_t *)pCol, ttlen)) {
            fprintf(reportFP, ">> ERROR in block %d, column %d data part is broken\n", j, ti);
            printCompBlock(pBlock, reportFP);
            fileHasError = 1;
            *err = 1;
            colHasError = 1;
            continue;
          }
        }

        if (colHasError) {
          numOfCorrectBlocks--;
          continue;
        }

      }

      // Copy the correct block
      if (repairFd > 0) {
        memcpy((void *)(pRepairBlocks+blockCounter), (void *)pBlock, sizeof(SBlock));
        blockCounter++;
      }
    }

    if (numOfCorrectBlocks == 0) {
      pHeader[i].compInfoOffset = 0;
    }

    if (repairFd > 0) {
      if (pHeader[i].compInfoOffset != 0) {
        lseek(repairFd, pHeader[i].compInfoOffset, SEEK_SET);
        if (numOfCorrectBlocks == compInfo.numOfBlocks) {
          lseek(fd, pHeader[i].compInfoOffset, SEEK_SET);
          taosSendFile(repairFd, fd, NULL,
                    sizeof(SBlockInfo) + sizeof(SBlock) * compInfo.numOfBlocks + sizeof(TSCKSUM));
        } else {
          taosCalcChecksumAppend(0, (uint8_t *)pRepairBlocks, sizeof(SBlock)*numOfCorrectBlocks+sizeof(TSCKSUM));
          taosWrite(repairFd, (void *)pRepairBlocks, sizeof(SBlock)*numOfCorrectBlocks+sizeof(TSCKSUM));
        }
      }
    }
  }

__exit:
  if (repairFd > 0) {// write the new compHeader part to the repair FD
    lseek(repairFd, TSDB_FILE_HEADER_LEN, SEEK_SET);
    taosCalcChecksumAppend(0, (uint8_t *)pHeader, offset_size);
    taosWrite(repairFd, (void *)pHeader, offset_size);
  }

  if (pBlocks != NULL) free(pBlocks);
  if (pHeader != NULL) free(pHeader);
  if (pFields != NULL) free(pFields);
  if (pCol != NULL) free(pCol);
  if (fd > 0) close(fd);
  if (dfd > 0) close(dfd);
  if (lfd > 0) close(lfd);
  // if (reportFP != stdout) fclose(reportFP);
  if (reportFP != stdout) {  // No error, remove it
    fclose(reportFP);
    if (!(gArg.keepAll)) {
      if (fileHasError == 0) REMOVE(reportFname);
    }
  }
  if (repairFd > 0) {
    close(repairFd);
    if (fileHasError == 0) {
      REMOVE(repairFname);
    } else {
      if (gArg.inPlace) {
        char datafname[TSDB_FILENAME_LEN] = "\0";
        char backfname[TSDB_FILENAME_LEN] = "\0";
        if (readlink(headFile, datafname, TSDB_FILENAME_LEN) < 0) return;
        sprintf(backfname, "%s_back", datafname);
        if (rename(datafname, backfname) < 0) return;
        // Copy the repaired file here
        int nfd = open(datafname, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
        if (nfd < 0) return;
        repairFd = open(repairFname, O_RDONLY);
        if (repairFd < 0) {
          close(nfd);
          return;
        }

        struct stat filestat;
        fstat(repairFd, &filestat);

        lseek(nfd, 0, SEEK_SET);
        lseek(repairFd, 0, SEEK_SET);
        taosSendFile(nfd, repairFd, NULL, filestat.st_size);

        printf("  >> Recovering new file:%s\n", datafname);

        close(nfd);
        close(repairFd);
      }
    }
  }
  return;

__exit_error:
  if (pBlocks != NULL) free(pBlocks);
  if (pRepairBlocks != NULL) free(pBlocks);
  if (pHeader != NULL) free(pHeader);
  if (pFields != NULL) free(pFields);
  if (pCol != NULL) free(pCol);
  if (fd > 0) close(fd);
  if (dfd > 0) close(dfd);
  if (lfd > 0) close(lfd);
  // if (reportFP != stdout) fclose(reportFP);
  if (reportFP != stdout) {  // No error, remove it
    fclose(reportFP);
    if (fileHasError == 0) REMOVE(reportFname);
  }
  if (repairFd > 0) {
    close(repairFd);
    if (fileHasError == 0) {
      REMOVE(repairFname);
    }
  }
  return;
}

void printHeader(SCompHeader *pHeader, int maxSessions) {
  printf("==========OFFSET===============\n");
  for (int i = 0; i < maxSessions; i++) {
    printf("i: %d offset:%" PRId64 "\n", i, pHeader[i].compInfoOffset);
  }
  printf("===============================\n");
}

void printVnodeCfg(FILE *fp, SVnodeCfg *pCfg) {
  fprintf(fp, "============VnodeCfg=============\n");
  fprintf(fp, "acct:            %s\n", pCfg->acct);
  fprintf(fp, "db:              %s\n", pCfg->db);
  fprintf(fp, "vgId:            %d\n", pCfg->vgId);
  fprintf(fp, "maxSessions:     %d\n", pCfg->maxSessions);
  fprintf(fp, "cacheBlockSize:  %d\n", pCfg->cacheBlockSize);
  fprintf(fp, "daysPerFile:     %d\n", pCfg->daysPerFile);
  fprintf(fp, "daysToKeep1:     %d\n", pCfg->daysToKeep1);
  fprintf(fp, "daysToKeep2:     %d\n", pCfg->daysToKeep2);
  fprintf(fp, "daysToKeep:      %d\n", pCfg->daysToKeep);
  fprintf(fp, "rowsInFileBlock: %d\n", pCfg->rowsInFileBlock);
  fprintf(fp, "blocksPerTable:  %d\n", pCfg->blocksPerTable);
  fprintf(fp, "precision:       %d\n", pCfg->precision);
  fprintf(fp, "================================\n\n");
}

int printCompInfo(SBlockInfo *pCompInfo, uint64_t uid, FILE *fp) {
  int isRight = 1;
  if (!taosCheckChecksumWhole((uint8_t *)pCompInfo, sizeof(SBlockInfo))) isRight = 0;
  fprintf(fp, "CompInfo:\n");
  fprintf(fp, "uid:         %" PRIu64 "\n", pCompInfo->uid);
  fprintf(fp, "last:        %" PRId64 "\n", (int64_t)(pCompInfo->last));
  fprintf(fp, "numOfBlocks: %" PRId64 "\n", (int64_t)(pCompInfo->numOfBlocks));
  fprintf(fp, "delimeter:   %u\n", pCompInfo->delimiter);
  fprintf(fp, "checksum:    %d\n", pCompInfo->checksum);
  if (isRight == 0) {
    fprintf(fp, "> ERROR in CompInfo part\n");
  } else {
    fprintf(fp, "> CompInfo part is correct\n");
    if (uid != pCompInfo->uid) {
      fprintf(fp, "> ERROR: CompInfo uid not match, obj uid:%" PRIu64 ", comp uid:%" PRIu64 "\n", uid, pCompInfo->uid);
      isRight = 0;
    }
  }
  fprintf(fp, "\n");
  return isRight;
}

void printCompBlock(SBlock *pBlock, FILE *fp) {
  fprintf(fp, "   last:        %" PRId64 "\n",  (int64_t)(pBlock->last));
  fprintf(fp, "   offset:      %" PRId64 "\n",  (int64_t)(pBlock->offset));
  fprintf(fp, "   algorithm:   %d\n",   (int32_t)(pBlock->algorithm));
  fprintf(fp, "   numOfPoints: %d\n",   (int32_t)(pBlock->numOfPoints));
  fprintf(fp, "   sversion:    %d\n",   (int32_t)(pBlock->sversion));
  fprintf(fp, "   len:         %d\n",   (int32_t)(pBlock->len));
  fprintf(fp, "   numOfCols:   %d\n",   (int16_t)(pBlock->numOfCols));
  fprintf(fp, "   keyFirst:    %" PRId64 "\n",  (int64_t)(pBlock->keyFirst));
  fprintf(fp, "   keyLast:     %" PRId64 "\n",  (int64_t)(pBlock->keyLast));
}

int createDir(const char *dirName) {
  struct stat st = {0};
  if (stat(dirName, &st) == -1) {
    if (mkdir(dirName, 0700) < 0) {
      fprintf(stderr, "failed to create directory: %s\n", dirName);
      return -1;
    }
  }
  return 0;
}

void vnodeCreateFileHeaderFd(int fd) {
  char temp[TSDB_FILE_HEADER_LEN / 4];
  int  lineLen;

  lineLen = sizeof(temp);

  // write the first line`
  memset(temp, 0, lineLen);
  *(int16_t *)temp = 0;
  sprintf(temp + sizeof(int16_t), "tsdb version: %s\n", "1.6.2.0");
  /* *((int16_t *)(temp + TSDB_FILE_HEADER_LEN/8)) = vnodeFileVersion; */
  lseek(fd, 0, SEEK_SET);
  taosWrite(fd, temp, lineLen);

  // second line
  memset(temp, 0, lineLen);
  taosWrite(fd, temp, lineLen);

  // the third/forth line is the dynamic info
  memset(temp, 0, lineLen);
  taosWrite(fd, temp, lineLen);
  taosWrite(fd, temp, lineLen);
}

void scanDir(const char *dbDir, SVnodeInfo *pInfo) {
  int   minFileId = INT32_MAX;
  int   maxFileId = 0;
  int   vnode = 0;
  char  buf[128] = "\0";
  int   bufSize = 128;
  char  lheadName[128] = "\0";
  char  ldataName[128] = "\0";
  char  llastName[128] = "\0";
  char  headName[128] = "\0";
  char  dataName[128] = "\0";
  char  lastName[128] = "\0";
  int   flag = 1;
  char *dDir = NULL;

  struct dirent *dent = NULL;
  DIR *          dir = opendir(dbDir);
  if (dir) {
    while ((dent = readdir(dir)) != NULL) {
      if (strcmp(dent->d_name, ".") == 0 || strcmp(dent->d_name, "..") == 0) continue;
      if (strcmp(dent->d_name + strlen(dent->d_name) - 5, ".head") != 0) continue;

      int fileId = 0;
      sscanf(dent->d_name, "v%df%d.head", &vnode, &fileId);
      if (minFileId > fileId) minFileId = fileId;
      if (maxFileId < fileId) maxFileId = fileId;
      if (flag) {
        char tmpfile[1024] = "\0";
        sprintf(tmpfile, "%s/%s", dbDir, dent->d_name);
        readlink(tmpfile, buf, bufSize);
        dDir = dirname(buf);
        flag = 0;
      }
    }
    closedir(dir);
  } else {
    fprintf(stderr, "failed to open directory:%s\n", dbDir);
    return;
  }

  printf(" > vnode:%d minFileId:%d maxFileId:%d, vnodeFileId:%d numOfFiles:%d\n", vnode, minFileId, maxFileId,
         pInfo->fileId, pInfo->numOfFiles);
  {
    for (int fid = pInfo->fileId - pInfo->numOfFiles + 1; fid <= pInfo->fileId; fid++) {
      sprintf(lheadName, "%s/v%df%d.head", dbDir, vnode, fid);
      sprintf(ldataName, "%s/v%df%d.data", dbDir, vnode, fid);
      sprintf(llastName, "%s/v%df%d.last", dbDir, vnode, fid);

      if (access(lheadName, F_OK) == -1) {  // .head not exists
        remove(lheadName);
        sprintf(headName, "%s/v%df%d.head0", dDir, vnode, fid);
        printf("  >> Need to create missing file: %s\n", headName);
        if (gArg.inPlace) {
          symlink(headName, lheadName);
          int fd = open(lheadName, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
          if (fd < 0) {
            fprintf(stderr, "failed to open file:%s, reason:%s\n", headName, strerror(errno));
            continue;
          }

          vnodeCreateFileHeaderFd(fd);

          int   size = sizeof(SCompHeader) * pInfo->cfg.maxSessions + sizeof(TSCKSUM);
          char *pHeader = malloc(size);
          memset((void *)pHeader, 0, size);
          taosCalcChecksumAppend(0, (uint8_t *)pHeader, size);
          lseek(fd, TSDB_FILE_HEADER_LEN, SEEK_SET);
          taosWrite(fd, (void *)pHeader, size);
          free(pHeader);
          close(fd);
        }
      }

      if (access(ldataName, F_OK) == -1) {  // .data not exists
        remove(ldataName);
        sprintf(dataName, "%s/v%df%d.data", dDir, vnode, fid);
        printf("  >> Need to create missing file: %s\n", dataName);
        if (gArg.inPlace) {
          symlink(dataName, ldataName);
          int fd = open(ldataName, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
          if (fd < 0) {
            fprintf(stderr, "failed to create data file:%s, reason:%s", dataName, strerror(errno));
            continue;
          }

          vnodeCreateFileHeaderFd(fd);
          close(fd);
        }
      }

      if (access(llastName, F_OK) == -1) {  // .last not exists
        remove(llastName);
        sprintf(lastName, "%s/v%df%d.last0", dDir, vnode, fid);
        printf("  >> Need to create missing file: %s\n", lastName);
        if (gArg.inPlace) {
          symlink(lastName, llastName);
          int fd = open(llastName, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
          if (fd < 0) {
            fprintf(stderr, "failed to create data file:%s, reason:%s", lastName, strerror(errno));
            continue;
          }

          vnodeCreateFileHeaderFd(fd);
          close(fd);
        }
      }
    }
  }
  return;
}
