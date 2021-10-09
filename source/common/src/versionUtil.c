#include "os.h"
#include "tdef.h"
#include "ulog.h"
#include "taoserror.h"

bool taosGetVersionNumber(char *versionStr, int *versionNubmer) {
  if (versionStr == NULL || versionNubmer == NULL) {
    return false;
  }

  int versionNumberPos[5] = {0};
  int len = (int)strlen(versionStr);
  int dot = 0;
  for (int pos = 0; pos < len && dot < 4; ++pos) {
    if (versionStr[pos] == '.') {
      versionStr[pos] = 0;
      versionNumberPos[++dot] = pos + 1;
    }
  }

  if (dot != 3) {
    return false;
  }

  for (int pos = 0; pos < 4; ++pos) {
    versionNubmer[pos] = atoi(versionStr + versionNumberPos[pos]);
  }
  versionStr[versionNumberPos[1] - 1] = '.';
  versionStr[versionNumberPos[2] - 1] = '.';
  versionStr[versionNumberPos[3] - 1] = '.';

  return true;
}

int taosCheckVersion(char *input_client_version, char *input_server_version, int comparedSegments) {
  char client_version[TSDB_VERSION_LEN] = {0};
  char server_version[TSDB_VERSION_LEN] = {0};
  int clientVersionNumber[4] = {0};
  int serverVersionNumber[4] = {0};

  tstrncpy(client_version, input_client_version, sizeof(client_version));
  tstrncpy(server_version, input_server_version, sizeof(server_version));

  if (!taosGetVersionNumber(client_version, clientVersionNumber)) {
    uError("invalid client version:%s", client_version);
    return TSDB_CODE_TSC_INVALID_VERSION;
  }

  if (!taosGetVersionNumber(server_version, serverVersionNumber)) {
    uError("invalid server version:%s", server_version);
    return TSDB_CODE_TSC_INVALID_VERSION;
  }

  for(int32_t i = 0; i < comparedSegments; ++i) {
    if (clientVersionNumber[i] != serverVersionNumber[i]) {
      uError("the %d-th number of server version:%s not matched with client version:%s", i, server_version,
             client_version);
      return TSDB_CODE_TSC_INVALID_VERSION;
    }
  }

  return 0;
}
