

int taosGetFqdnPortFromEp(const char *ep, char *fqdn, uint16_t *port) {
  *port = 0;
  strcpy(fqdn, ep);

  char *temp = strchr(fqdn, ':');
  if (temp) {
    *temp = 0;
    *port = atoi(temp+1);
  }

  if (*port == 0) {
    *port = tsServerPort;
    return -1;
  }

  return 0;
}

