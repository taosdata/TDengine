/***************************************************************************
 * A program illustrating mapping source identifiers and SEED codes
 *
 * This file is part of the miniSEED Library.
 *
 * Copyright (c) 2020 Chad Trabant, IRIS Data Management Center
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ***************************************************************************/

#include <libmseed.h>

int
map_sid (char *original_sid)
{
  char sid[LM_SIDLEN];
  char network[11];
  char station[11];
  char location[11];
  char channel[31];
  int rv;

  /* Parse network, station, location and channel from SID */
  rv = ms_sid2nslc (original_sid, network, station, location, channel);

  if (rv)
  {
    printf ("Error returned ms_sid2nslc()\n");
    return -1;
  }

  /* Construct SID from network, station, location and channel */
  rv = ms_nslc2sid (sid, sizeof(sid), 0, network, station, location, channel);

  if (rv <= 0)
  {
    printf ("Error returned ms_nslc2sid()\n");
    return -1;
  }

  printf ("Original SID: '%s'\n", original_sid);
  printf ("  network: '%s', station: '%s', location: '%s', channel: '%s'\n",
          network, station, location, channel);
  printf ("  SID: '%s'\n", sid);

  return 0;
}

int
main (int argc, char **argv)
{
  char *sid;
  int idx;

  sid = "XFDSN:NET_STA_LOC_C_H_N";

  if (map_sid (sid))
  {
    printf ("Error with map_sid()\n");
    return 1;
  }

  /* Map each value give on the command line */
  if (argc > 1)
  {
    for (idx = 1; idx < argc; idx++)
    {
      if (map_sid (argv[idx]))
      {
        printf ("Error with map_sid()\n");
        return 1;
      }
    }
  }

  return 0;
}
