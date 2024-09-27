#ifdef RUBBISH

*** how to run the server on unix
./sample-server -s rcmd -i local=0.0.0.0:23,remote=0.0.0.0:23 -m KERBEROS_V4

*** arguements to the client on the mac

Use this to test privacy:
-b min=56,max=20000 -i local=0.0.0.0:23,remote=0.0.0.0:23 -s rcmd -n AKUTAKTAK.ANDREW.CMU.EDU -u n3liw

Use this to test authenticity:
-b min=1,max=1 -i local=0.0.0.0:23,remote=0.0.0.0:23 -s rcmd -n AKUTAKTAK.ANDREW.CMU.EDU -u n3liw

Use this to test authentication only (no privacy no authenticity):
-i local=0.0.0.0:23,remote=0.0.0.0:23 -s rcmd -n AKUTAKTAK.ANDREW.CMU.EDU -u n3liw


C: BAYAQU5EUkVXLkNNVS5FRFUAOCBx+Dj9fo8RD0Wegm7Qr2iSopuKxKGTq6cA6ux+lEPfB4GFO9BxF9jWOKLa5Hw/sIqkSfcqwah+hLFCUakVHcviUo7UOTHX0CFWy8QsnCuz6qco9FzlS23r

- check lifetimes of data returned by kerberos glue functions
  like realm of host and gethostbyname
C: AAAAbAQGAEFORFJFVy5DTVUuRURVADggcfg4/X6PEQ9FnoJu0K9okqKbisShk6unYXiKjun/vccUEytAAMdTj1pLaQjd3hkDltVId4q9la64zfZG+haHMETI+kDpHzLAtABnUTl4NHvzjbuwfwdvSA==

-e ssf=-57 -i local=128.2.121.100:23,remote=128.2.121.2:23 -s rcmd -n AKUTAKTAK.ANDREW.CMU.EDU -u n3liw

#endif

static int bletch_the_compiler_wants_something_non_empty;
