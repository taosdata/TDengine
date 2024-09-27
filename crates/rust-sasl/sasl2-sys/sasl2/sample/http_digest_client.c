/*
 * Cheesy HTTP 1.1 client used for testing HTTP Digest (RFC 2617)
 * variant of DIGEST-MD5 plugin
 *
 * XXX  This client REQUIRES a persistent connection and
 * WILL NOT accept a body in any HTTP response
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pwd.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#include <sasl.h>

#define SUCCESS 0
#define ERROR   1

#define BUFFER_SIZE 8192

#define DIGEST_AUTH_HEADER "\r\nWWW-Authenticate: Digest "
#define DIGEST_OK_HEADER "\r\nAuthentication-Info: "

void interact(sasl_interact_t *ilist)
{
    while (ilist->id != SASL_CB_LIST_END) {
	switch (ilist->id) {

	case SASL_CB_AUTHNAME:			/* auth as current uid */
	    ilist->result = strdup(getpwuid(getuid())->pw_name);
	    break;

	case SASL_CB_PASS:			/* prompt for password */
	    printf("%s: ", ilist->prompt);
	    ilist->result = strdup(getpass(""));
	    break;
	}
	ilist->len = strlen(ilist->result);

	ilist++;
    }
}

int main(int argc __attribute__((unused)), char *argv[])
{
    const char *hostname = "localhost";	
    int port = 80;
	
    int sd, rc, status;
    struct sockaddr_in localAddr, servAddr;
    struct hostent *h;

    const char *sasl_impl, *sasl_ver;
    sasl_conn_t *saslconn;
    sasl_interact_t *interactions = NULL;
    sasl_security_properties_t secprops = { 0,		/* min SSF ("auth") */
					    1,		/* max SSF ("auth-int") */
					    0,		/* don't need maxbuf */
					    0,		/* security flags */
					    NULL,
					    NULL };
    sasl_http_request_t httpreq = { "HEAD",		/* Method */
				    "/",		/* URI */
				    (u_char *) "",	/* Empty body */
				    0,			/* Zero-length body */
				    0 };		/* Persistent cxn */
    sasl_callback_t callbacks[] = {
	{ SASL_CB_AUTHNAME, NULL, NULL },
	{ SASL_CB_PASS, NULL, NULL },
	{ SASL_CB_LIST_END, NULL, NULL }
    };

    const char *response = NULL;
    unsigned int resplen = 0;
    char buffer[BUFFER_SIZE+1], *request, *challenge, *p;
    int i, code;

    printf("\n-- Hostname = %s , Port = %d , URI = %s\n",
	   hostname, port, httpreq.uri);

    h = gethostbyname(hostname);
    if(h == NULL) {
	printf("unknown host: %s \n ", hostname);
	exit(ERROR);
    }

    servAddr.sin_family = h->h_addrtype;
    memcpy((char *) &servAddr.sin_addr.s_addr, h->h_addr_list[0], h->h_length);
    servAddr.sin_port = htons(port);

    /* create socket */
    printf("-- Create socket...    ");
    sd = socket(AF_INET, SOCK_STREAM, 0);
    if (sd < 0) {
	perror("cannot open socket ");
	exit(ERROR);
    }

    /*  bind port number */
    printf("Bind port number...  ");
	
    localAddr.sin_family = AF_INET;
    localAddr.sin_addr.s_addr = htonl(INADDR_ANY);
    localAddr.sin_port = htons(0);
  
    rc = bind(sd, (struct sockaddr *) &localAddr, sizeof(localAddr));
    if (rc < 0) {
	printf("%s: cannot bind port TCP %u\n",argv[0],port);
	perror("error ");
	exit(ERROR);
    }
				
    /* connect to server */
    printf("Connect to server...\n");
    rc = connect(sd, (struct sockaddr *) &servAddr, sizeof(servAddr));
    if (rc < 0) {
	perror("cannot connect ");
	exit(ERROR);
    }

    /* get SASL version info */
    sasl_version_info(&sasl_impl, &sasl_ver, NULL, NULL, NULL, NULL);

    /* initialize client-side of SASL */
    status = sasl_client_init(callbacks);

    /* request the URI twice, so we test both initial auth and reauth */
    for (i = 0; i < 2; i++) {
	/* initialize a client exchange
	 *
	 * SASL_NEED_HTTP:    forces HTTP Digest mode (REQUIRED)
	 * SASL_SUCCESS_DATA: HTTP supports success data in
	 *                    Authentication-Info header (REQUIRED)
	 */
	status = sasl_client_new("http", hostname, NULL, NULL, NULL,
			     SASL_NEED_HTTP | SASL_SUCCESS_DATA, &saslconn);
	if (status != SASL_OK) {
	    perror("sasl_client_new() failed ");
	    exit(ERROR);
	}

	/* Set security peoperties as specified above */
	sasl_setprop(saslconn, SASL_SEC_PROPS, &secprops);

	/* Set HTTP request as specified above (REQUIRED) */
	sasl_setprop(saslconn, SASL_HTTP_REQUEST, &httpreq);

	do {
	    /* start the Digest exchange */
	    status = sasl_client_start(saslconn, "DIGEST-MD5", &interactions,
				       &response, &resplen, NULL);
	    if (status == SASL_INTERACT) interact(interactions);
	} while (status == SASL_INTERACT);

	if ((status != SASL_OK) && (status != SASL_CONTINUE)) {
	    perror("sasl_client_start() failed ");
	    exit(ERROR);
	}

	do {
	    /* send request (with Auth data if we have it ) */
	    request = buffer;
	    request += sprintf(request, "%s %s HTTP/1.1\r\n",
			       httpreq.method, httpreq.uri);
	    request += sprintf(request, "Host: %s\r\n", hostname);
	    request += sprintf(request, "User-Agent: HTTP Digest Test Client"
			       " (%s/%s)\r\n", sasl_impl, sasl_ver);
	    request += sprintf(request, "Connection: keep-alive\r\n");
	    request += sprintf(request, "Keep-Alive: 300\r\n");
	    if (response) {
		request += sprintf(request, "Authorization: Digest %s\r\n",
				   response);
	    }
	    request += sprintf(request, "\r\n");
	    request = buffer;

	    printf("\n-- Send HTTP request:\n\n%s", request);
	    rc = write(sd, request, strlen(request));
	    if (rc < 0) {   
		perror("cannot send data ");
		close(sd);
		exit(ERROR);  
	    }
	
	    /* display response */
	    printf("-- Received response:\n\tfrom server: http://%s%s, IP = %s,\n\n",
		   hostname, httpreq.uri, inet_ntoa(servAddr.sin_addr));
	    rc = read(sd, buffer, BUFFER_SIZE);
	    if (rc <= 0) {   
		perror("cannot read data ");
		close(sd);
		exit(ERROR);  
	    }
	    buffer[rc] = '\0';

	    printf("%s", buffer);

	    /* get response code */
	    sscanf(buffer, "HTTP/1.1 %d ", &code);

	    if (code == 401) {
		/* find Digest challenge */
		challenge = strstr(buffer, DIGEST_AUTH_HEADER);
		if (!challenge) break;
		challenge += strlen(DIGEST_AUTH_HEADER);
		p = strchr(challenge, '\r');
		*p = '\0';

		do {
		    /* do the next step in the exchange */
		    status = sasl_client_step(saslconn,
					      challenge, strlen(challenge),
					      &interactions,
					      &response, &resplen);
		    if (status == SASL_INTERACT) interact(interactions);
		} while (status == SASL_INTERACT);

		if ((status != SASL_OK) && (status != SASL_CONTINUE)) {
		    perror("sasl_client_step failed ");
		    exit(ERROR);
		}
	    }
	} while (code == 401);

	if ((code == 200) && (status == SASL_CONTINUE)) {
	    /* find Digest response */
	    challenge = strstr(buffer, DIGEST_OK_HEADER);
	    if (challenge) {
		challenge += strlen(DIGEST_OK_HEADER);
		p = strchr(challenge, '\r');
		*p = '\0';

		/* do the final step in the exchange (server auth) */
		status = sasl_client_step(saslconn,
					  challenge, strlen(challenge),
					  &interactions, &response, &resplen);
	    }
	}

	sasl_dispose(&saslconn);
    }

    sasl_client_done();

    close(sd);
    return SUCCESS;
}
