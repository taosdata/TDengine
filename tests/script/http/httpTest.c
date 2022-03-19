#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <netinet/in.h>
#include <stdlib.h>
 
#define MAXLINE 1024

typedef struct {
  TdThread pid;
  int threadId;
  int rows;
  int tables;
} ThreadObj;
 
void post(char *ip,int port,char *page,char *msg) {
    int sockfd,n;
    char recvline[MAXLINE];
    struct sockaddr_in servaddr;
    char content[4096];
    char content_page[50];
    sprintf(content_page,"POST /%s HTTP/1.1\r\n",page);
    char content_host[50];
    sprintf(content_host,"HOST: %s:%d\r\n",ip,port);
    char content_type[] = "Content-Type: text/plain\r\n";
    char Auth[] = "Authorization: Basic cm9vdDp0YW9zZGF0YQ==\r\n";
    char content_len[50];
    sprintf(content_len,"Content-Length: %ld\r\n\r\n",strlen(msg));
    sprintf(content,"%s%s%s%s%s%s",content_page,content_host,content_type,Auth,content_len,msg);
    if((sockfd = socket(AF_INET,SOCK_STREAM,0)) < 0) {
        printf("socket error\n");
    }
    bzero(&servaddr,sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(port);
    if(inet_pton(AF_INET,ip,&servaddr.sin_addr) <= 0) {
        printf("inet_pton error\n");
    }   
    if(connect(sockfd,(struct sockaddr *)&servaddr,sizeof(servaddr)) < 0) {
        printf("connect error\n");
    }
    write(sockfd,content,strlen(content));
    printf("%s\n", content);
    while((n = read(sockfd,recvline,MAXLINE)) > 0) {
        recvline[n] = 0;
        if(fputs(recvline,stdout) == EOF) {
            printf("fputs error\n");
        }
    }
    if(n < 0) {
        printf("read error\n");
    }
}

void singleThread() {
    char ip[] = "127.0.0.1";
    int port = 6041;
    char page[] = "rest/sql";
    char page1[] = "rest/sql/db1";
    char page2[] = "rest/sql/db2";
    char nonexit[] = "rest/sql/xxdb";

    post(ip,port,page,"drop database if exists db1");
    post(ip,port,page,"create database if not exists db1");
    post(ip,port,page,"drop database if exists db2");
    post(ip,port,page,"create database if not exists db2");
    post(ip,port,page1,"create table t11 (ts timestamp, c1 int)");
    post(ip,port,page2,"create table t21 (ts timestamp, c1 int)");
    post(ip,port,page1,"insert into t11 values (now, 1)");
    post(ip,port,page2,"insert into t21 values (now, 2)");
    post(ip,port,nonexit,"create database if not exists db3");
}

void execute(void *params) {
    char ip[] = "127.0.0.1";
    int port = 6041;
    char page[] = "rest/sql";
    char *unique = calloc(1, 1024);
    char *sql = calloc(1, 1024);
    ThreadObj *pThread = (ThreadObj *)params;
    printf("Thread %d started\n", pThread->threadId);
    sprintf(unique, "rest/sql/db%d",pThread->threadId);
    sprintf(sql, "drop database if exists db%d", pThread->threadId);
    post(ip,port,page, sql);
    sprintf(sql, "create database if not exists db%d", pThread->threadId);
    post(ip,port,page, sql);
    for (int i = 0; i < pThread->tables; i++) {
        sprintf(sql, "create table t%d (ts timestamp, c1 int)", i);
        post(ip,port,unique, sql);
    }
    for (int i = 0; i < pThread->rows; i++) {
        sprintf(sql, "insert into t%d values (now + %ds, %d)", pThread->threadId, i, pThread->threadId);
        post(ip,port,unique, sql);
    }
    free(unique);
    free(sql);
    return;
}

void multiThread() {
    int numOfThreads = 100;
    int numOfTables = 100;
    int numOfRows = 1;
    ThreadObj *threads = calloc((size_t)numOfThreads, sizeof(ThreadObj));
    for (int i = 0; i < numOfThreads; i++) {
        ThreadObj *pthread = threads + i;
        TdThreadAttr thattr;
        pthread->threadId = i + 1;
        pthread->rows = numOfRows;
        pthread->tables = numOfTables;
        taosThreadAttrInit(&thattr);
        taosThreadAttrSetDetachState(&thattr, PTHREAD_CREATE_JOINABLE);
        taosThreadCreate(&pthread->pid, &thattr, (void *(*)(void *))execute, pthread);
    }
    for (int i = 0; i < numOfThreads; i++) {
        taosThreadJoin(threads[i].pid, NULL);
    }
    free(threads);
}
 
int main() {
    singleThread();
    multiThread();
    exit(0);
}