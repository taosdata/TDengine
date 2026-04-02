/* Process data migration
 */

#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <stdint.h>
#include <wordexp.h>
#include <unistd.h>
#include <libgen.h>

#include "mnode.h"
#include "vnode.h"
#include "taosdef.h"

#define SDB_DELIMITER 0xFFF00F00

typedef struct
{
    uint64_t swVersion;
    int16_t sdbFileVersion;
    char reserved[6];
    TSCKSUM checkSum;
} SSdbHeader;

typedef struct
{
    int32_t delimiter;
    int32_t rowSize;
    int64_t id;
    char data[];
} SRowHead;

typedef struct
{
    char ip_str[TSDB_IPv4ADDR_LEN];
    in_addr_t ip_val;
} SIPEntry;

typedef struct
{
    SIPEntry o_publicIp;
    SIPEntry o_privateIp;
    SIPEntry o_internalIp;
    SIPEntry publicIp;
    SIPEntry privateIp;
    SIPEntry internalIp;
} SDnodeModEntry;

typedef void (*sdb_mod_fun_t)(void *, SDnodeModEntry *, int);

void modDnodeObj(void *buff, SDnodeModEntry *dnodeTable, int n_nodes) {
    SDnodeObj *pObj = (SDnodeObj *)(buff);

    int idx = -1;
    for (int i = 0; i < n_nodes; i++)
    {
        if (dnodeTable[i].o_privateIp.ip_val == pObj->privateIp /*&&
            dnodeTable[i].o_publicIp.ip_val == pObj->publicIp*/)
        {
            idx = i;
            break;
        }
    }

    if (idx < 0)
    {
        fprintf(stderr, "ERROR! Invalid dnode IP address, privateIp:%u publicIp:%u\n", pObj->publicIp, pObj->publicIp);
        abort();
    }

    pObj->privateIp = dnodeTable[idx].privateIp.ip_val;
    // pObj->publicIp = dnodeTable[idx].publicIp.ip_val;

    return;
}

void modSdbPeer(void *buff, SDnodeModEntry *dnodeTable, int n_nodes) {
    SSdbPeer *pObj = (SSdbPeer *)(buff);

    int idx = -1;
    for (int i = 0; i < n_nodes; i++)
    {
        if (dnodeTable[i].o_privateIp.ip_val == pObj->ip /*&&
            dnodeTable[i].o_publicIp.ip_val == pObj->publicIp &&
            strcmp(pObj->ipstr, dnodeTable[i].o_privateIp.ip_str) == 0*/)
        {
            idx = i;
            break;
        }
    }

    if (idx < 0)
    {
        fprintf(stderr, "ERROR! Invalid sdbPeer IP address privateIp:%u pbulicIp:%u ipstr:%s\n", pObj->ip, pObj->publicIp, pObj->ipstr);
        abort();
    }

    pObj->ip = dnodeTable[idx].privateIp.ip_val;
    pObj->publicIp = dnodeTable[idx].publicIp.ip_val;
    strcpy(pObj->ipstr, dnodeTable[idx].privateIp.ip_str);
}

void modVgObj(void *buff, SDnodeModEntry *dnodeTable, int n_nodes) {
    SVgObj *pObj = (SVgObj *)(buff);
    for (int j = 0; j < TSDB_VNODES_SUPPORT; j++)
    {
        SVnodeGid *pVgid = pObj->vnodeGid + j;
        if (pVgid->ip == 0)
            continue;
        int idx = -1;
        for (int i = 0; i < n_nodes; i++)
        {
            if (dnodeTable[i].o_privateIp.ip_val == pVgid->ip /*&&
                dnodeTable[i].o_publicIp.ip_val == pVgid->publicIp*/)
            {
                idx = i;
                break;
            }
        }

        if (idx < 0)
        {
            fprintf(stderr, "ERROR! Invalid vgroup IP address, privateIp:%u publicIp:%u\n", pVgid->ip, pVgid->publicIp);
            abort();
        }

        pVgid->ip = dnodeTable[idx].privateIp.ip_val;
        pVgid->publicIp = dnodeTable[idx].publicIp.ip_val;
    }

}

typedef struct {
    char *filename;
    sdb_mod_fun_t func;
} SSdbFileModifier;

void printIPChangeSummary(const char *rootDir, const SDnodeModEntry *dnodeTable, int n_nodes) {
    printf("********************** Mod Info ******************************\n");
    printf("* Data directory: %s\n", rootDir);
    printf("* Number of nodes: %d\n", n_nodes);
    for (int i = 0; i < n_nodes; i++)
    {
        puts("*");
        printf("* Node %d:\n", i+1);
        printf("*     old publicIp:%s    =====>  new publicIp:%s\n", dnodeTable[i].o_publicIp.ip_str, dnodeTable[i].publicIp.ip_str);
        printf("*     old privateIp:%s   =====>  new privateIp:%s\n", dnodeTable[i].o_privateIp.ip_str, dnodeTable[i].privateIp.ip_str);
        // printf("*     old internalIp:%s  =====>  new internalIp:%s\n", dnodeTable[i].o_internalIp.ip_str, dnodeTable[i].internalIp.ip_str);
    }
    printf("**************************************************************\n");
}

int main(int argc, char *argv[])
{
    char rootDir[TSDB_FILENAME_LEN] = "\0";
    char tsdbDir[TSDB_FILENAME_LEN] = "\0";
    char dataDir[TSDB_FILENAME_LEN] = "\0";
    char mgmtDir[TSDB_FILENAME_LEN] = "\0";
    char vnodeDir[388] = "\0";
    char linkName[645] = "\0";
    char targetName[TSDB_FILENAME_LEN] = "\0";
    wordexp_t full_path;
    struct dirent *dent1, *dent2;

    printf("Welcome to use the TDengine data migrate tool. Please make sure\n");
    printf("to run this tool on the machine the data is migrated to, and \n");
    printf("follow the instructions.\n\n");

    printf("Please enter the data directory (it is /var/lib/taos by default): ");
    fgets(rootDir, TSDB_FILENAME_LEN, stdin);

    size_t size = strlen(rootDir);
    if (size == 1) {
        printf("Using default data directory:/var/lib/taos\n");
        strcpy(rootDir, "/var/lib/taos");
    } else {
        if (rootDir[size - 1] == '\n')
            rootDir[size - 1] = '\0';
    }

    if (wordexp(rootDir, &full_path, 0) != 0)
    {
        fprintf(stderr, "Invalid file path: %s\n", rootDir);
        exit(EXIT_FAILURE);
    }

    strcpy(rootDir, full_path.we_wordv[0]);
    wordfree(&full_path);

    printf("Please enter the number of nodes you want to migrate, or enter a number <= 0 to skip IP modification: ");
    int n_nodes = 0;
    fscanf(stdin, "%d", &n_nodes);

    SDnodeModEntry *dnodeTable = NULL;

    if (n_nodes > 0) {
        dnodeTable = taosMemoryCalloc(n_nodes, sizeof(SDnodeModEntry));
        if (dnodeTable == NULL)
        {
            fprintf(stderr, "ERROR! Failed to allocate memory\n");
            exit(EXIT_FAILURE);
        }

        for (int i = 0; i < n_nodes; i++)
        {
            puts("");
            printf("Please enter the DNODE %d info:\n", i);

            printf(">> Enter the old public IP: ");
            scanf("%s", dnodeTable[i].o_publicIp.ip_str);
            dnodeTable[i].o_publicIp.ip_val = inet_addr(dnodeTable[i].o_publicIp.ip_str);

            printf(">> Enter the old private IP: ");
            scanf("%s", dnodeTable[i].o_privateIp.ip_str);
            dnodeTable[i].o_privateIp.ip_val = inet_addr(dnodeTable[i].o_privateIp.ip_str);

            // printf(">>Enter the old internal IP: ");
            // scanf("%s", dnodeTable[i].o_internalIp.ip_str);
            // dnodeTable[i].o_internalIp.ip_val = inet_addr(dnodeTable[i].o_internalIp.ip_str);

            printf(">> Enter the new public IP: ");
            scanf("%s", dnodeTable[i].publicIp.ip_str);
            dnodeTable[i].publicIp.ip_val = inet_addr(dnodeTable[i].publicIp.ip_str);

            printf(">> Enter the new private IP: ");
            scanf("%s", dnodeTable[i].privateIp.ip_str);
            dnodeTable[i].privateIp.ip_val = inet_addr(dnodeTable[i].privateIp.ip_str);

            // printf(">>Enter the new internal IP: ");
            // scanf("%s", dnodeTable[i].internalIp.ip_str);
            // dnodeTable[i].internalIp.ip_val = inet_addr(dnodeTable[i].internalIp.ip_str);
        }
    }


    printIPChangeSummary(rootDir, dnodeTable, n_nodes);

    if (n_nodes <= 0) goto __link_mod;

    // =======================================  MOD MGMT FILES =======================================
    printf("Start to process mgmt files in rootDir:%s\n", rootDir);

    sprintf(mgmtDir, "%s/mgmt", rootDir);

    // Register the SDB modifier vector table
    SSdbFileModifier sdb_mod_vector_table[] = {
        {"dnodes.db", (sdb_mod_fun_t)modDnodeObj},
        {"mnode.db", (sdb_mod_fun_t)modSdbPeer},
        {"vgroups.db", (sdb_mod_fun_t)modVgObj}
    };

    char ofname[400] = "\0";
    TdFilePtr pFile = NULL;
    uint32_t sdbEcommit = 0;
    SRowHead *pRowHead = taosMemoryMalloc(1024 * 1024);

    taosResolveCRC();

    for (int k = 0; k < sizeof(sdb_mod_vector_table)/sizeof(sdb_mod_vector_table[0]); k++)
    {
        sprintf(ofname, "%s/%s", mgmtDir, sdb_mod_vector_table[k].filename);
        pFile =  taosOpenFile(ofname, TD_FILE_WRITE | TD_FILE_READ);
        if (pFile == NULL)
        {
            fprintf(stderr, "failed to open file %s\n", ofname);
            continue;
        }
        else
        {
            printf("> Processing file:%s\n", ofname);
            __off_t offset = taosLSeekFile(pFile, sizeof(SSdbHeader) + sizeof(sdbEcommit), SEEK_SET);
            while (1)
            {
                memset(pRowHead, 0, 1024 * 1024);
                int bytes = taosReadFile(pFile, pRowHead, sizeof(SRowHead));
                if (bytes < 0)
                {
                    fprintf(stderr, "ERROR! file %s may be broken.....\n", ofname);
                    break;
                }

                if (bytes == 0)
                    break;

                if (bytes < sizeof(SRowHead) || pRowHead->delimiter != SDB_DELIMITER)
                {
                    offset = taosLSeekFile(pFile, -(bytes - 1), SEEK_CUR);
                    continue;
                }

                if (pRowHead->rowSize < 0 || pRowHead->rowSize > 1024 * 1024)
                {
                    fprintf(stderr, "ERROR! file %s may be broken.....\n", ofname);
                    break;
                }

                bytes = taosReadFile(pFile, pRowHead->data, pRowHead->rowSize + sizeof(TSCKSUM));
                if (bytes < pRowHead->rowSize + sizeof(TSCKSUM))
                {
                    fprintf(stderr, "ERROR! file %s may be broken.....\n", ofname);
                    break;
                }

                int tsize = sizeof(SRowHead) + pRowHead->rowSize + sizeof(TSCKSUM);
                if (!taosCheckChecksumWhole((uint8_t *)pRowHead, tsize))
                {
                    fprintf(stderr, "ERROR! file %s may be broken.....\n", ofname);
                    break;
                }

                (sdb_mod_vector_table[k].func)(pRowHead->data, dnodeTable, n_nodes);

                taosCalcChecksumAppend(0, (uint8_t *)pRowHead, tsize);
                taosLSeekFile(pFile, offset, SEEK_SET);
                taosWriteFile(pFile, pRowHead, tsize);

                // offset += sizeof(SRowHead)+pRowHead->rowSize+sizeof(TSCKSUM);
                offset = taosLSeekFile(pFile, 0, SEEK_CUR);
            }

            taosCloseFile(&pFile);
        }
    }

    taosMemoryFree(pRowHead);


    // =======================================  MOD LINK =======================================
__link_mod:
    printf("Start to process vnode files in rootDir:%s...\n", rootDir);

    sprintf(tsdbDir, "%s/tsdb", rootDir);
    int dsize = sprintf(dataDir, "%s/data", rootDir);

    DIR *dir = opendir(tsdbDir);
    if (dir) { // Open dir OK
        while ((dent1 = readdir(dir)) != NULL)
        {
            if (strcmp(dent1->d_name, ".") == 0 || strcmp(dent1->d_name, "..") == 0)
                continue;

            printf("> Processing directory:%s/%s...\n", tsdbDir, dent1->d_name);

            // Modify IP in file
            if (n_nodes > 0) {
                int vnode;
                SVPeerDesc vpeers[TSDB_VNODES_SUPPORT];
                sscanf(dent1->d_name, "vnode%d", &vnode);

                sprintf(ofname, "%s/%s/meterObj.v%d", tsdbDir, dent1->d_name, vnode);
                FILE *fp = fopen(ofname, "r+");
                if (fp == NULL)
                {
                    fprintf(stderr, "ERROR! Failed to open file %s\n", ofname);
                    continue;
                }
                // printf("Processing vnode: %d....\n", vnode);

                fseek(fp, TSDB_FILE_HEADER_LEN * 3 / 4, SEEK_SET);
                fread(&vpeers, sizeof(SVPeerDesc), TSDB_VNODES_SUPPORT, fp);

                // Change the IP part
                for (size_t i = 0; i < TSDB_VNODES_SUPPORT; i++)
                {
                    if (vpeers[i].ip != 0)
                    {
                        int tidx = -1;
                        for (int k = 0; k < n_nodes; k++)
                        {
                            if (dnodeTable[k].o_privateIp.ip_val == vpeers[i].ip)
                            {
                                tidx = k;
                                break;
                            }
                        }

                        if (tidx < 0)
                        {
                            fprintf(stderr, "ERROR! Invalid IP addr in vid:%d, ip:%u", vnode, vpeers[i].ip);
                            abort();
                        }
                        vpeers[i].ip = dnodeTable[tidx].privateIp.ip_val;
                    }
                }

                fseek(fp, TSDB_FILE_HEADER_LEN * 3 / 4, SEEK_SET);
                fwrite(&vpeers, sizeof(SVPeerDesc), TSDB_VNODES_SUPPORT, fp);

                fclose(fp);
            }

            sprintf(vnodeDir, "%s/%s/db", tsdbDir, dent1->d_name);
            sprintf(dataDir + dsize, "/%s", dent1->d_name);

            // Modify links
            DIR *tdir = opendir(vnodeDir);
            if (tdir)
            {
                while ((dent2 = readdir(tdir)) != NULL)
                {
                    if (strcmp(dent2->d_name, ".") == 0 || strcmp(dent2->d_name, "..") == 0)
                        continue;
                    if (strncmp(dent2->d_name + strlen(dent2->d_name) - 3, "log", 3) == 0) {
                        printf(">> Log file %s still exists in vnodeDir %s, skip\n", dent2->d_name, vnodeDir);
                        continue;
                    }
                    if (strncmp(dent2->d_name + strlen(dent2->d_name) - 2, ".t", 2) == 0) {
                        printf(">> Remove file %s in vnodeDir %s\n", dent2->d_name, vnodeDir);
                        char dropLinkName[645] = "\0";
                        sprintf(dropLinkName, "%s/%s", vnodeDir, dent2->d_name);
                        remove(dropLinkName);
                        continue;
                    }
                    if (strncmp(dent2->d_name + strlen(dent2->d_name) - 2, ".l", 2) == 0) {
                        printf(">> Remove file %s in vnodeDir %s\n", dent2->d_name, vnodeDir);
                        char dropLinkName[645] = "\0";
                        sprintf(dropLinkName, "%s/%s", vnodeDir, dent2->d_name);
                        remove(dropLinkName);
                        continue;
                    }
                    sprintf(linkName, "%s/%s", vnodeDir, dent2->d_name);

                    // TODO : check if the file is a symbolic link
                    printf(">> Processing link file %s\n", linkName);
                    ssize_t tsize = 0;
                    tsize = readlink(linkName, targetName, TSDB_FILENAME_LEN);
                    if (tsize < 0)
                    {
                        fprintf(stderr, "Failed to read link file name\n");
                        continue;
                    }
                    targetName[tsize] = '\0';
                    printf(">> Processing target file %s\n", targetName);

                    if (access(targetName, F_OK) < 0)
                    { // Fix those broken files
                        printf("Link file %s is broken, try to recover it\n", linkName);
                        char possibleTarget[TSDB_FILENAME_LEN] = "\0";
                        char possibleTarget0[TSDB_FILENAME_LEN] = "\0";
                        char possibleTarget1[TSDB_FILENAME_LEN] = "\0";
                        sprintf(possibleTarget, "%s/%s", dataDir, basename(targetName));
                        sprintf(possibleTarget0, "%s/%s0", dataDir, basename(linkName));
                        sprintf(possibleTarget1, "%s/%s1", dataDir, basename(linkName));
                        if (access(possibleTarget, F_OK) >= 0 )
                        {
                            remove(linkName);
                            symlink(possibleTarget, linkName);
                        }
                        else 
                        {
                            if (access(possibleTarget1, F_OK) >= 0)
                            {
                                remove(linkName);
                                symlink(possibleTarget1, linkName);
                            }
                            else if (access(possibleTarget0, F_OK) >= 0)
                            {
                                remove(linkName);
                                symlink(possibleTarget0, linkName);
                            }
                            else {
                                fprintf(stderr, "ERROR! possible target file %s or %s not exists!", possibleTarget0, possibleTarget1);
                                continue;
                            }
                        }
                    }
                }
                closedir(tdir);
            }
            else
            {
                fprintf(stderr, "failed to open directory %s, reason:%s. Continue!", vnodeDir, strerror(errno));
                continue;
            }
        }
        closedir(dir);
    }
    else
    {
        fprintf(stderr, "Failed to open directory %s, reason:%s. Exit!\n", tsdbDir, strerror(errno));
        exit(EXIT_FAILURE);
    }

    return 0;
}
