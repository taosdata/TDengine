/*
 * An example demonstrating recursive directory traversal.
 *
 * Compile this file with Visual Studio and run the produced command in
 * console with a directory name argument.  For example, command
 *
 *     find "C:\Program Files"
 *
 * will output thousands of file names such as
 *
 *     c:\Program Files/7-Zip/7-zip.chm
 *     c:\Program Files/7-Zip/7-zip.dll
 *     c:\Program Files/7-Zip/7z.dll
 *     c:\Program Files/Adobe/Reader 10.0/Reader/logsession.dll
 *     c:\Program Files/Adobe/Reader 10.0/Reader/LogTransport2.exe
 *     c:\Program Files/Windows NT/Accessories/wordpad.exe
 *     c:\Program Files/Windows NT/Accessories/write.wpc
 *
 * The find command provided by this file is only an example: the command does
 * not provide options to restrict the output to certain files as the Linux
 * version does.
 *
 * Copyright (C) 1998-2019 Toni Ronkko
 * This file is part of dirent.  Dirent may be freely distributed
 * under the MIT license.  For all details and documentation, see
 * https://github.com/tronkko/dirent
 */
#define _CRT_SECURE_NO_WARNINGS
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <errno.h>
#include <locale.h>

static int find_directory (const char *dirname);


int
main(
    int argc, char *argv[])
{
    int i;
    int ok;

    /* Select default locale */
    setlocale (LC_ALL, "");

    /* For each directory in command line */
    i = 1;
    while (i < argc) {
        ok = find_directory (argv[i]);
        if (!ok) {
            exit (EXIT_FAILURE);
        }
        i++;
    }

    /* List current working directory if no arguments on command line */
    if (argc == 1) {
        find_directory (".");
    }
    return EXIT_SUCCESS;
}

/* Find files and subdirectories recursively */
static int
find_directory(
    const char *dirname)
{
    DIR *dir;
    char buffer[PATH_MAX + 2];
    char *p = buffer;
    const char *src;
    char *end = &buffer[PATH_MAX];
    int ok;

    /* Copy directory name to buffer */
    src = dirname;
    while (p < end  &&  *src != '\0') {
        *p++ = *src++;
    }
    *p = '\0';

    /* Open directory stream */
    dir = opendir (dirname);
    if (dir != NULL) {
        struct dirent *ent;

        /* Print all files and directories within the directory */
        while ((ent = readdir (dir)) != NULL) {
            char *q = p;
            char c;

            /* Get final character of directory name */
            if (buffer < q) {
                c = q[-1];
            } else {
                c = ':';
            }

            /* Append directory separator if not already there */
            if (c != ':'  &&  c != '/'  &&  c != '\\') {
                *q++ = '/';
            }

            /* Append file name */
            src = ent->d_name;
            while (q < end  &&  *src != '\0') {
                *q++ = *src++;
            }
            *q = '\0';

            /* Decide what to do with the directory entry */
            switch (ent->d_type) {
            case DT_LNK:
            case DT_REG:
                /* Output file name with directory */
                printf ("%s\n", buffer);
                break;

            case DT_DIR:
                /* Scan sub-directory recursively */
                if (strcmp (ent->d_name, ".") != 0
                        &&  strcmp (ent->d_name, "..") != 0) {
                    find_directory (buffer);
                }
                break;

            default:
                /* Ignore device entries */
                /*NOP*/;
            }

        }

        closedir (dir);
        ok = 1;

    } else {
        /* Could not open directory */
        fprintf (stderr, "Cannot open %s (%s)\n", dirname, strerror (errno));
        ok = 0;
    }

    return ok;
}
