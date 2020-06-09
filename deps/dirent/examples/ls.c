/*
 * An example demonstrating basic directory listing.
 *
 * Compile this file with Visual Studio and run the produced command in
 * console with a directory name argument.  For example, command
 *
 *     ls "c:\Program Files"
 *
 * might output something like
 *
 *     ./
 *     ../
 *     7-Zip/
 *     Internet Explorer/
 *     Microsoft Visual Studio 9.0/
 *     Microsoft.NET/
 *     Mozilla Firefox/
 *
 * The ls command provided by this file is only an example: the command does
 * not have any fancy options like "ls -al" in Linux and the command does not
 * support file name matching like "ls *.c".
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

static void list_directory (const char *dirname);


int
main(
    int argc, char *argv[])
{
    int i;

    /* Select default locale */
    setlocale (LC_ALL, "");

    /* For each directory in command line */
    i = 1;
    while (i < argc) {
        list_directory (argv[i]);
        i++;
    }

    /* List current working directory if no arguments on command line */
    if (argc == 1) {
        list_directory (".");
    }
    return EXIT_SUCCESS;
}

/*
 * List files and directories within a directory.
 */
static void
list_directory(
    const char *dirname)
{
    DIR *dir;
    struct dirent *ent;

    /* Open directory stream */
    dir = opendir (dirname);
    if (dir != NULL) {

        /* Print all files and directories within the directory */
        while ((ent = readdir (dir)) != NULL) {
            switch (ent->d_type) {
            case DT_REG:
                printf ("%s\n", ent->d_name);
                break;

            case DT_DIR:
                printf ("%s/\n", ent->d_name);
                break;

            case DT_LNK:
                printf ("%s@\n", ent->d_name);
                break;

            default:
                printf ("%s*\n", ent->d_name);
            }
        }

        closedir (dir);

    } else {
        /* Could not open directory */
        fprintf (stderr, "Cannot open %s (%s)\n", dirname, strerror (errno));
        exit (EXIT_FAILURE);
    }
}
