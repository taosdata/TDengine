/*
 * An example demonstrating wide-character functions
 *
 * Compile this file with Visual Studio and run the produced command in
 * console with a directory name argument.  For example, command
 *
 *     updatedb C:\
 *
 * will produce the file locate.db with one file name per line such as
 *
 *     c:\Program Files/7-Zip/7-zip.chm
 *     c:\Program Files/7-Zip/7-zip.dll
 *     c:\Program Files/7-Zip/7z.dll
 *     c:\Program Files/Adobe/Reader 10.0/Reader/logsession.dll
 *     c:\Program Files/Adobe/Reader 10.0/Reader/LogTransport2.exe
 *     c:\Program Files/Windows NT/Accessories/wordpad.exe
 *     c:\Program Files/Windows NT/Accessories/write.wpc
 *
 * Be ware that this file uses wide-character API which is not compatible
 * with Linux or other major Unixes.
 *
 * Copyright (C) 1998-2019 Toni Ronkko
 * This file is part of dirent.  Dirent may be freely distributed
 * under the MIT license.  For all details and documentation, see
 * https://github.com/tronkko/dirent
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <wchar.h>
#ifdef WIN32
#   include <io.h>
#   include <fcntl.h>
#endif
#include <dirent.h>

/* File name and location of database file */
#define DB_LOCATION L"locate.db"


/* Forward-decl */
static int update_directory (const wchar_t *dirname);
static void db_open (void);
static void db_close (void);
static void db_store (const wchar_t *dirname);


/* Module local variables */
static FILE *db = NULL;


int
main(
    int argc, char *argv[])
{
#ifdef WIN32
    int i;
    int ok;

    /* Prepare for unicode output */
    _setmode (_fileno (stdout), _O_U16TEXT);

    /* Open locate.db */
    db_open ();

    /* For each directory in command line */
    i = 1;
    while (i < argc) {
        wchar_t buffer[PATH_MAX + 1];
        errno_t error;
        size_t n;

        /* Convert ith argument to wide-character string */
        error = mbstowcs_s (&n, buffer, PATH_MAX, argv[i], _TRUNCATE);
        if (!error) {

            /* Scan directory for files */
            ok = update_directory (buffer);
            if (!ok) {
                wprintf (L"Cannot open directory %s\n", buffer);
                exit (EXIT_FAILURE);
            }

        }

        i++;
    }

    /* Use current working directory if no arguments on command line */
    if (argc == 1) {
        update_directory (L".");
    }

    db_close ();
#else
    printf ("updatedb only works on Microsoft Windows\n");
#endif

    return EXIT_SUCCESS;
}

/* Find files recursively */
static int
update_directory(
    const wchar_t *dirname)
{
    int ok = 0;

#ifdef WIN32
    _WDIR *dir;
    wchar_t buffer[PATH_MAX + 2];
    wchar_t *p = buffer;
    const wchar_t *src;
    wchar_t *end = &buffer[PATH_MAX];

    /* Copy directory name to buffer */
    src = dirname;
    while (p < end  &&  *src != '\0') {
        *p++ = *src++;
    }
    *p = '\0';

    /* Open directory stream */
    dir = _wopendir (dirname);
    if (dir != NULL) {
        struct _wdirent *ent;

        /* Print all files and directories within the directory */
        while ((ent = _wreaddir (dir)) != NULL) {
            wchar_t *q = p;
            wchar_t c;

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
            case DT_REG:
                /* Store file name */
                db_store (buffer);
                break;

            case DT_DIR:
                /* Scan sub-directory recursively */
                if (wcscmp (ent->d_name, L".") != 0
                        &&  wcscmp (ent->d_name, L"..") != 0) {
                    update_directory (buffer);
                }
                break;

            default:
                /* Do not device entries */
                /*NOP*/;
            }

        }

        wclosedir (dir);
        ok = 1;

    } else {

        /* Cannot open directory */
        ok = 0;

    }
#endif

    return ok;
}

/* Store file name to locate.db */
static void
db_store(
    const wchar_t *dirname)
{
#ifdef WIN32
    if (db) {
        /* Output line to file */
        fwprintf (db, L"%s\n", dirname);
    } else {
        wprintf (L"Database not open\n");
        exit (EXIT_FAILURE);
    }
#endif
}

/* Open database file locate.db */
static void
db_open(
    void)
{
#ifdef WIN32
    if (db == NULL) {
        errno_t error;

        /* Open file for writing */
        error = _wfopen_s (&db, DB_LOCATION, L"wt, ccs=UNICODE");
        if (error) {
            wprintf (L"Cannot open %s\n", DB_LOCATION);
            exit (EXIT_FAILURE);
        }
    }
#endif
}

/* Close database file */
static void
db_close(
    void)
{
    if (db) {
        fclose (db);
        db = NULL;
    }
}
