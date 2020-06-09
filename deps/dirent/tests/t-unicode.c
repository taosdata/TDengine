/*
 * Test program to try unicode file names.
 *
 * Copyright (C) 1998-2019 Toni Ronkko
 * This file is part of dirent.  Dirent may be freely distributed
 * under the MIT license.  For all details and documentation, see
 * https://github.com/tronkko/dirent
 */

/* Silence warning about fopen being insecure */
#define _CRT_SECURE_NO_WARNINGS

#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <wchar.h>
#include <time.h>
#include <locale.h>

#undef NDEBUG
#include <assert.h>


int
main(
    int argc, char *argv[])
{
#ifdef WIN32
    wchar_t wpath[MAX_PATH+1];
    char path[MAX_PATH+1];
    DWORD i, j, k, x;
    BOOL ok;
    HANDLE fh;
    _WDIR *wdir;
    struct _wdirent *wentry;
    DIR *dir;
    struct dirent *entry;
    char buffer[100];
    FILE *fp;
    int counter = 0;

    (void) argc;
    (void) argv;


    /* Initialize random number generator */
    srand (((int) time (NULL)) * 257 + ((int) GetCurrentProcessId ()));

    /* Set current locale */
    if (argc > 1) {
        printf ("Locale %s\n", argv[1]); 
        setlocale (LC_ALL, argv[1]);
    } else {
        setlocale (LC_ALL, "");
    }


    /****** CREATE FILE WITH UNICODE FILE NAME ******/

    /* Get path to temporary directory (wide-character and ascii) */
    i = GetTempPathW (MAX_PATH, wpath);
    assert (i > 0);
    j = GetTempPathA (MAX_PATH, path);
    assert (j > 0);

    /* Append random directory name */
    for (k = 0; k < 10; k++) {
        char c;

        /* Generate random character */
        c = "abcdefghijklmnopqrstuvwxyz"[rand() % 26];

        /* Append character to paths */
        assert (i < MAX_PATH  &&  j < MAX_PATH);
        wpath[i++] = c;
        path[j++] = c;
    }

    /* Terminate paths */
    assert (i < MAX_PATH  &&  j < MAX_PATH);
    wpath[i] = '\0';
    path[j] = '\0';

    /* Remember the end of directory name */
    k = i;

    /* Create directory using unicode */
    ok = CreateDirectoryW (wpath, NULL);
    if (!ok) {
        DWORD e = GetLastError ();
        wprintf (L"Cannot create directory %ls (code %u)\n", wpath, e);
        abort ();
    }

    /* Overwrite zero terminator with path separator */
    assert (i < MAX_PATH  &&  j < MAX_PATH);
    wpath[i++] = '\\';

    /* Append a few unicode characters */
    assert (i < MAX_PATH);
    wpath[i++] = 0x6d4b;
    assert (i < MAX_PATH);
    wpath[i++] = 0x8bd5;

    /* Terminate string */
    assert (i < MAX_PATH);
    wpath[i] = '\0';

    /* Create file with unicode */
    fh = CreateFileW(
        wpath,
        /* Access */ GENERIC_READ | GENERIC_WRITE,
        /* Share mode */ 0,
        /* Security attributes */ NULL,
        /* Creation disposition */ CREATE_NEW,
        /* Attributes */ FILE_ATTRIBUTE_NORMAL,
        /* Template files */ NULL
    );
    assert (fh != INVALID_HANDLE_VALUE);

    /* Write some data to file */
    ok = WriteFile(
        /* File handle */ fh,
        /* Pointer to data */ "hep\n",
        /* Number of bytes to write */ 4,
        /* Number of bytes written */ NULL,
        /* Overlapped */ NULL
    );
    assert (ok);

    /* Close file */
    ok = CloseHandle (fh);
    assert (ok);


    /****** MAKE SURE THAT UNICODE FILE NAME CAN BE READ BY _WREADDIR ******/

    /* Zero terminate wide-character path and open directory stream */
    wpath[k] = '\0';
    wdir = _wopendir (wpath);
    if (wdir == NULL) {
        wprintf (L"Cannot open directory %ls\n", wpath);
        abort ();
    }

    /* Read through entries */
    counter = 0;
    while ((wentry = _wreaddir (wdir)) != NULL) {

        /* Skip pseudo directories */
        if (wcscmp (wentry->d_name, L".") == 0) {
            continue;
        }
        if (wcscmp (wentry->d_name, L"..") == 0) {
            continue;
        }

        /* Found a file */
        counter++;
        assert (wentry->d_type == DT_REG);

        /* Append file name to path */
        i = k;
        assert (i < MAX_PATH);
        wpath[i++] = '\\';
        x = 0;
        while (wentry->d_name[x] != '\0') {
            assert (i < MAX_PATH);
            wpath[i++] = wentry->d_name[x++];
        }
        assert (i < MAX_PATH);
        wpath[i] = '\0';

        /* Open file for read */
        fh = CreateFileW(
            wpath,
            /* Access */ GENERIC_READ,
            /* Share mode */ 0,
            /* Security attributes */ NULL,
            /* Creation disposition */ OPEN_EXISTING,
            /* Attributes */ FILE_ATTRIBUTE_NORMAL,
            /* Template files */ NULL
        );
        assert (fh != INVALID_HANDLE_VALUE);

        /* Read data from file */
        ok = ReadFile(
            /* File handle */ fh,
            /* Output buffer */ buffer,
            /* Maximum number of bytes to read */ sizeof (buffer) - 1,
            /* Number of bytes actually read */ &x,
            /* Overlapped */ NULL
        );
        assert (ok);

        /* Make sure that we got the file contents right */
        assert (x == 4);
        assert (buffer[0] == 'h');
        assert (buffer[1] == 'e');
        assert (buffer[2] == 'p');
        assert (buffer[3] == '\n');

        /* Close file */
        ok = CloseHandle (fh);
        assert (ok);

    }
    assert (counter == 1);

    /* Close directory */
    _wclosedir (wdir);


    /****** MAKE SURE THAT UNICODE FILE NAME CAN BE READ BY READDIR ******/

    /* Zero terminate ascii path and open directory stream */
    k = j;
    path[k] = '\0';
    dir = opendir (path);
    if (dir == NULL) {
        fprintf (stderr, "Cannot open directory %s\n", path);
        abort ();
    }

    /* Read through entries */
    counter = 0;
    while ((entry = readdir (dir)) != NULL) {

        /* Skip pseudo directories */
        if (strcmp (entry->d_name, ".") == 0) {
            continue;
        }
        if (strcmp (entry->d_name, "..") == 0) {
            continue;
        }

        /* Found a file */
        counter++;
        assert (entry->d_type == DT_REG);

        /* Append file name to path */
        j = k;
        assert (j < MAX_PATH);
        path[j++] = '\\';
        x = 0;
        while (entry->d_name[x] != '\0') {
            assert (j < MAX_PATH);
            path[j++] = entry->d_name[x++];
        }
        assert (j < MAX_PATH);
        path[j] = '\0';

        /* Open file for read */
        fp = fopen (path, "r");
        if (!fp) {
            fprintf (stderr, "Cannot open file %s\n", path);
            abort ();
        }

        /* Read data from file */
        if (fgets (buffer, sizeof (buffer), fp) == NULL) {
            fprintf (stderr, "Cannot read file %s\n", path);
            abort ();
        }

        /* Make sure that we got the file contents right */
        assert (buffer[0] == 'h');
        assert (buffer[1] == 'e');
        assert (buffer[2] == 'p');
        assert (buffer[3] == '\n');
        assert (buffer[4] == '\0');

        /* Close file */
        fclose (fp);

    }
    assert (counter == 1);

    /* Close directory */
    closedir (dir);


    /****** CREATE FILE WITH UTF-8 ******/

    /* Append UTF-8 file name (åäö.txt) to path */
    j = k;
    path[j++] = '\\';
    path[j++] = 0xc3;
    path[j++] = 0xa5;
    path[j++] = 0xc3;
    path[j++] = 0xa4;
    path[j++] = 0xc3;
    path[j++] = 0xb6;
    path[j++] = 0x2e;
    path[j++] = 0x74;
    path[j++] = 0x78;
    path[j++] = 0x74;
    assert (j < MAX_PATH);
    path[j] = '\0';

    /*
     * Create file.
     *
     * Be ware that the code below creates a different file depending on the
     * current locale!  For example, if the current locale is
     * english_us.65001, then the file name will be "åäö.txt" (7 characters).
     * However, if the current locale is english_us.1252, then the file name
     * will be "ÃċÃĊÃ¶.txt" (10 characters).
     */
    printf ("Creating %s\n", path);
    fp = fopen (path, "w");
    if (!fp) {
        fprintf (stderr, "Cannot open file %s\n", path);
        abort ();
    }
    fputs ("hep\n", fp);
    fclose (fp);

    /* Open directory again */
    path[k] = '\0';
    dir = opendir (path);
    if (dir == NULL) {
        fprintf (stderr, "Cannot open directory %s\n", path);
        abort ();
    }

    /* Read through entries */
    counter = 0;
    while ((entry = readdir (dir)) != NULL) {

        /* Skip pseudo directories */
        if (strcmp (entry->d_name, ".") == 0) {
            continue;
        }
        if (strcmp (entry->d_name, "..") == 0) {
            continue;
        }

        /* Found a file */
        counter++;
        assert (entry->d_type == DT_REG);

        /* Append file name to path */
        j = k;
        assert (j < MAX_PATH);
        path[j++] = '\\';
        x = 0;
        while (entry->d_name[x] != '\0') {
            assert (j < MAX_PATH);
            path[j++] = entry->d_name[x++];
        }
        assert (j < MAX_PATH);
        path[j] = '\0';

        /* Print file name for debugging */
        printf ("Opening \"%s\" hex ", path + k + 1);
        x = 0;
        while (entry->d_name[x] != '\0') {
            printf ("0x%02x ", (unsigned) (entry->d_name[x++] & 0xff));
        }
        printf ("\n");

        /* Open file for read */
        fp = fopen (path, "r");
        if (!fp) {
            fprintf (stderr, "Cannot open file %s\n", path);
            abort ();
        }

        /* Read data from file */
        if (fgets (buffer, sizeof (buffer), fp) == NULL) {
            fprintf (stderr, "Cannot read file %s\n", path);
            abort ();
        }

        /* Make sure that we got the file contents right */
        assert (buffer[0] == 'h');
        assert (buffer[1] == 'e');
        assert (buffer[2] == 'p');
        assert (buffer[3] == '\n');
        assert (buffer[4] == '\0');

        /* Close file */
        fclose (fp);

    }
    assert (counter == 2);

    /* Close directory */
    closedir (dir);

#else

    /* Linux */
    (void) argc;
    (void) argv;

#endif
    return EXIT_SUCCESS;
}
