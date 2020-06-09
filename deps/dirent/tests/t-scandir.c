/*
 * Make sure that scandir function works OK.
 *
 * Copyright (C) 1998-2019 Toni Ronkko
 * This file is part of dirent.  Dirent may be freely distributed
 * under the MIT license.  For all details and documentation, see
 * https://github.com/tronkko/dirent
 */

/* Silence warning about fopen being insecure */
#define _CRT_SECURE_NO_WARNINGS

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <time.h>
#include <limits.h>

#undef NDEBUG
#include <assert.h>


/* Filter and sort functions */
static int only_readme (const struct dirent *entry);
static int no_directories (const struct dirent *entry);
static int reverse_alpha (const struct dirent **a, const struct dirent **b);


int
main(
    int argc, char *argv[])
{
    struct dirent **files;
    int i;
    int n;

    (void) argc;
    (void) argv;

    /* Initialize random number generator */
    srand ((unsigned) time (NULL));

    /* Basic scan with simple filter function */
    {
        /* Read directory entries */
        n = scandir ("tests/3", &files, only_readme, alphasort);
        assert (n == 1);

        /* Make sure that the filter works */
        assert (strcmp (files[0]->d_name, "README.txt") == 0);

        /* Release file names */
        for (i = 0; i < n; i++) {
            free (files[i]);
        }
        free (files);
    }

    /* Basic scan with default sorting function */
    {
        /* Read directory entries in alphabetic order */
        n = scandir ("tests/3", &files, NULL, alphasort);
        assert (n == 13);

        /* Make sure that we got all the file names in the proper order */
        assert (strcmp (files[0]->d_name, ".") == 0);
        assert (strcmp (files[1]->d_name, "..") == 0);
        assert (strcmp (files[2]->d_name, "3zero.dat") == 0);
        assert (strcmp (files[3]->d_name, "666.dat") == 0);
        assert (strcmp (files[4]->d_name, "Qwerty-my-aunt.dat") == 0);
        assert (strcmp (files[5]->d_name, "README.txt") == 0);
        assert (strcmp (files[6]->d_name, "aaa.dat") == 0);
        assert (strcmp (files[7]->d_name, "dirent.dat") == 0);
        assert (strcmp (files[8]->d_name, "empty.dat") == 0);
        assert (strcmp (files[9]->d_name, "sane-1.12.0.dat") == 0);
        assert (strcmp (files[10]->d_name, "sane-1.2.3.dat") == 0);
        assert (strcmp (files[11]->d_name, "sane-1.2.4.dat") == 0);
        assert (strcmp (files[12]->d_name, "zebra.dat") == 0);

        /* Release file names */
        for (i = 0; i < n; i++) {
            free (files[i]);
        }
        free (files);
    }

    /* Custom filter AND sort function */
    {
        /* Read directory entries in alphabetic order */
        n = scandir ("tests/3", &files, no_directories, reverse_alpha);
        assert (n == 11);

        /* Make sure that we got all the FILE names in the REVERSE order */
        assert (strcmp (files[0]->d_name, "zebra.dat") == 0);
        assert (strcmp (files[1]->d_name, "sane-1.2.4.dat") == 0);
        assert (strcmp (files[2]->d_name, "sane-1.2.3.dat") == 0);
        assert (strcmp (files[3]->d_name, "sane-1.12.0.dat") == 0);
        assert (strcmp (files[4]->d_name, "empty.dat") == 0);
        assert (strcmp (files[5]->d_name, "dirent.dat") == 0);
        assert (strcmp (files[6]->d_name, "aaa.dat") == 0);
        assert (strcmp (files[7]->d_name, "README.txt") == 0);
        assert (strcmp (files[8]->d_name, "Qwerty-my-aunt.dat") == 0);
        assert (strcmp (files[9]->d_name, "666.dat") == 0);
        assert (strcmp (files[10]->d_name, "3zero.dat") == 0);

        /* Release file names */
        for (i = 0; i < n; i++) {
            free (files[i]);
        }
        free (files);
    }

    /* Trying to read from non-existent directory leads to an error */
    {
        files = NULL;
        n = scandir ("tests/invalid", &files, NULL, alphasort);
        assert (n == -1);
        assert (files == NULL);
        assert (errno == ENOENT);
    }

    /* Trying to open file as a directory produces ENOTDIR error */
    {
        files = NULL;
        n = scandir ("tests/3/666.dat", &files, NULL, alphasort);
        assert (n == -1);
        assert (files == NULL);
        assert (errno == ENOTDIR);
    }

    /* Scan large directory */
    {
        char dirname[PATH_MAX+1];
        int i, j;
        int ok;

        /* Copy name of temporary directory to variable dirname */
#ifdef WIN32
        i = GetTempPathA (PATH_MAX, dirname);
        assert (i > 0);
#else
        strcpy (dirname, "/tmp/");
        i = strlen (dirname);
#endif

        /* Append random characters to dirname */
        for (j = 0; j < 10; j++) {
            char c;

            /* Generate random character */
            c = "abcdefghijklmnopqrstuvwxyz"[rand() % 26];

            /* Append character to dirname */
            assert (i < PATH_MAX);
            dirname[i++] = c;
        }

        /* Terminate directory name */
        assert (i < PATH_MAX);
        dirname[i] = '\0';

        /* Create directory */
#ifdef WIN32
        ok = CreateDirectoryA (dirname, NULL);
        assert (ok);
#else
        ok = mkdir (dirname, 0700);
        assert (ok == /*success*/0);
#endif

        /* Create one thousand files */
        assert (i + 5 < PATH_MAX);
        for (j = 0; j < 1000; j++) {
            FILE *fp;

            /* Construct file name */
            dirname[i] = '/';
            dirname[i+1] = 'z';
            dirname[i+2] = '0' + ((j / 100) % 10);
            dirname[i+3] = '0' + ((j / 10) % 10);
            dirname[i+4] = '0' + (j % 10);
            dirname[i+5] = '\0';

            /* Create file */
            fp = fopen (dirname, "w");
            assert (fp != NULL);
            fclose (fp);

        }

        /* Cut out the file name part */
        dirname[i] = '\0';

        /* Scan directory */
        n = scandir (dirname, &files, no_directories, alphasort);
        assert (n == 1000);

        /* Make sure that all 1000 files are read back */
        for (j = 0; j < n; j++) {
            char match[100];

            /* Construct file name */
            match[0] = 'z';
            match[1] = '0' + ((j / 100) % 10);
            match[2] = '0' + ((j / 10) % 10);
            match[3] = '0' + (j % 10);
            match[4] = '\0';

            /* Make sure that file name matches that on the disk */
            assert (strcmp (files[j]->d_name, match) == 0);

        }

        /* Release file names */
        for (j = 0; j < n; j++) {
            free (files[j]);
        }
        free (files);
    }

    printf ("OK\n");
    return EXIT_SUCCESS;
}

/* Only pass README.txt file */
static int
only_readme (const struct dirent *entry)
{
    int pass;

    if (strcmp (entry->d_name, "README.txt") == 0) {
        pass = 1;
    } else {
        pass = 0;
    }

    return pass;
}

/* Filter out directories */
static int
no_directories (const struct dirent *entry)
{
    int pass;

    if (entry->d_type != DT_DIR) {
        pass = 1;
    } else {
        pass = 0;
    }

    return pass;
}

/* Sort in reverse direction */
static int
reverse_alpha(
    const struct dirent **a, const struct dirent **b)
{
    return strcoll ((*b)->d_name, (*a)->d_name);
}
