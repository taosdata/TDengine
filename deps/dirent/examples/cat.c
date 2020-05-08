/*
 * Output contents of a file.
 *
 * Compile this file with Visual Studio and run the produced command in
 * console with a file name argument.  For example, command
 *
 *     cat include\dirent.h
 *
 * will output the dirent.h to screen.
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

static void output_file (const char *fn);


int
main(
    int argc, char *argv[])
{
    int i;

    /* Select default locale */
    setlocale (LC_ALL, "");

    /* Require at least one file */
    if (argc == 1) {
        fprintf (stderr, "Usage: cat filename\n");
        return EXIT_FAILURE;
    }

    /* For each file name argument in command line */
    i = 1;
    while (i < argc) {
        output_file (argv[i]);
        i++;
    }
    return EXIT_SUCCESS;
}

/*
 * Output file to screen
 */
static void
output_file(
    const char *fn)
{
    FILE *fp;

    /* Open file */
    fp = fopen (fn, "r");
    if (fp != NULL) {
        size_t n;
        char buffer[4096];

        /* Output file to screen */
        do {

            /* Read some bytes from file */
            n = fread (buffer, 1, 4096, fp);

            /* Output bytes to screen */
            fwrite (buffer, 1, n, stdout);

        } while (n != 0);

        /* Close file */
        fclose (fp);

    } else {
        /* Could not open directory */
        fprintf (stderr, "Cannot open %s (%s)\n", fn, strerror (errno));
        exit (EXIT_FAILURE);
    }
}
