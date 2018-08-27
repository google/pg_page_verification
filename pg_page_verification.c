/* Copyright 2018 Google LLC

 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/postgresql
*/

/*
 * Utility to verify a postgres base directory.
 *
 * This utility scans the 'base' data directory of a postgres instance to find
 * directories and segment files.  Each segment file is then checked for any
 * pages that may have invalid checksums.  Invalid checksums are determined by
 * comparing the stored checksum against the current checksum.
 *
 * In order to use this utility, it is necessary to enable checksums with
 * initdb.  The flag for initdb is either -k or --data-checksums.  Without,
 * enabling checksums, it is not possible to use this utility.
 *
 */

/* standard header files */
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <getopt.h>

/* headers for directory scanning */
#include <string.h>
#include <sys/stat.h>
#include <dirent.h>
#include <stddef.h>

/* postgres specific header files */
#include "c.h"
#include "pg_config.h"
#include "storage/checksum_impl.h"
#include "storage/bufpage.h"

#define MAX_DIR_LENGTH 256

/* global flag values */
int verbose = 0;
int dump_corrupted = 0;

static unsigned int
parse_segment_number(const char* filename)
{
    /* Filename here should be the segment number
     *   example: /path/to/data/base/[oid]/[segmentnumber]
     */

    int segmentnumber = strlen(filename) - 1;

    while(isdigit(filename[segmentnumber])) {
        segmentnumber--;
        if(segmentnumber < 0)
            return 0;
    } 

    return atoi(&filename[segmentnumber + 1]);
}


static bool
is_page_corrupted(const char *page, BlockNumber blkno, const char *filename,
    const char *dirpath)
{
    /* Function checks a page header checksum value aginst the current
     * checksum value of a page.  NewPage checksums will be zero until they
     * are set.  There is a similar function PageIsVerified responsible for
     * checking pages before they are loaded into buffer pool.
     *  see:  src/backend/storage/page/bufpage.c
     *
     * Consider returning a negative value if page is new or checksum unset
     * or if more detail for a page verifiction can be found.
     */

    PageHeader phdr = (PageHeader)page;

    /* calculating blkno needs to be aboslute so that subsequent segment files
    *  have the blkno calculated based on all segment files and not relative to
    *  the current segment file. see: https://goo.gl/qRTn46
    */

    /* Segment size in bytes, BLCKSZ is 8192 by default, 8KB pages
    *  1GB segment files are 131072 blocks of 8KB page size
    *  NOTE: pd_pagesize_version is BLCKSZ + version, since 8.3+, version is 4, 
    *   resulting in pd_pagesize_version being 8196 when pagesize is 8KB
    */
    static unsigned int segmentSize = RELSEG_SIZE * BLCKSZ;

    /* Number of current segment */
    static unsigned int segmentNumber = 0;

    segmentNumber = parse_segment_number(filename);

    /* segmentBlockOffset is the absolute blockNumber of the block when taking
     * into account any previous segment files.
    */
    uint32 segmentBlockOffset = RELSEG_SIZE * segmentNumber;
    
    uint16 checksum = pg_checksum_page((char *)page, segmentBlockOffset + blkno);

    bool corrupted = false;

    if (verbose)
    {
        printf("FILENAME: %s\n", filename);

        printf("DEBUG: filename: %s/%s[%d]\n \
            \tsegmentBlockOffset: %d, maxSegmentSize: %d,\n \
            \tsegmentNumber: %d, relative blkno: %d, absolute blkno: %d,\n \
            \tchecksum: %x, phdr->pd_checksum: %x,\n \
            \tphdr->pd_flags: %d, phdr->pd_lower: %d, phdr->pd_upper: %d,\n \
            \tphdr->pd_special: %d, phdr->pd_pagesize_version: %d,\n \
            \tphdr->pd_prune_xid: %d\n",
            dirpath, filename, blkno,
            segmentBlockOffset, segmentSize,
            segmentNumber, blkno, segmentBlockOffset + blkno,
            checksum, phdr->pd_checksum,
            phdr->pd_flags, phdr->pd_lower, phdr->pd_upper,
            phdr->pd_special, phdr->pd_pagesize_version,
            phdr->pd_prune_xid );
    }

    if (phdr->pd_checksum != 0 && phdr->pd_checksum != checksum)
    {
        corrupted = true;
        if (verbose)
            printf("ERROR: corruption found in %s/%s[%d], expected %x, found %x\n",
                dirpath, filename, blkno, checksum, phdr->pd_checksum);
    }

    if (verbose)
        printf("DEBUG: is_page_corrupted for %s/%s[%d] returns: %d\n",
                dirpath, filename, blkno, corrupted);

    return corrupted;
}

static uint32
scan_segmentfile(const char *filename, const char *dirpath)
{

    /* Performance considerations:
     * segment files can be up to 1GB in size before they are split
     * https://www.postgresql.org/docs/9.6/static/storage-file-layout.html
     */

    /* Always skip checking pg_internal.init because always shows as
     * corrupted.  If this file ever becomes corrupted, OK to remove
     * it as it is recreated upon server startup. Return false since
     * zero corrupt pages are checked here.
     */
    if (strstr(filename, "pg_internal.init") != NULL)
        return 0;

    if (verbose)
        printf("DEBUG: scanning segment filename: %s/%s\n",
            dirpath, filename);

    int fd, n;
    char page[BLCKSZ];
    BlockNumber blkno = 0;
    BlockNumber corrupted = 0;

    fd = open(filename, O_RDONLY);
    if (fd < 0)
    {
        fprintf(stderr, "ERROR: %s: %s cannot be opened\n", strerror(errno), filename);
        /* return 1 so that other segment files can be scanned, but that this
         * segment file is marked as corrupted/some unknown error
         */
        return 1;
    }

    while ((n = read(fd, page, BLCKSZ)) == BLCKSZ)
    {
        if (is_page_corrupted(page, blkno, filename, dirpath))
        {
            corrupted++;
        }
        blkno++;
    }
    if (n > 0)
    {
        /* treat short reads as corrupted pages */
        corrupted++;
    }
    close(fd);

    return corrupted;
}

static uint32
scan_directory(const char *dirpath)
{
    /* scope is only to check the base directory, where the actual data files
     * are located.  Other directories contain temporary files used for
     * transactions or queries in progress and should not be checked.
     *
     * Postgres stores data files in one directory per database defined,
     * without additional nesting or leafs.  This causes depth of database
     * directories to always be one.
     */

    DIR *d;
    struct dirent *dir;
    struct stat statbuf;
    uint32 corrupt_pages_found = 0;

    d = opendir(dirpath);

    if (verbose)
        printf("DEBUG: called scan_directory(%s)\n", dirpath);

    if (d)
    {
        chdir(dirpath);
        while ((dir = readdir(d)) != NULL)
        {
            lstat(dir->d_name, &statbuf);

            if (verbose)
                printf("DEBUG: direntry: %s/%s - statbuf.st_mode: %d\n",
                    dirpath, dir->d_name, statbuf.st_mode);

            if (S_ISDIR(statbuf.st_mode))
            {
                if(strcmp(".", dir->d_name) == 0 ||
                    strcmp("..", dir->d_name) == 0)
                    continue;

                char new_dirpath[MAX_DIR_LENGTH];
                snprintf(new_dirpath, MAX_DIR_LENGTH, "%s/%s", dirpath, dir->d_name);

                corrupt_pages_found += scan_directory(new_dirpath);
            }
            else if (S_ISREG(statbuf.st_mode))
            {
                corrupt_pages_found += scan_segmentfile(dir->d_name, dirpath);
            }
        }
        closedir(d);
    }

    return corrupt_pages_found;
}

static void
print_help(const char *argv_value)
{
    printf("Usage: %s [OPTIONS]\n", argv_value);
    printf("  -v                        verbose\n");
    printf("  -D directory              data directory\n");
    printf("  -h, --help                print this help and exit\n");
    printf("\n");
}

int
main(int argc, char *argv[])
{
    /*
     * Flow is to use a data directory and traverse to find segments, each
     * segment file is then scanned for corrupt pages.
     */

    int c;
    uint32 corrupted_pages_found = 0;
    const char *short_opt = "chD:v";
    char datadir[MAX_DIR_LENGTH];
    struct stat statbuf;
    struct option long_opt[] =
    {
        {"dumpcorrupted", no_argument,       NULL, 'c'},
        {"datadir",       required_argument, NULL, 'D'},
        {"help",          no_argument,       NULL, 'h'},
        {"verbose",       no_argument,       NULL, 'v'},
        {NULL,            0,                 NULL, 0  }
    };

    /* if no arguments passed, print help and exit(1) */
    if (argc == 1)
    {
        print_help(argv[0]);
        exit(1);
    }

    while((c = getopt_long(argc, argv, short_opt, long_opt, NULL)) != -1)
    {
        switch(c)
        {
            case -1:       /* no more arguments */
            case 0:        /* long options toggles */
                break;

            case 'c':
                dump_corrupted = 1;
                break;

            case 'v':
                verbose = 1;
                break;

            case 'D':
                if (optarg)
                {
                    /* '-D' argument of data directory must contain 'base'
                     * directory to scan, check if trailing back slash and
                     * append appropriately.  Must use base since other
                     * directories, such as pg_xlog, contain currently
                     * unsupported file types.
                     */
                    strcpy(datadir, optarg);
                    if (strcmp(&optarg[strlen(optarg) - 1], "/") == 0)
                    {
                        strcat(datadir, "base");
                    }
                    else
                    {
                        strcat(datadir, "/base");
                    }
                }
                else
                {
                    fprintf(stderr, "ERROR: -D argument could not be parsed\n");
                    exit(1);
                }
                break;

            case 'h':
                print_help(argv[0]);
                exit(1);

            default:
                fprintf(stderr, "ERROR: %s: invalid option -- %c\n", argv[0], c);
                print_help(argv[0]);
                exit(1);
        };
    };

    lstat(datadir, &statbuf);
    if (!S_ISDIR(statbuf.st_mode))
    {
        fprintf(stderr, "ERROR: base %s is not a directory\n", datadir);
        exit(1);
    }

    corrupted_pages_found = scan_directory(datadir);

    if (corrupted_pages_found > 0)
    {
        printf("CORRUPTION FOUND: %d\n", corrupted_pages_found);
        exit(1);
    }
    else
    {
        printf("NO CORRUPTION FOUND\n");
        exit(0);
    }
}
