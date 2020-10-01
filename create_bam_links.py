#!/usr/bin/env python

import os
import glob
import re
import argparse

FOLDERS_EXCLUDE = [
    'QC_Results', 'log', 'picard_metrics', 'Joint_QC', 'tmp',
    'bams', 'duplex', 'simplex', '_results']


def add_sample_dirs_to_dict(sample_dirs, dirs, project):
    """
    given a set of absolute paths to sample directories, convert it into
    a dictionary so we know which sample corresponds to which directory.
    also get the directory creation date and get the most recent one.
    """

    for d in dirs:
        sample_name = os.path.split(d)[1]
        dir_age = os.stat(d).st_mtime

        if sample_name in sample_dirs:
            if sample_dirs[sample_name][2] < dir_age:
                sample_dirs[sample_name] = [project, d, dir_age]
        else:
            sample_dirs[sample_name] = [project, d, dir_age]

    return sample_dirs


def filter_dirs(dirs):
    """
    remove any dirs that we know do not contain any sample dirs
    """

    dirs = list(filter(
        lambda x: not any([x.endswith(i) for i in FOLDERS_EXCLUDE]), dirs))

    return dirs


def list_dir(path):
    """
    list contents of a directory and return on directories
    """

    dirs = glob.glob(os.path.join(path, "*"))

    dirs = list(filter(lambda x: os.path.isdir(x), dirs))
    dirs = filter_dirs(dirs)

    return dirs


def list_sample_dirs(dirs, sampleregex):

    return list(filter(lambda x: re.search(sampleregex, x), dirs))


def get_sample_dirs(final_sample_dirs, path, sampleregex, project):

    dirs = list_dir(path)

    # if there is a current dir, then look only in that

    if any([i.endswith('current') for i in dirs]):
        dirs = list_dir(os.path.join(path, 'current'))

    # look within the first level of the directory for any sample dirs

    sample_dirs = list_sample_dirs(dirs, sampleregex)
    final_sample_dirs = add_sample_dirs_to_dict(
        final_sample_dirs, sample_dirs, project)

    if dirs:
        # look within the second level of the directory for any sample dirs
        # this will overwrite any sample dirs previously found

        dirs = list(filter(lambda x: re.search(sampleregex, x) is None, dirs))
        dirs = [list_dir(x) for x in dirs]
        dirs = [item for sublist in dirs for item in sublist]

        sample_dirs = list_sample_dirs(dirs, sampleregex)
        final_sample_dirs = add_sample_dirs_to_dict(
            final_sample_dirs, sample_dirs, project)

    if dirs:
        # look within the third level of the directory for any sample dirs
        # this will overwrite any sample dirs previously found

        dirs = list(filter(lambda x: re.search(sampleregex, x) is None, dirs))
        dirs = [list_dir(x) for x in dirs]
        dirs = [item for sublist in dirs for item in sublist]

        sample_dirs = list_sample_dirs(dirs, sampleregex)
        final_sample_dirs = add_sample_dirs_to_dict(
            final_sample_dirs, sample_dirs, project)

    return final_sample_dirs


def print_data_processed(data):

    print('\n\nFiles and samples that were linked')
    print('{}          {}   {}'.format('Project', 'Samples', 'Files'))
    for project, stats in data.items():
        print('{}  {}        {}'.format(project, stats['samples'], stats['files']))


def create_links(args, final_sample_dirs):

    count_files_linked = {i: {'samples': 0,'files': 0} for i in args.projects}

    for sample, info in final_sample_dirs.items():

        project = info[0]
        dir = info[1]

        files = glob.glob(os.path.join(dir, '*bam')) + \
            glob.glob(os.path.join(dir, '*bai'))

        if len(files) == 0:
            print('Found no bam files for sample {}. Skipping...'.format(
                sample))
            continue

        # create the base direcotory to contain the links

        patient_id = '-'.join(sample.split('-')[0:2])

        basedir = os.path.join(
            os.path.abspath(args.outdir), patient_id, sample, args.version)

        if args.dryrun or args.verbose:
            print('\nCreating directory: {}'.format(basedir))

        if not args.dryrun:
            os.makedirs(basedir, exist_ok=True)

        # loop over the files and creat the links

        count_files_linked[project]['samples'] += 1

        for fpath in files:
            dest = os.path.join(basedir, os.path.split(fpath)[1])

            if not args.overwrite and os.path.exists(dest):
                print('File exists: {}'.format(
                    dest))
                continue

            count_files_linked[project]['files'] += 1

            if args.dryrun or args.verbose:
                print('New symlink\n\tsrc: {}\n\tlink: {}'.format(fpath, dest))

            if not args.dryrun:
                os.symlink(fpath, dest)

        # create link for latest

        if args.latest:
            basedir_latest = os.path.join(
                os.path.abspath(args.outdir), patient_id, sample, 'latest')

            if args.dryrun or args.verbose:
                print('Removing any existing latest directory')
                print('New symlink\n\tsrc: {}\n\tlink: {}'.format(
                    basedir, basedir_latest))

            if not args.dryrun:

                if os.path.exists(basedir_latest) and os.path.islink(basedir_latest):
                    # just a safety measure to be sure the directory to
                    # be removed is a symlink. Want to avoid any
                    # unexpected bugs that might remove an important
                    # directory

                    os.remove(basedir_latest)

                os.symlink(basedir, basedir_latest)

    print_data_processed(count_files_linked)


def main():

    args = get_args()

    final_sample_dirs = {}

    for project in args.projects:

        path = os.path.join(args.runsdir, project, "bam_qc")

        final_sample_dirs = get_sample_dirs(
            final_sample_dirs, path, args.sampleregex, project)

    create_links(args, final_sample_dirs)


def get_args():

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='''
            Creates soft links to the bam files in the runs directory.
            Give it a list of projects to search, and it will search
            down three directory levels within the bam_qc directory. It
            will prioritize the \"current\" directory within the bam_qc folder.
            If it finds the same sample directory twice then it will take
            the most recent one.
            ''')
    parser.add_argument(
        '-p', '--projects', action="append", required=True,
        help='''
            Project to create bam links for (e.g. Project_10747_D).
            Can specify more than once. Required.''')
    parser.add_argument(
        '-v', '--version', required=True,
        help="Version of the BAM files (e.g. V1). Required.")
    parser.add_argument(
        '-o', '--outdir', default='/work/access/production/data/bams/',
        help='Base directory to create the linked files in.')
    parser.add_argument(
        '-r', '--runsdir', help='Directory containing the projects.',
        default='/work/access/production/runs/')
    parser.add_argument(
        '-d', '--dryrun', action='store_true',
        help="Print the links and directories to be created, but don't execute them.")
    parser.add_argument(
        '-ov', '--overwrite', action='store_true',
        help="Overwrite any links that already exist.")
    parser.add_argument(
        '-vb', '--verbose', action='store_true',
        help="Print all logging info to stdout.")
    parser.add_argument(
        '-l', '--latest', action='store_true',
        help='''
            The version you specify will be considered the latest version.
            (e.g. a directory called latest will point to V1 if you
            set --version V1).''')
    parser.add_argument(
        '-sr', '--sampleregex', default="C-(.*)-(L|N)(\d*)-d",
        help='''
            The regex pattern for finding the sample directories within
            the project folder that contain the original bam files.''')

    args = parser.parse_args()

    return args


if __name__ == '__main__':
    main()
