#!/usr/bin/env python

import os
import glob
import re
import argparse
import sys
import logging

FORMAT = '%(levelname)s - %(asctime)-15s: %(message)s'
formatter = logging.Formatter(FORMAT)
logger = logging.getLogger("create_bam_links")
logger.setLevel(logging.INFO)

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
    """
    For each sample, search for the directory that contains its bam files.
    Then return a dictionary that contains sample as the key and a list of
    the sample's project, directory path, and directory age as the value.
    """

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

    logger.info('Files and samples that were linked:')
    for project, stats in data.items():
        logger.info(project)
        logger.info('  Samples found: {}'.format(stats['samples']))
        logger.info('  Files updated: {}'.format(stats['files']))


def create_links(args, projects, final_sample_dirs):

    count_files_linked = {i: {'samples': 0, 'files': 0} for i in projects}

    for sample, info in final_sample_dirs.items():

        project = info[0]
        dir = info[1]

        files = glob.glob(os.path.join(dir, '*bam')) + \
            glob.glob(os.path.join(dir, '*bai'))

        if len(files) == 0:
            logger.warning(
                'Found no bam files for sample {}. Skipping...'.format(sample))
            continue

        # create the base direcotory to contain the links

        patient_id = '-'.join(sample.split('-')[0:2])

        # some samples have a Sample_ prefix, so remove it

        if 'Sample_' in patient_id:
            patient_id = patient_id.replace('Sample_', '')

        sample_dir = sample.replace('Sample_', '')

        basedir = os.path.join(
            os.path.abspath(args.outdir), patient_id, sample_dir, args.version)

        logger.debug('Creating directory: {}'.format(basedir))

        if not args.dryrun:
            os.makedirs(basedir, exist_ok=True)

        # loop over the files and creat the links

        count_files_linked[project]['samples'] += 1

        for fpath in files:
            dest = os.path.join(basedir, os.path.split(fpath)[1])

            if os.path.exists(dest):

                replace_old = False

                if args.replace_old:
                    # determine if there is a newer file than the one the
                    # link points to

                    old_path = os.path.realpath(dest)
                    if os.stat(old_path).st_mtime < os.stat(fpath).st_mtime:
                        replace_old = True
                        logger.info('Replacing old file with newer one.')
                        logger.info('  Old file: {}'.format(old_path))
                        logger.info('  New file: {}'.format(fpath))


                if not args.overwrite and not replace_old:
                    # if the symlink already exists, then replace it if you
                    # set the --overwrite flag

                    logger.debug('File exists: {}. Skipping...'.format(dest))
                    continue

                count_files_linked[project]['files'] += 1

                logger.debug('Replacing old symlink.')
                logger.debug('  src: {}'.format(fpath))
                logger.debug('  link: {}'.format(dest))

                if not args.dryrun:
                    if not os.path.islink(dest):
                        # just a safety measure to be sure we're removing
                        # only symlinks

                        logger.error(
                            'Cannot remove old symlink. It is not a link: {}.'.format(dest))
                        sys.exit(1)

                    os.remove(dest)
                    os.symlink(fpath, dest)
            else:
                # create a new symlink

                logger.info('Creating new symlink.')
                logger.info('  src: {}'.format(fpath))
                logger.info('  link: {}'.format(dest))

                # check if the link is pointing to a missing file
                # if it is, then remove the link

                if is_dead_link(dest):

                    link_source = os.path.realpath(dest)

                    logger.warning('Source file for a link does not exist. Removing the dead link.')
                    logger.warning('  missing src: {}'.format(link_source))
                    logger.warning('  link: {}'.format(dest))

                    if not args.dryrun:
                        os.remove(dest)

                if not args.dryrun:

                    count_files_linked[project]['files'] += 1

                    os.symlink(fpath, dest)

        # create link for latest

        if args.latest:
            basedir_latest = os.path.join(
                os.path.abspath(args.outdir), patient_id, sample_dir, 'latest')

            if not args.dryrun:

                if os.path.exists(basedir_latest) and os.path.islink(basedir_latest):
                    # just a safety measure to be sure the directory to
                    # be removed is a symlink. Want to avoid any
                    # unexpected bugs that might remove an important
                    # directory

                    logger.debug('Removing old linked directory: {}'.format(
                        basedir_latest))

                    os.unlink(basedir_latest)

                logger.debug('Creating new linked directory.')
                logger.debug('  src: {}'.format(basedir))
                logger.debug('  link: {}'.format(basedir_latest))

                os.symlink(basedir, basedir_latest)

    print_data_processed(count_files_linked)


def is_dead_link(fpath):
    if os.path.islink(fpath) and not os.path.exists(fpath):
        return True
    else:
        return False


def main():

    args = get_args()

    final_sample_dirs = {}

    if args.subparser_name == 'all':

        projects = glob.glob(os.path.join(args.runsdir, "Project_*"))
        projects = [os.path.split(i)[1] for i in projects]

        if args.exclude:
            projects = list(filter(
                lambda x: x not in args.exclude, projects))
    elif args.subparser_name == 'project':
        projects = args.projects

    for project in projects:

        path = os.path.join(args.runsdir, project, "bam_qc")

        final_sample_dirs = get_sample_dirs(
            final_sample_dirs, path, args.sampleregex, project)

    create_links(args, projects, final_sample_dirs)


def add_common_args(parser):

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
        '-ro', '--replace-old', action='store_true',
        help="Overwrite any links that point to old files.")
    parser.add_argument(
        '--debug', action='store_true',
        help="Set loging level to debug.")
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
    parser.add_argument('--log', help='File to print log to.')

    return parser


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
    subparsers = parser.add_subparsers(
        help='Creates BAM/BAI file links.',
        dest="subparser_name")

    project_parser = subparsers.add_parser(
        'project',
        help='Link BAM files for one or more specific projects.')
    project_parser.add_argument(
        '-p', '--projects', action="append", required=True,
        help='''
            Project to create bam links for (e.g. Project_10747_D).
            Can specify more than once. Required.''')
    project_parser = add_common_args(project_parser)

    all_parser = subparsers.add_parser(
        'all',
        help='''
            Link BAM files for all projects simultaneously.
            Will search for projects via 'Project_*' pattern
            in the --runsdir.''')
    all_parser.add_argument(
        '-e', '--exclude', action="append",
        help='''Exclude certain projects. Can be specified more than
        once. Optional.''')
    all_parser = add_common_args(all_parser)


    args = parser.parse_args()

    logger = logging.getLogger("create_bam_links")

    if args.debug:
        logger.setLevel(logging.DEBUG)

    if args.log:
        fh = logging.FileHandler(args.log)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    else:
        logging.basicConfig(format=FORMAT)
        logger = logging.getLogger("create_bam_links")

    return args


if __name__ == '__main__':
    main()
