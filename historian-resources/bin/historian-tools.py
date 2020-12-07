import shutil, os, glob, time, math
from distutils.dir_util import copy_tree
import re, logging, argparse
import datetime, calendar
import pandas as pd
import sys, os, hashlib, sh
from multiprocessing import Pool, Process, Manager


logger = logging.getLogger('historian-tools')


def setup_logger(out_dir):
    logger.setLevel(logging.DEBUG)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    fh = logging.FileHandler(out_dir + '/historian-tool.log')
    fh.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info("------------ START -------------")




def read_csv(file):
    """Read a single csv file as a pandas dataframe
    :param file: the csv
    :return: a dataframe
    """
    try:
        # some file may be broken
        df = pd.read_csv(file, sep=';')
        return df
    except Exception as ex:
        logger.error("unable to process file " + file + " exception " +ex)
        pass


def process_csv_files(root_dir, out_dir, num_threads, do_compaction):
    """Take a root directory with subdir containing csv files and make just
    one single tgz csv file for each day

    :param root_dir:
    :return:
    """

    # 0. get all direct sub dir

    subdirs = glob.glob(root_dir + '/ISNT*/')
    subdirs_count = len(subdirs)
    logger.info("start csv compaction for {} directories in {}".format(subdirs_count, root_dir))

    for subdir in subdirs:

        #  1. get the month from the root dir
        m = re.search(r'(.*)\/ISNTS(.*)-N-(.*)-(.*)\/', subdir)
        root_path, server, year, month = m.groups()

        root_path = root_path + "/ISNTS{}-N-{}-{}".format(server, year, month)

        logger.info("root_path {}, server {}, year {}, month {}".format(root_path, server, year, month))

        # 2. group files by day
        num_days = calendar.monthrange(int(year), int(month))[1]
        days = [datetime.date(int(year), int(month), day) for day in range(1, num_days + 1)]
        for day in days:
            out_dir_day = out_dir + day.strftime("/%Y-%m-ISNTS") + server
            out_file_day = out_dir_day + "/dataHistorian-ISNTS" + server + "-N-" + day.strftime("%Y%m%d.csv.gz")
            file_pattern = root_path + "/dataHistorian-ISNTS" + server + "-N-" + day.strftime("%Y%m%d") + "*.csv"
            files = glob.glob(file_pattern)

            # check for completion
            files_count = len(files)
            is_complete = (files_count == 1440)

            if is_complete:
                completion_message = "complete data"
            else:
                completion_message = "missing data"
            logger.info("found {} files for ISNTS{}-N-{} => {}".format(files_count, server, day.strftime("%Y%m%d"), completion_message))

            # loop over files to do the compaction
            if files_count > 0 and do_compaction:

                if not os.path.exists(out_dir_day):
                    os.makedirs(out_dir_day)

                if os.path.exists(out_file_day):
                    logger.info("{} already exists, no processing for this day".format(out_file_day))
                else:
                    logger.info("found {} files for day {} with following file pattern {}".format(files_count, day,
                                                                                                  file_pattern))
                    start = time.time()
                    pool = Pool(num_threads)
                    df_collection = pool.map(read_csv, files)
                    pool.close()
                    pool.join()

                    try:
                        big_df = pd.concat(df_collection)

                        print("done in %d seconds" %
                              (time.time() - start))

                        logger.info("done creating one single big dataframe")

                        if not os.path.exists(out_file_day):
                            big_df.to_csv(out_file_day, index=False, compression='gzip')
                            logger.info("done zipping big dataframe")
                    except:
                        logger.error("unable to concatenate thoses files into a single one " + out_file_day)

            else:
                logger.info("found no files for day {} ".format(day))





def synchronize_folders(src, dst):
    copy_tree(src, dst)


def put_to_dhfs(local_file_path, hdfs_folder):

    # upload file to hdfs
    sh.hdfs( "dfs", "-put", local_file_path, hdfs_folder )


def check_csv_file_integrity(local_file_path):

    # verify that the file can be dezipped, that its a valid csv and has a full day range

    return os.EX_OK


def report(server, day):

    # count finished


def parse_command_line():
    """Take command line and parse it"""
    parser = argparse.ArgumentParser(description='Take ')
    parser.add_argument('--rootDir', help='root directory for input csv files', default='.')
    parser.add_argument('--outDir', help='output directory for zipped csv files', default='.')
    parser.add_argument('--numThreads', help='num thread to read files in parralel', default=8)
    parser.add_argument('--mode', help='report or run', default='report')
    args = parser.parse_args()
    return args



def main():
    """Main program entry"""
    args = parse_command_line()
    setup_logger(args.outDir)
    process_csv_files(args.rootDir, args.outDir, args.numThreads, False)
    logger.info("------------ END -------------")


if __name__ == "__main__":
    main()

