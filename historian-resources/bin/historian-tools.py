import shutil, os, glob, time, math
from distutils.dir_util import copy_tree
import re, logging, argparse
import datetime, calendar
import pandas as pd
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


# create the env
# conda create --name historian pandas


def synchronize_folders(src, dst):
    copy_tree(src, dst)


def read_csv(file):
    try:
        # some file may be broken
        df = pd.read_csv(file, sep=';')
        return df
    except OSError as e:
        print(e)
        pass


def compact_csv_files(root_dir, out_dir):
    """

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

        logger.info("root_path {}, server {}, year {}, month {}".format( root_path, server, year, month))

        # 2. group files by day
        num_days = calendar.monthrange(int(year), int(month))[1]
        days = [datetime.date(int(year), int(month), day) for day in range(1, num_days+1)]
        for day in days:
            file_pattern = root_path + "/dataHistorian-ISNTS" + server + "-N-" + day.strftime("%Y%m%d") + "*.csv"
            files = glob.glob(file_pattern)
            files_count = len(files)
            if files_count > 0:
                out_dir_day = out_dir + '/' + year + '-ISNTS' + server
                out_file_day = out_dir_day + "/dataHistorian-ISNTS" + server + "-N-" + day.strftime("%Y%m%d.csv.gz")
                if not os.path.exists(out_dir_day):
                    os.makedirs(out_dir_day)

                if os.path.exists(out_file_day):
                    logger.info("{} already exists, no procesing for this day".format(out_file_day))
                else:
                    logger.info("found {} files for day {} with following file pattern {}".format( files_count, day, file_pattern))
                    start = time.time()
                    pool = Pool(8)
                    df_collection = pool.map(read_csv, files)
                    pool.close()
                    pool.join()

                    big_df = pd.concat(df_collection)

                    print("done in %d seconds" %
                          (time.time()-start))

                    logger.info("done creating one single big dataframe")

                    if not os.path.exists(out_file_day):
                        big_df.to_csv(out_file_day, index=False, compression='gzip')
                        logger.info("done zipping big dataframe")

            else:
                logger.info("found no files for day {} ".format( day))


def parse_command_line():
    parser = argparse.ArgumentParser(description='Take ')
    parser.add_argument('--rootDir', help='root directory for input csv files', default='.')
    parser.add_argument('--outDir', help='output directory for zipped csv files', default='.')
    args = parser.parse_args()
    return args


def main():
    args = parse_command_line()
    setup_logger(args.outDir)
    compact_csv_files(args.rootDir, args.outDir)
    logger.info("------------ END -------------")

if __name__ == "__main__":
    main()


#rootDir='/Users/tom/Documents/workspace/historian/data/chemistry'
#outDir = '/Users/tom/Documents/workspace/historian/data/out'

