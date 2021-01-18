import os
import errno
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from datetime import datetime
import jsonlines

from src.utility.logger import logger
from src.utility import dict_tools

def create_path(path):
    if not os.path.exists(os.path.dirname(path)):
        try:
            os.makedirs(os.path.dirname(path))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                logger.error(exc)
                raise
        except Exception as e:
            logger.error(f'Error creating path <{path}> : {str(e)}')
            return False


def write_data(path, mode, data):
    """Creates dirs if not existent and writes data in given mode

    Args:
        path (String): Path to write to
        mode (String): Write mode
        data (?): any file

    Returns:
        Boolean: Write operation successfull or not
    """

    try:
        create_path(path)

        with open(path, mode) as f:
            f.write(data)

    except Exception as e:
        logger.error(e)
        return False

    return True

def read_json_to_dict(path):
    try:
        with open(path) as f:
            return dict_tools.load_json_to_dict(f.read())
    except Exception as e:
        logger.error(f'Error reading json to dict : {str(e)}')
        return {}

def write_json_lines(path, mode, data):
    try:
        create_path(path)

        with jsonlines.open(path, mode=mode) as writer:
            writer.write(data)

    except Exception as e:
        logger.error(f'Error writing json lines to <{path}> : {str(e)}')
        return False

    return True


def read_json_lines(path,flatten):
    data = []
    try:
        with jsonlines.open(path) as reader:
            for json_line in reader:
                if flatten:
                    json_line = dict_tools.flatten_json(json_line)
                data.append(json_line)
    except Exception as e:
        logger.error(f'Error reading json lines from <{path}> : {str(e)}')
        return []
    return data


def read_partitioned_parquet_to_pandas(path):
    """Reads partioned parquet file from given path

    Args:
        path (String): Path to read from

    Returns:
        pandas data frame: parquet file as pandas data frame
    """
    try:
        table = pq.read_table(path)
        pdf = table.to_pandas()
    except Exception as e:
        logger.error(f'Error reading partioned parquet from <{path}> : {str(e)}')
        return pd.DataFrame()
    return pdf


def write_partitioned_parquet_from_pandas(pdf, path, partition_cols=None):
    """Writes partitioned parquet file to given path. Partition by given columns of data

    Args:
        pdf (pandas data frame): Data
        path (String): Path to write to
        partition_cols (Stringarray): Columns from the dataset to partition by

    Returns:
        Boolean: Write operation successfull or not
    """

    try:
        table = pa.Table.from_pandas(pdf)
        pq.write_to_dataset(table, path, partition_cols)

    except Exception as e:
        logger.error(f'Error writing partioned parquet to path <{path}> : {str(e)}')
        return False

    return True
