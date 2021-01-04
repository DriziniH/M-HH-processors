import os
import errno
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


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
        if not os.path.exists(os.path.dirname(path)):
            try:
                os.makedirs(os.path.dirname(path))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
            except Exception as e:
                print(e)
                return False

        with open(path, mode) as f:
            f.write(data)

    except Exception as e:
        print(str(e))
        return False

    print(f"Write operation successfull. Path: {path}")
    return True


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
        print("Successfully red partitioned parquet file")
    except Exception as e:
        print(e)
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
        print(e)
        return False

    print("Writing partitioned parquet operation successfull")
    return True
