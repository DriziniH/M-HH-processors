import json
import pandas as pd
from datetime import datetime, timedelta

from src.utility import io
from src.utility.logger import logger
from src.utility import tools


def json_to_parquet(path_json, datetime):
    """Writes json lines from source path with time information to pandas dataframe

    Args:
        path_json (String): Source path with json data

    Returns:
        pdf (Pandas data frame)
    """
    try:
        data_json = io.read_json_lines(path_json, True)
        for row in data_json:
            row.update({
                "year": datetime.year,
                "month": datetime.month,
                "day": datetime.day,
            })
        pdf = pd.DataFrame(data_json)
    except Exception as e:
        logger.error(f'Error converting json to pandas: {str(e)}')
        return pd.DateFrame()

    return pdf


def write_json_to_partioned_parquet(path_json, filename_json, path_parquet, datetime):
    """Reads json data, converts it to pandas dataframe and writes it to destination path as parquet

    Args:
        path_json (String): Source path with json data
        path_parquet (String): Destination path
    """
    json_file = f'{path_json}{tools.get_partitioned_path_from_date(datetime)}\\{filename_json}'
    pdf = json_to_parquet(json_file, datetime)

    if pdf.empty:
        logger.warning(
            f'Failed to transform json data from <{json_file}> to parquet!')
        return False

    if not io.write_partitioned_parquet_from_pandas(pdf, path_parquet, ["year", "month", "day"]):
        logger.error(f'Failed to persist parquet data to <{path_parquet}>!')
        return False

    logger.info(f'Succesfully persisted parquet data to <{path_parquet}>')
    return True


path_json = f"C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_USA_PROCESSED\\CAR\\"
file_json = "car-usa.json"
path_parquet = f"C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_USA_PROCESSED\\CAR_PARQUET\\"


write_json_to_partioned_parquet(
    path_json, file_json, path_parquet, datetime.now() - timedelta(1))
