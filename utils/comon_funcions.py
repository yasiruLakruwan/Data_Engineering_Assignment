import os 
import pandas
from src.logger import get_logger
from src.custom_exeption import CustomExeption
import pandas as pd

# function to read yaml file.
logger = get_logger(__name__)


def read_csv(path):
    try:
        logger.info(f"Listing CSV files in: {path}")

        # List all CSV files
        csv_files = [f for f in os.listdir(path) if f.endswith(".csv")]

        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in {path}")

        file_path = os.path.join(path, csv_files[0])
        logger.info(f"Reading CSV file: {file_path}")

        return pd.read_csv(file_path)

    except Exception as e:
        logger.error(f"Error happening when reading the csv file {e}")
        raise CustomExeption("CSV reading error",e)
    