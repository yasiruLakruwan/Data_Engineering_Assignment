from src.data_ingestion import DataIngestion
from src.data_processing import DataProcessor
from config.path_config import *


if __name__ == "__main__":
    # Data ingestion pipeline  

    dataingestion = DataIngestion()
    dataingestion.run()

    # Data Processing pipeline  

    processor = DataProcessor(RAW_DATA_PATH,PROCESSED_DATA_PATH)
    processor.process()


