from src.logger import get_logger
from src.custom_exeption import CustomExeption
import pandas as pd
import numpy as np
import boto3
from io import StringIO
from dotenv import load_dotenv
import os
from config.path_config import *

load_dotenv()
logger = get_logger(__name__)

class DataIngestion:
    def __init__(self):

        self.bucket_name = os.getenv("S3_BUCKET_NAME")
        self.data_file = os.getenv("S3_DATA_FILE")
        self.region = os.getenv("REGION_NAME")


        os.makedirs(PROCESSED_DATA_PATH,exist_ok=True)
        os.makedirs(RAW_DATA_PATH,exist_ok=True)

        logger.info(f"Data ingestion started with {self.bucket_name} and file {self.data_file}")
    
    def download_data_from_s3(self):
        try:
            s3 = boto3.client(
                "s3",
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY_ID"),
                region_name=self.region
            )
            
            response = s3.get_object(
                Bucket=self.bucket_name,
                Key=self.data_file
            )

            csv_data=response["Body"].read().decode("utf-8")

            file_path = os.path.join(RAW_DATA_PATH,"hotel_dataset.csv")

            # Save data in raw data directory
            with open(file_path,"w",encoding="utf-8") as f:
                f.write(csv_data)

            logger.info("Dataset saved successfully...!")

            data = pd.read_csv(StringIO(csv_data))

            logger.info("Data downloaded successfully....!")

            return data.head()
        

        except Exception as e:
            logger.error("Error happening in data ingestion")
            raise CustomExeption("Error happening in data ingestion",e)
    
    def run(self):
        first_5_data = self.download_data_from_s3()
        print(first_5_data)
        
if __name__=="__main__":
    ingesion=DataIngestion()
    ingesion.run()