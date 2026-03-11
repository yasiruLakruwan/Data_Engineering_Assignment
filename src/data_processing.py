from src.logger import get_logger
from src.custom_exeption import CustomExeption
import pandas as pd
from utils.comon_funcions import read_csv
import os
from config.path_config import *

logger = get_logger(__name__)

class DataProcessor:
    def __init__(self,raw_dir,processed_dir):
        self.raw_dir = raw_dir
        self.processed_dir = processed_dir

        if not os.path.exists(self.processed_dir):
            os.makedirs(self.processed_dir)

    def process_data(self,df):
        try:
            logger.info("Starting our Data Preprocessing step")
            logger.info("Droping the columns")
            df.drop_duplicates(inplace = True)

            outlier_has_columns = ['no_of_special_requests','avg_price_per_room','lead_time','no_of_weekend_nights','no_of_week_nights']

            for column in outlier_has_columns:
                Q1 = df[column].quantile(0.25)
                Q3 = df[column].quantile(0.75)

                IQR = Q3-Q1

                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR

                df_clean = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]

            string_cols = df_clean.select_dtypes(include="str")

            for col in string_cols:
                df_clean[col] = df_clean[col].str.lower()
                
            # This creates a new DataFrame containing only the "bad" data
            rejected_records1 = df_clean[df_clean['avg_price_per_room'] <= 0]

            if not rejected_records1.empty:
                logger.info(f"Found invalid records: {rejected_records1}")
                
            df_clean['arrival_full_date'] = pd.to_datetime(
                df_clean['arrival_year'].astype(str) + '-' +
                df_clean['arrival_month'].astype(str) + '-' +
                df_clean['arrival_date'].astype(str),
                errors='coerce'
            )

            df_clean[df_clean['arrival_full_date'].isna()]

            records=df_clean['arrival_full_date'].isna().sum()
            logger.info(f"Number of records are invalid: {records}")

            df_clean = df_clean.dropna(subset=['arrival_full_date'])

            df_clean.drop(['arrival_year','arrival_month','arrival_date'], axis=1, inplace=True)

            return df_clean

        except Exception as e:
            logger.error("Error during data processing step {e}")
            raise CustomExeption("Error while preprocess data",e)
    
    def save_data(self,df):
        try:
            logger.info("Saving processed data to the processed folder.")
            processed_file_path = os.path.join(PROCESSED_DATA_PATH, "processed_hotel_data.csv")
            df.to_csv(processed_file_path, index=False, encoding="utf-8")
            print(f"Processed data saved to {processed_file_path}") 
            logger.info(f"Successfully saved data to the folder {self.processed_dir}")

        except Exception as e:
            logger.error(f"Error during data processing step {e}")
            raise CustomExeption("Error while saving data",e)
        
    def process(self):
        try:
            logger.info("Loading data from the RAW direcry")
            df = read_csv(self.raw_dir)
            df_clean = self.process_data(df)

            self.save_data(df_clean)

            logger.info("Data preprocessing complete in combined functions")
            logger.info(f"Processed data stored in to {self.processed_dir}")

        except Exception as e:
            logger.error("Error happening combined process step.")
            CustomExeption("Error while in the combined process step",e)

if __name__=="__main__":
    processor = DataProcessor(RAW_DATA_PATH,PROCESSED_DATA_PATH)
    processor.process()