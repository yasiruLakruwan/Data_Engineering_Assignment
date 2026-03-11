from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from src.logger import get_logger

load_dotenv()

logger = get_logger(__name__)

class DbConnector:
    def __init__(self,df):
        self.df = df
        self.user = os.getenv("USER")
        self.password = os.getenv("PASSWORD")
        self.host = os.getenv("HOST")
        self.database = os.getenv("DATABASE")
        self.table = os.getenv("TABLE")
    
    def load_to_mysql(self):
        try:
            engine = create_engine(
                f"mysql+pymysql://{self.user}:{self.password}@{self.host}/{self.database}"
            )

            self.df.to_sql(
                self.table,
                engine,
                if_exists="append",
                index=False
            )
            
            logger.info("Data loaded to mysql successfully...")
        except Exception as e:
            logger.error("Error happening while data loading to mysql...!")
