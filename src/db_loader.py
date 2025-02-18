import os
import time
import pandas as pd
from sqlalchemy import inspect

class DatabaseHandler:
    def __init__(self, engine):
        self.engine = engine
        self.driver = "mysql+pymysql"

    def load_to_db(self, df, table_name):
        df.to_sql(table_name, self.engine, if_exists="replace", index=False)   
        print(f"Data written to MySQL table: {table_name}")
        return True

class FileCleaner:
    def __init__(self, dir_location, num_days=1):
        self.dir_location = os.path.join(os.getcwd(), dir_location)
        self.num_days = num_days
        self.current_time = time.time()
        self.day_seconds = 86400
    
    def clean_files_older_than_one_day(self):
        list_of_files = os.listdir(self.dir_location) 
        for file_name in list_of_files: 
            file_path = os.path.join(self.dir_location, file_name) 
            file_time = os.stat(file_path).st_mtime 
            if file_time < self.current_time - self.day_seconds * self.num_days: 
                print(f"Deleting: {file_name}") 
                os.remove(file_path)
