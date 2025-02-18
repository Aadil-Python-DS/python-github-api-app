import os
import time
import pandas as pd
from sqlalchemy import inspect

class DatabaseHandler:
    def __init__(self, engine):
        self.engine = engine
    
    def load_to_db(self, df, table_name):
        df.to_sql(table_name, self.engine, if_exists="replace")   
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

class PRMetricsProcessor:
    def __init__(self, config):
        self.config = config
    
    @staticmethod
    def merge_status(merge_time):
        return "Unmerged" if merge_time == -99 else "Merged"
    
    @staticmethod
    def capitalize(input_string):
        return input_string.capitalize()
    
    def calculate_pr_metrics(self, cleaned_pr_data):
        df = pd.read_json(cleaned_pr_data)
        df["pr_creation_time"] = pd.to_datetime(df["pr_creation_time"])
        df["pr_updation_time"] = pd.to_datetime(df["pr_updation_time"])
        df["pr_closing_time"] = pd.to_datetime(df["pr_closing_time"])
        df["pr_merging_time"] = pd.to_datetime(df["pr_merging_time"])
        df["pr_merge"] = df["pr_merging_time"].fillna(-99).apply(self.merge_status)
        df["pr_state"] = df["pr_state"].apply(self.capitalize)

        df["time_delta_updation_creation"] = (df["pr_updation_time"] - df["pr_creation_time"]).dt.total_seconds()
        df["time_delta_closing_creation"] = (df["pr_closing_time"] - df["pr_creation_time"]).dt.total_seconds()
        df["time_delta_merging_creation"] = (df["pr_merging_time"] - df["pr_creation_time"]).dt.total_seconds()

        return df.set_index("pr_id")
    
    def update_metrics(self, engine, existing_data_table, new_metrics_df):
        existing_data_df = pd.read_sql_table(existing_data_table, engine)
        existing_data_df.set_index("pr_id", inplace=True)
        existing_data_df.update(new_metrics_df.set_index("pr_url"))
        return existing_data_df
