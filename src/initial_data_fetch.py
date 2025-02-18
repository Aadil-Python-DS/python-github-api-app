import os
from sqlalchemy import create_engine
from multiprocessing.pool import ThreadPool

from utils import load_config
from pr_fetcher import repository_pr_data_fetch 
from cleaner import clean_pr_data 
from metrics import calculate_pr_metrics 
from db_loader import load_to_db, clean_files_older_than_one_day

class PRDataProcessor:
    def __init__(self, config, repo_details):
        self.config = config
        self.repo_details = repo_details
        self.engine = self._create_db_engine()
    
    def _create_db_engine(self):
        mysql_user = self.config.get("DB_USERNAME")
        mysql_password = self.config.get("DB_PASSWD")
        mysql_db = self.config.get("DB_NAME")
        mysql_host = self.config.get("DB_NETWORK")
        engine_url = f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:3306/{mysql_db}"
        return create_engine(engine_url)
    
    def process_repo_data(self):
        pr_data_file = repository_pr_data_fetch(self.config, self.repo_details)
        pr_data_cleaned_file = clean_pr_data(self.config, pr_data_file)
        pr_metrics_df = calculate_pr_metrics(self.config, pr_data_cleaned_file)
        
        table_name = f"{self.repo_details.get('REPO_NAME')}_pr_data"
        load_to_db(pr_metrics_df, self.engine, table_name)
        
        self._clean_old_files()
    
    def _clean_old_files(self):
        clean_files_older_than_one_day(self.config["RAW_DATA_PATH"])
        clean_files_older_than_one_day(self.config["CLEANED_DATA_PATH"])
        clean_files_older_than_one_day(self.config["METRICS_DATA_PATH"])

if __name__ == "__main__":
    config = load_config(os.environ.get("PAT_CONFIG_FILE"))
    repo_details_list = config["REPO_DETAILS"]
    
    pool = ThreadPool()
    processors = [PRDataProcessor(config, repo_details) for repo_details in repo_details_list]
    results = pool.map(lambda processor: processor.process_repo_data(), processors)
