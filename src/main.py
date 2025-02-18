import os
from sqlalchemy import create_engine
import trio
from utils import load_config
from pr_fetcher import repository_pr_data_fetch, repository_comment_data_fetch
from cleaner import clean_pr_data, clean_pr_comments_data
from metrics import calculate_pr_metrics, calculate_pr_comment_metrics
from db_loader import load_to_db


class GitHubDataProcessor:
    def __init__(self, config, repo_details):
        self.config = config
        self.repo_details = repo_details
        self.engine = self._create_db_engine()

    def _create_db_engine(self):
        mysqldb_user = self.config.get("DB_USERNAME")
        mysqldb_password = self.config.get("DB_PASSWD")
        mysqldb_name = self.config.get("DB_NAME")
        mysqldb_network = self.config.get("DB_NETWORK")
        engine_url = f"mysql+pymysql://{mysqldb_user}:{mysqldb_password}@{mysqldb_network}:3306/{mysqldb_name}"
        return create_engine(engine_url)

    async def update_data(self):
        # Fetch and clean PR data
        pr_data_file = await repository_pr_data_fetch(self.config, self.repo_details)
        pr_data_cleaned_file = clean_pr_data(self.config, pr_data_file)

        # Fetch and clean PR comments data
        pr_comments_data_file = await repository_comment_data_fetch(self.config, pr_data_cleaned_file)
        pr_comments_data_cleaned_file = clean_pr_comments_data(self.config, pr_comments_data_file)

        # Calculate PR and PR comment metrics
        pr_metrics_data_file = calculate_pr_metrics(self.config, pr_data_cleaned_file)
        pr_comment_metrics_data_file, pr_comment_stats_data_file = calculate_pr_comment_metrics(
            self.config, pr_comments_data_cleaned_file
        )

        # Load data into the database
        repo_name = self.repo_details.get("REPO_NAME")
        load_to_db(pr_metrics_data_file, self.engine, f"{repo_name}_pr_data")
        load_to_db(pr_comment_metrics_data_file, self.engine, f"{repo_name}_pr_comments_data")
        load_to_db(pr_comment_stats_data_file, self.engine, f"{repo_name}_pr_comments_stats")


async def main():

    config = load_config(os.environ.get("PAT_CONFIG_FILE"))
    async with trio.open_nursery() as nursery:
        for repo_details in config["REPO_DETAILS"]:
            processor = GitHubDataProcessor(config, repo_details)
            nursery.start_soon(processor.update_data)


if __name__ == "__main__":
    trio.run(main)


    