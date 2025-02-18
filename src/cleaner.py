import json
import glob
import pandas as pd
import os
from src.utils import write_to_file, read_file

class PRDataCleaner:
    def __init__(self, config):
        self.config = config

    def clean_pr_data(self, pr_data_file):
        pr_data = read_file(pr_data_file)

        cleaned_pr_data = []
        for pr in pr_data:
            if isinstance(pr, dict):
                cleaned_pr_data.append({
                    "pr_url": pr.get("html_url"),
                    "pr_id": pr.get("id"),
                    "pr_number": pr.get("number"),
                    "pr_state": pr.get("state"),
                    "pr_title": pr.get("title"),
                    "pr_creator": pr.get("user", {}).get("login"),
                    "pr_creator_type": pr.get('user', {}).get('type'),
                    "pr_creation_time": pr.get("created_at"),
                    "pr_updation_time": pr.get("updated_at"),
                    "pr_closing_time": pr.get("closed_at"),
                    "pr_merging_time": pr.get("merged_at"),
                    "pr_merge_commit": pr.get("merge_commit_sha"),
                    "pr_comments_url": pr.get("comments_url"),
                    "pr_merge_branch_from": pr.get("head", {}).get("label"),
                    "pr_merge_branch_to": pr.get("base", {}).get("label"),
                    "pr_author_association": pr.get("author_association")
                })

        cleaned_pr_data_file_path = write_to_file(
            cleaned_pr_data,
            self.config["CLEANED_DATA_PATH"],
            "cleaned_pr_data",
            "json"
        )
        return cleaned_pr_data_file_path
