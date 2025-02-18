import requests
import time
from utils import write_to_file


class GitHubPRFetcher:
    def __init__(self, config, repo_details):
        self.config = config
        self.repo_details = repo_details
        self.base_pr_url = (
            f"https://api.github.com/repos/{repo_details['REPO_OWNER']}/{repo_details['REPO_NAME']}/pulls"
        )
        self.github_api_version = "2022-11-28"

    def _request_github_api(self, request_url):
        bearer_token = f"Bearer {self.config['GITHUB_API_TOKEN']}"
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": bearer_token,
            "X-GitHub-Api-Version": self.github_api_version,
        }

        print(f"Requesting URL: {request_url}")
        response = requests.get(request_url, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json()

    def _get_pull_request_pages(self):
        pr_page_link = f"{self.base_pr_url}?state=all&per_page=100"
        first_pr_page = f"{pr_page_link}&page=1"
        response = self._request_github_api(first_pr_page)
        num_pages = -(-response[0]["number"] // 100)

        return [f"{pr_page_link}&page={page_num}" for page_num in range(1, num_pages + 1)]

    def fetch_all_pr_data(self):

        pr_pages = self._get_pull_request_pages()
        pr_data = []

        for pr_page in pr_pages:
            pr_data += self._request_github_api(pr_page)
            time.sleep(int(self.config["REQUEST_TIME_INTERVAL"]))

        print(f"Number of PRs: {len(pr_data)}")
        pr_data_file_path = write_to_file(pr_data, self.config["RAW_DATA_PATH"], "pr_data", "json")
        return pr_data_file_path

    def fetch_updated_pr_data(self):

        pr_page_link = f"{self.base_pr_url}?state=all&per_page=100&sort=updated&direction=desc"
        response = self._request_github_api(pr_page_link)

        print(f"Number of PRs: {len(response)}")
        pr_data_file_path = write_to_file(response, self.config["RAW_DATA_PATH"], "updated_pr_data", "json")
        return pr_data_file_path