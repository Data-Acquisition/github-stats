import requests
from pydantic import BaseModel
from typing import Optional

# Constants
GITHUB_API_URL = "https://api.github.com"
ACCESS_TOKEN = ""


class Repo(BaseModel):
    name: str
    total_commits: int = 0


class Commit(BaseModel):
    sha: str
    author: str
    date: str
    additions: int
    deletions: int
    message: Optional[str]


def get_org_repos(org_name):
    repos = []
    page = 1
    per_page = 100
    headers = {'Authorization': f'token {ACCESS_TOKEN}'}

    while True:
        url = f"{GITHUB_API_URL}/orgs/{org_name}/repos?per_page={per_page}&page={page}"
        response = requests.get(url, headers=headers)
        try:
            response.raise_for_status()
            page_repos = response.json()
            if not page_repos:
                break
            print(f"Page {page} contains {len(page_repos)} repos")
            for repo in page_repos:
                repos.append(Repo(name=repo['name']))
            page += 1
        except requests.exceptions.HTTPError as e:
            print(f"Error fetching repositories: {e}")
            break

    return repos


def get_repo_commit_info(org_name, repo):
    repo_name = repo.name
    commits_info = []
    headers = {'Authorization': f'token {ACCESS_TOKEN}'}
    repo_url = f"{GITHUB_API_URL}/repos/{org_name}/{repo_name}"
    repo_response = requests.get(repo_url, headers=headers)
    try:
        repo_response.raise_for_status()
        branch = repo_response.json()['default_branch']
    except requests.exceptions.HTTPError as e:
        print(f"Error fetching repository details for {repo_name}: {e}")
        return []
    except KeyError:
        print(f"Could not determine default branch for {repo_name}")
        return []
    page = 1
    per_page = 100
    headers = {'Authorization': f'token {ACCESS_TOKEN}'}

    while True:
        url = f"{GITHUB_API_URL}/repos/{org_name}/{repo_name}/commits?per_page={per_page}&page={page}&sha={branch}"
        response = requests.get(url, headers=headers)
        try:
            response.raise_for_status()
            page_commits = response.json()
            if page_commits:
                for commit in page_commits:
                    if isinstance(commit, dict):
                        author_login = commit['author']['login'] if commit['author'] else commit['commit'][
                            'committer'].get('name', 'Unknown')
                        commit_details = {
                            'sha': commit['sha'],
                            'author': author_login,
                            'date': commit['commit']['author']['date'],
                            'message': commit['commit']['message']
                        }
                        commit_url = f"{GITHUB_API_URL}/repos/{org_name}/{repo_name}/commits/{commit['sha']}"
                        commit_response = requests.get(commit_url, headers=headers)
                        commit_response.raise_for_status()
                        commit_data = commit_response.json()
                        commit_details['additions'] = commit_data['stats']['additions']
                        commit_details['deletions'] = commit_data['stats']['deletions']

                        commits_info.append(commit_details)
                page += 1
            else:
                break
        except requests.exceptions.HTTPError as e:
            print(f"Error fetching commits for {repo_name}: {e}")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
            break

    return commits_info


def main():
    org_name = "Data-Acquisition"
    print(f"Getting repos for {org_name}")
    repos = get_org_repos(org_name)

    for repo in repos:
        print(repo)


if __name__ == "__main__":
    main()
