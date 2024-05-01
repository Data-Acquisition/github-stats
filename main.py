import requests

# Constants
GITHUB_API_URL = "https://api.github.com"
ACCESS_TOKEN = ""


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
            if page_repos:
                repos.extend(page_repos)
                page += 1
            else:
                break
        except requests.exceptions.HTTPError as e:
            print(f"Error fetching repositories: {e}")
            break

    return repos


def get_repo_commit_info(org_name, repo_name):
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


org_name = "Data-Acquisition"
repos = get_org_repos(org_name)
print(f"Total repositories in organization: {len(repos)}")

for repo in repos:
    repo_name = repo['name']
    commits_info = get_repo_commit_info(org_name, repo_name)
    print(f"\nRepository: {repo_name}, Total Commits: {len(commits_info)}")
    for commit in commits_info:
        print(f"- Commit {commit['sha']}:")
        print(f"  Author: {commit['author']}")
        print(f"  Date: {commit['date']}")
        print(f"  Additions: {commit['additions']}, Deletions: {commit['deletions']}")
        print(f"  Message: {commit['message']}\n")
