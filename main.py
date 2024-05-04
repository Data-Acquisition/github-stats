import requests
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timedelta
import psycopg2
import os

# Constants
GITHUB_API_URL = "https://api.github.com"
ACCESS_TOKEN = ""
UPDATE_ONLY = os.getenv('UPDATE_ONLY', 'false').lower() == 'true'


class Repo(BaseModel):
    name: str
    total_commits: int = 0


class Commit(BaseModel):
    sha: str
    author: str
    date: datetime
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
                repos.append(Repo(name=repo['name'], total_commits=0))
            page += 1
        except requests.exceptions.HTTPError as e:
            print(f"Error fetching repositories: {e}")
            break

    return repos


def get_repo_commit_info(org_name, repo):
    repo_name = repo.name
    headers = {'Authorization': f'token {ACCESS_TOKEN}'}

    # Fetch the default branch
    repo_url = f"{GITHUB_API_URL}/repos/{org_name}/{repo_name}"
    repo_response = requests.get(repo_url, headers=headers)
    try:
        repo_response.raise_for_status()
        default_branch = repo_response.json()['default_branch']
    except requests.exceptions.HTTPError as e:
        print(f"Error fetching repository details for {repo_name}: {e}")
        return []
    except KeyError:
        print(f"Could not determine default branch for {repo_name}")
        return []

    page = 1
    per_page = 100
    commits_info = []
    since = (datetime.utcnow() - timedelta(days=1)).isoformat() + "Z" if UPDATE_ONLY else None

    while True:
        if since:
            url = f"{GITHUB_API_URL}/repos/{org_name}/{repo_name}/commits?per_page={per_page}&page={page}&sha={default_branch}&since={since}"
        else:
            url = f"{GITHUB_API_URL}/repos/{org_name}/{repo_name}/commits?per_page={per_page}&page={page}&sha={default_branch}"
        response = requests.get(url, headers=headers)
        try:
            response.raise_for_status()
            page_commits = response.json()
            if not page_commits:
                break

            for commit in page_commits:
                author_login = commit['author']['login'] if commit['author'] else commit['commit']['committer'].get(
                    'name', 'Unknown')
                commit_data = commit.get('commit', {})

                commit_url = f"{GITHUB_API_URL}/repos/{org_name}/{repo_name}/commits/{commit['sha']}"
                commit_response = requests.get(commit_url, headers=headers)
                commit_response.raise_for_status()
                detailed_commit_data = commit_response.json()
                stats = detailed_commit_data.get('stats', {})

                sha = commit.get('sha', 'unknown')
                author = author_login
                date = datetime.strptime(commit_data.get('author', {}).get('date', 'unknown'), "%Y-%m-%dT%H:%M:%SZ")
                additions = stats.get('additions', 0)
                deletions = stats.get('deletions', 0)
                message = commit_data.get('message', 'No message')

                commits_info.append(
                    Commit(
                        sha=sha,
                        author=author,
                        date=date,
                        additions=additions,
                        deletions=deletions,
                        message=message
                    )
                )
            page += 1
        except requests.exceptions.HTTPError as e:
            print(f"Error fetching commits for {repo_name}: {e}")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
            break

    repo.total_commits = len(commits_info)

    return commits_info


def make_migrations(conn):
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS repos (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) UNIQUE NOT NULL,
        total_commits INT DEFAULT 0
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS commits (
        id SERIAL PRIMARY KEY,
        sha VARCHAR(255) UNIQUE NOT NULL,
        author VARCHAR(255),
        date TIMESTAMP,
        additions INT,
        deletions INT,
        message TEXT,
        repo_id INT REFERENCES repos(id) ON DELETE CASCADE
        );
    """)

    conn.commit()
    cur.close()


def insert_into_postgres(conn, repos, commits_data):
    cur = conn.cursor()

    for repo in repos:
        if UPDATE_ONLY:
            # Update only mode: Fetch existing total commits count from the DB
            cur.execute("SELECT total_commits FROM repos WHERE name=%s", (repo.name,))
            result = cur.fetchone()
            total_commits = repo.total_commits + result[0] if result else repo.total_commits
        else:
            total_commits = repo.total_commits

        cur.execute("""
            INSERT INTO repos (name, total_commits)
            VALUES (%s, %s)
            ON CONFLICT (name) DO UPDATE SET
                total_commits = EXCLUDED.total_commits
            RETURNING id;
            """, (repo.name, total_commits))
        repo_id = cur.fetchone()[0]

        for commit in commits_data.get(repo.name, []):
            cur.execute("""
                INSERT INTO commits (sha, author, date, additions, deletions, message, repo_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sha) DO NOTHING;
                """, (
                commit.sha,
                commit.author,
                commit.date,
                commit.additions,
                commit.deletions,
                commit.message,
                repo_id
            ))

    conn.commit()
    cur.close()


def main():
    org_name = "Data-Acquisition"

    print("Connecting to Postgres...")
    conn = psycopg2.connect(user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"],
                            database=os.environ["POSTGRES_DATABASE"], host=os.environ["POSTGRES_HOST"],
                            port=os.environ["POSTGRES_PORT"])

    print("Running migrations...")
    make_migrations(conn)

    print(f"Getting repos for {org_name}")
    repos = get_org_repos(org_name)

    all_commits_data = {}

    for repo in repos:
        print(f"Fetching commits for {repo.name}")
        commits = get_repo_commit_info(org_name, repo)

        repo.total_commits = len(commits)
        all_commits_data[repo.name] = commits

        print(f"{repo.name} has {repo.total_commits} commits")
        print(commits)

    insert_into_postgres(conn, repos, all_commits_data)

    conn.close()


if __name__ == "__main__":
    main()
