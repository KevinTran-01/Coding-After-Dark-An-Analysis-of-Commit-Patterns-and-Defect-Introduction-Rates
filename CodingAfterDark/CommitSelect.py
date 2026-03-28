"""
STEP 2 — Commit Collector
Reads repos.txt and pulls up to 500 commits per repo within the date range.
Saves everything raw to commits.csv — no time filtering here, that happens in analysis.
Run overnight — takes ~10 hours for 100 repos x 500 commits.
"""

from github import Github
from dotenv import load_dotenv
import os, csv, time
from datetime import datetime, timezone

load_dotenv()
g = Github(os.getenv("GITHUB_TOKEN"))

# ---------------------------------------------------------------
# DATE RANGE — adjust these to control how fresh your data is
# Recommendation: 2 years back gives you rich, recent data
# without hitting ancient commits that may lack context
# ---------------------------------------------------------------
DATE_FROM = datetime(2022, 1, 1, tzinfo=timezone.utc)   # start: Jan 1 2022
DATE_TO   = datetime(2024, 12, 31, tzinfo=timezone.utc) # end:   Dec 31 2024

# Max commits to pull per repo — 500 is enough for statistical validity
COMMIT_LIMIT = 500

# Files
REPOS_FILE      = "repos.txt"
OUTPUT_CSV      = "commits.csv"
CHECKPOINT_FILE = "collected_repos.txt"

# ---------------------------------------------------------------
# Checkpoint helpers — so a crash doesn't make you start over
# ---------------------------------------------------------------
def already_collected(repo_name):
    if not os.path.exists(CHECKPOINT_FILE):
        return False
    with open(CHECKPOINT_FILE) as f:
        return repo_name in f.read()

def mark_collected(repo_name):
    with open(CHECKPOINT_FILE, "a") as f:
        f.write(repo_name + "\n")

# ---------------------------------------------------------------
# Load repo list
# ---------------------------------------------------------------
if not os.path.exists(REPOS_FILE):
    print(f"ERROR: {REPOS_FILE} not found. Run 1_select_repos.py first.")
    exit()

with open(REPOS_FILE) as f:
    repo_list = [line.strip() for line in f if line.strip()]

print(f"Loaded {len(repo_list)} repos from {REPOS_FILE}")
print(f"Date range: {DATE_FROM.date()} to {DATE_TO.date()}")
print(f"Commit limit per repo: {COMMIT_LIMIT}\n")

# ---------------------------------------------------------------
# Main collection loop
# ---------------------------------------------------------------
write_header = not os.path.exists(OUTPUT_CSV)

with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)

    if write_header:
        writer.writerow([
            "repo",
            "sha",
            "author_name",
            "author_login",
            "author_location",   # for timezone inference later
            "utc_timestamp",
            "utc_hour",          # 0-23, used for time bucket analysis
            "utc_day_of_week",   # 0=Monday, 6=Sunday
            "message",
            "message_length",    # word count
            "files_changed",
            "lines_added",
            "lines_deleted",
        ])

    for repo_name in repo_list:

        # Skip if already done
        if already_collected(repo_name):
            print(f"SKIP  {repo_name} (already collected)")
            continue

        print(f"COLLECTING  {repo_name}")

        try:
            repo = g.get_repo(repo_name)
            commits = repo.get_commits(since=DATE_FROM, until=DATE_TO)
            count = 0

            for commit in commits:
                if count >= COMMIT_LIMIT:
                    break

                try:
                    dt = commit.commit.author.date

                    # Ensure timezone aware
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)

                    # Get author location for later timezone enrichment
                    author_login = ""
                    author_location = ""
                    try:
                        user = commit.author
                        if user:
                            author_login = user.login or ""
                            author_location = user.location or ""
                    except Exception:
                        pass  # author info is optional, don't crash over it

                    # Commit stats
                    files_changed = 0
                    lines_added   = 0
                    lines_deleted = 0
                    try:
                        files_changed = len(commit.files)
                        lines_added   = commit.stats.additions
                        lines_deleted = commit.stats.deletions
                    except Exception:
                        pass  # stats sometimes unavailable

                    # Message word count
                    message = commit.commit.message.strip().replace("\n", " ")
                    message_length = len(message.split())

                    writer.writerow([
                        repo_name,
                        commit.sha,
                        commit.commit.author.name,
                        author_login,
                        author_location,
                        dt.isoformat(),
                        dt.hour,
                        dt.weekday(),
                        message,
                        message_length,
                        files_changed,
                        lines_added,
                        lines_deleted,
                    ])

                    count += 1

                    # Print progress every 100 commits
                    if count % 100 == 0:
                        print(f"  {count} commits collected...")

                    time.sleep(0.1)  # avoid hammering the API

                except Exception as e:
                    print(f"  Skipped commit: {e}")
                    continue

            print(f"  Done — {count} commits saved")
            mark_collected(repo_name)

        except Exception as e:
            print(f"  ERROR on {repo_name}: {e}")
            time.sleep(5)  # wait before moving on
            continue

        # Check rate limit every repo and pause if running low
        try:
            rate = g.get_rate_limit()
            remaining = rate.core.remaining
            print(f"  Rate limit: {remaining} remaining")

            if remaining < 200:
                reset_time = rate.core.reset
                now = datetime.now(timezone.utc)
                wait_seconds = (reset_time - now).total_seconds() + 10
                print(f"  Rate limit low — sleeping {int(wait_seconds)}s until reset...")
                time.sleep(max(wait_seconds, 0))
        except Exception:
            pass

print(f"\nAll done. Data saved to {OUTPUT_CSV}")
