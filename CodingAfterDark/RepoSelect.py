"""
Search github for top repos accross top languages
"""

import github
from github import Github
from dotenv import load_dotenv
import os, time

load_dotenv()
g = Github(auth=github.Auth.Token(os.getenv("GITHUB_TOKEN")))

# top lanuages to sample
LANGUAGES = ["python", "java", "javascript", "c", "c++"]
REPOS_PER_LANGUAGE=20


output_file = "repos.txt"
collected = []

def is_qualified(repo):
    # not a fork
    if repo.fork:
        return False, "is a fork"
    
    # blacklist
    name = (repo.name).lower()
    blacklist = ["awesome", "tutorial", "learning", "beginner",
                 "course", "guide", "cheatsheet", "interview"]

    for word in blacklist:
        if word in name:
            return False, "got blacklisted keyword"
    
    return True, "passed"

for lang in LANGUAGES:
    
    # criteria
    query = f"stars:>=10000 forks:>=500 language:{lang} pushed:>=2025-01-01"
    results = g.search_repositories(query=query, sort="updated", order="desc")

    count = 0                      
    for repo in results:

        if count >= REPOS_PER_LANGUAGE:
            break

        qualified, reason = is_qualified(repo)
        if not qualified:
            print(f"Not taking {repo.full_name} - {reason}")
            continue

        collected.append(repo.full_name)
        count+=1
        time.sleep(0.5) # mercy to the API call

# save to file
with open(output_file, "w") as f:
    for repo in collected:
        f.write(repo + "\n")

print(f"total {len(collected)} repos saved")