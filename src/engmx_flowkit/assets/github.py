"""GitHub asset definitions.

Assets:
    - github_repositories: Repository metadata for configured organizations
    - github_pull_requests: Pull request data with review information
    - github_commits: Commit history across repositories
    - github_actions_runs: GitHub Actions workflow run execution data

Connection: github_default (http)
API: GitHub REST API v3 (/repos, /pulls, /commits, /actions/runs)
"""
