# scripts/generate_repo_stats.py
from __future__ import annotations

import json
import subprocess
from collections import Counter, defaultdict
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_MD = PROJECT_ROOT / "REPO_STATS.md"

# Official team roster
# git_names should match the real output of:
# git log --format="%an" | sort | uniq -c

TEAM_MEMBERS = [
    {
        "name": "Hilfritz Camallere",
        "github": "hilfritz",
        "git_names": ["Hilfritz", "Hil Fritz", "Hilfritz Camallere"],
    },
    {
        "name": "Ananya Mandal",
        "github": "AnanyaMandal-DataAnalyst",
        "git_names": ["Ananya Mandal", "Ananya"],
    },
    {
        "name": "Daniyal Khan",
        "github": "daniyalnkh",
        "git_names": ["Daniyal Khan", "Daniyal"],
    },
    {
        "name": "Joseph Jamoralin",
        "github": "Joseph-dataanalyst",
        "git_names": ["Joseph Jamoralin", "Joseph"],
    },
]


def normalize_text(value: str) -> str:
    return value.strip().lower()


def run_command(command: list[str], check: bool = True) -> str:
    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=check,
    )
    return result.stdout.strip()


def safe_run_command(command: list[str]) -> str:
    try:
        return run_command(command, check=True)
    except Exception:
        return ""


def get_total_commits() -> int:
    output = safe_run_command(["git", "rev-list", "--count", "HEAD"])
    return int(output) if output.isdigit() else 0


def get_branches() -> list[str]:
    output = safe_run_command(["git", "branch", "--format=%(refname:short)"])
    return [line.strip() for line in output.splitlines() if line.strip()]


def get_contributor_stats() -> list[dict]:
    """
    Returns contributor statistics directly from git history.

    Example result:
        [
            {
                "name": "Hilfritz",
                "commits": 16,
                "insertions": 100,
                "deletions": 10,
                "files_changed": 25,
            }
        ]
    """
    output = safe_run_command(["git", "log", "--format=%an", "--numstat"])
    stats = defaultdict(
        lambda: {
            "commits": 0,
            "insertions": 0,
            "deletions": 0,
            "files_changed": 0,
        }
    )

    current_author = None

    for raw_line in output.splitlines():
        line = raw_line.strip()

        if not line:
            continue

        parts = line.split("\t")

        # numstat row
        if len(parts) == 3 and current_author:
            added, deleted, _filename = parts

            if added.isdigit():
                stats[current_author]["insertions"] += int(added)
            if deleted.isdigit():
                stats[current_author]["deletions"] += int(deleted)

            stats[current_author]["files_changed"] += 1
        else:
            current_author = line
            stats[current_author]["commits"] += 1

    result = []
    for author, values in stats.items():
        result.append(
            {
                "name": author,
                "commits": values["commits"],
                "insertions": values["insertions"],
                "deletions": values["deletions"],
                "files_changed": values["files_changed"],
            }
        )

    result.sort(key=lambda x: x["commits"], reverse=True)
    return result


def get_commit_type_distribution() -> dict[str, int]:
    output = safe_run_command(["git", "log", "--format=%s"])
    counter = Counter()

    known_types = ["feat", "fix", "docs", "test", "chore", "refactor", "merge"]

    for subject in output.splitlines():
        lowered = subject.strip().lower()
        matched = False

        for prefix in known_types:
            if lowered.startswith(prefix + "(") or lowered.startswith(prefix + ":"):
                counter[prefix] += 1
                matched = True
                break

        if not matched and lowered:
            counter["other"] += 1

    return dict(counter)


def get_file_counts() -> dict[str, int]:
    py_files = list(PROJECT_ROOT.rglob("*.py"))
    test_files = [p for p in py_files if "tests" in p.parts]
    src_files = [p for p in py_files if "src" in p.parts]
    md_files = list(PROJECT_ROOT.rglob("*.md"))

    return {
        "python_files": len(py_files),
        "test_files": len(test_files),
        "source_files": len(src_files),
        "markdown_files": len(md_files),
    }


def get_pr_stats_from_gh() -> dict:
    """
    Requires GitHub CLI:
        gh auth login

    Returns empty stats if gh is unavailable or repo is not connected.
    """
    gh_version = safe_run_command(["gh", "--version"])
    if not gh_version:
        return {
            "available": False,
            "total_prs": 0,
            "merged_prs": 0,
            "open_prs": 0,
            "prs": [],
        }

    prs_json = safe_run_command(
        [
            "gh",
            "pr",
            "list",
            "--state",
            "all",
            "--limit",
            "200",
            "--json",
            "number,title,state,author",
        ]
    )

    if not prs_json:
        return {
            "available": False,
            "total_prs": 0,
            "merged_prs": 0,
            "open_prs": 0,
            "prs": [],
        }

    try:
        prs = json.loads(prs_json)
    except json.JSONDecodeError:
        prs = []

    merged_count = 0
    open_count = 0

    for pr in prs:
        state = str(pr.get("state", "")).upper()
        if state == "MERGED":
            merged_count += 1
        elif state == "OPEN":
            open_count += 1

    return {
        "available": True,
        "total_prs": len(prs),
        "merged_prs": merged_count,
        "open_prs": open_count,
        "prs": prs,
    }


def get_pr_counts_by_author(prs: list[dict]) -> dict[str, int]:
    counts = Counter()

    for pr in prs:
        author = pr.get("author")

        if isinstance(author, dict):
            login = normalize_text(author.get("login", ""))
            if login:
                counts[login] += 1

    return dict(counts)


def build_git_name_to_member_map() -> dict[str, str]:
    """
    Build a case-insensitive lookup:
        contributor identifier -> official team member name

    Supports matching by:
    - official team member name
    - github username
    - git_names aliases
    """
    mapping = {}

    for member in TEAM_MEMBERS:
        official_name = member["name"]

        # Match official name
        mapping[normalize_text(member["name"])] = official_name

        # Match GitHub username
        github_name = member.get("github", "")
        if github_name:
            mapping[normalize_text(github_name)] = official_name

        # Match git aliases
        for git_name in member.get("git_names", []):
            mapping[normalize_text(git_name)] = official_name

    return mapping


def build_team_contribution_summary(
    contributors: list[dict],
    pr_stats: dict,
) -> list[dict]:
    """
    Combine contributor statistics and PR statistics into a full team roster summary.

    - All official team members are included
    - Team members with no contributions still appear with 0 values
    - Non-team contributors such as github-actions are excluded
    - Matching is case-insensitive across name / github / git_names
    """
    git_name_to_member = build_git_name_to_member_map()
    pr_counts = get_pr_counts_by_author(pr_stats["prs"]) if pr_stats["available"] else {}

    # Initialize every team member with zero values
    summary_lookup = {
        member["name"]: {
            "name": member["name"],
            "github": member["github"],
            "commits": 0,
            "files_changed": 0,
            "insertions": 0,
            "deletions": 0,
            "prs": 0,
            "status": "No contributions yet",
        }
        for member in TEAM_MEMBERS
    }

    # Merge Git contributor statistics into matching team member
    for contributor in contributors:
        raw_name = normalize_text(contributor["name"])

        # Ignore non-team contributors like github-actions
        if raw_name not in git_name_to_member:
            continue

        official_name = git_name_to_member[raw_name]
        row = summary_lookup[official_name]

        row["commits"] += contributor["commits"]
        row["files_changed"] += contributor["files_changed"]
        row["insertions"] += contributor["insertions"]
        row["deletions"] += contributor["deletions"]

    # Merge PR counts using github + official name as possible keys
    for member in TEAM_MEMBERS:
        official_name = member["name"]
        row = summary_lookup[official_name]

        possible_pr_keys = {
            normalize_text(member.get("github", "")),
            normalize_text(member.get("name", "")),
        }

        for alias in member.get("git_names", []):
            possible_pr_keys.add(normalize_text(alias))

        for key in possible_pr_keys:
            if key:
                row["prs"] += pr_counts.get(key, 0)

        if row["commits"] > 0 or row["prs"] > 0:
            row["status"] = "Active contributor"

    return list(summary_lookup.values())


def build_filtered_contributor_statistics(
    contributors: list[dict],
) -> list[dict]:
    """
    Return contributor statistics restricted to official team members only.

    - Removes bot/system contributors such as github-actions
    - Merges multiple aliases into the official team member name
    - Matching is case-insensitive across name / github / git_names
    """
    git_name_to_member = build_git_name_to_member_map()

    merged = {
        member["name"]: {
            "name": member["name"],
            "commits": 0,
            "files_changed": 0,
            "insertions": 0,
            "deletions": 0,
        }
        for member in TEAM_MEMBERS
    }

    for contributor in contributors:
        raw_name = normalize_text(contributor["name"])

        # Skip anything not mapped to a team member
        if raw_name not in git_name_to_member:
            continue

        official_name = git_name_to_member[raw_name]
        row = merged[official_name]

        row["commits"] += contributor["commits"]
        row["files_changed"] += contributor["files_changed"]
        row["insertions"] += contributor["insertions"]
        row["deletions"] += contributor["deletions"]

    return sorted(merged.values(), key=lambda x: x["commits"], reverse=True)


def build_markdown() -> str:
    total_commits = get_total_commits()
    branches = get_branches()
    contributors = get_contributor_stats()
    commit_types = get_commit_type_distribution()
    file_counts = get_file_counts()
    pr_stats = get_pr_stats_from_gh()

    team_summary = build_team_contribution_summary(contributors, pr_stats)
    filtered_contributors = build_filtered_contributor_statistics(contributors)

    lines = []

    lines.append("## Repository Statistics")
    lines.append("")
    lines.append(f"**Total Commits:** {total_commits}  ")
    lines.append(f"**Total Team Members:** {len(TEAM_MEMBERS)}  ")
    lines.append(f"**Branches Created:** {len(branches)}")
    lines.append("")

    lines.append("### Team Contribution Summary")
    lines.append("")
    lines.append("| Team Member | GitHub Username | Commits | PRs | Files Changed | Insertions | Deletions | Status |")
    lines.append("|-------------|-----------------|---------|-----|---------------|------------|-----------|--------|")
    for member in team_summary:
        github_display = f"`{member['github']}`" if member["github"] else "-"
        lines.append(
            f"| {member['name']} | {github_display} | {member['commits']} | {member['prs']} | {member['files_changed']} | {member['insertions']} | {member['deletions']} | {member['status']} |"
        )
    lines.append("")

    lines.append("### Contributor Statistics")
    lines.append("")
    lines.append("| Contributor | Commits | Files Changed | Insertions | Deletions |")
    lines.append("|-------------|---------|---------------|------------|-----------|")
    for c in filtered_contributors:
        lines.append(
            f"| {c['name']} | {c['commits']} | {c['files_changed']} | {c['insertions']} | {c['deletions']} |"
        )
    lines.append("")

    lines.append("### Commit Type Distribution")
    lines.append("")
    lines.append("| Type | Count |")
    lines.append("|------|------:|")
    for commit_type, count in sorted(commit_types.items()):
        lines.append(f"| {commit_type} | {count} |")
    lines.append("")

    lines.append("### File Metrics")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|------:|")
    lines.append(f"| Python Files | {file_counts['python_files']} |")
    lines.append(f"| Source Files | {file_counts['source_files']} |")
    lines.append(f"| Test Files | {file_counts['test_files']} |")
    lines.append(f"| Markdown Files | {file_counts['markdown_files']} |")
    lines.append("")

    #lines.append("### Branch List")
    #lines.append("")
    #for branch in branches:
    #    lines.append(f"- `{branch}`")
    #lines.append("")

    lines.append("### Pull Request Statistics")
    lines.append("")
    if pr_stats["available"]:
        lines.append(f"**Total Pull Requests:** {pr_stats['total_prs']}  ")
        lines.append(f"**Merged Pull Requests:** {pr_stats['merged_prs']}  ")
        lines.append(f"**Open Pull Requests:** {pr_stats['open_prs']}")
        lines.append("")

        lines.append("| PR # | Title | State | Author |")
        lines.append("|------|-------|-------|--------|")
        for pr in pr_stats["prs"]:
            author = ""
            if isinstance(pr.get("author"), dict):
                author = pr["author"].get("login", "")
            title = str(pr.get("title", "")).replace("|", "\\|")
            lines.append(
                f"| {pr.get('number', '')} | {title} | {pr.get('state', '')} | {author} |"
            )
    else:
        lines.append(
            "GitHub PR statistics are unavailable. Install and authenticate GitHub CLI with `gh auth login` to enable this section."
        )

    lines.append("")
    return "\n".join(lines)


def main() -> None:
    markdown = build_markdown()
    OUTPUT_MD.write_text(markdown, encoding="utf-8")
    print(f"Repository statistics written to: {OUTPUT_MD}")


if __name__ == "__main__":
    main()