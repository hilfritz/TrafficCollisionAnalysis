from __future__ import annotations

import json
import subprocess
from collections import Counter, defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

#PYTHONPATH=. python scripts/generate_contribution_charts.py

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "repo_charts"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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


def build_git_name_to_member_map() -> dict[str, str]:
    mapping: dict[str, str] = {}
    for member in TEAM_MEMBERS:
        official_name = member["name"]
        mapping[normalize_text(member["name"])] = official_name
        github_name = member.get("github", "")
        if github_name:
            mapping[normalize_text(github_name)] = official_name
        for git_name in member.get("git_names", []):
            mapping[normalize_text(git_name)] = official_name
    return mapping


def get_commit_rows() -> list[dict]:
    output = safe_run_command(
        ["git", "log", "--date=short", "--pretty=format:%an|%ad|%s"]
    )
    rows: list[dict] = []
    if not output:
        return rows

    for line in output.splitlines():
        parts = line.split("|", 2)
        if len(parts) != 3:
            continue
        author, date_str, subject = parts
        rows.append(
            {
                "author": author.strip(),
                "date": date_str.strip(),
                "subject": subject.strip(),
            }
        )
    return rows


def map_author_to_team(commit_rows: list[dict]) -> pd.DataFrame:
    name_map = build_git_name_to_member_map()
    mapped_rows = []

    for row in commit_rows:
        raw_author = normalize_text(row["author"])
        official_name = name_map.get(raw_author)
        if not official_name:
            continue
        mapped_rows.append(
            {
                "author": official_name,
                "date": row["date"],
                "subject": row["subject"],
            }
        )

    df = pd.DataFrame(mapped_rows)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


def get_pr_counts_by_author() -> pd.DataFrame:
    gh_version = safe_run_command(["gh", "--version"])
    if not gh_version:
        return pd.DataFrame(columns=["author", "prs"])

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
            "author",
        ]
    )
    if not prs_json:
        return pd.DataFrame(columns=["author", "prs"])

    try:
        prs = json.loads(prs_json)
    except json.JSONDecodeError:
        return pd.DataFrame(columns=["author", "prs"])

    counts = Counter()
    name_map = build_git_name_to_member_map()

    for pr in prs:
        author = pr.get("author")
        if isinstance(author, dict):
            login = normalize_text(author.get("login", ""))
            if login in name_map:
                counts[name_map[login]] += 1

    rows = [{"author": k, "prs": v} for k, v in counts.items()]
    return pd.DataFrame(rows)


def ensure_all_members(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    existing = set(df["author"].tolist()) if not df.empty else set()
    rows = df.to_dict("records") if not df.empty else []

    for member in TEAM_MEMBERS:
        if member["name"] not in existing:
            rows.append({"author": member["name"], value_col: 0})

    result = pd.DataFrame(rows)
    return result.sort_values("author").reset_index(drop=True)


def save_commits_by_member_chart(commit_df: pd.DataFrame) -> Path:
    counts = (
        commit_df.groupby("author")
        .size()
        .reset_index(name="commits")
        if not commit_df.empty
        else pd.DataFrame(columns=["author", "commits"])
    )
    counts = ensure_all_members(counts, "commits")
    counts = counts.sort_values("commits", ascending=False).reset_index(drop=True)

    plt.figure(figsize=(10, 5))
    plt.bar(counts["author"], counts["commits"])
    plt.title("Commits by Team Member")
    plt.xlabel("Team Member")
    plt.ylabel("Number of Commits")
    plt.xticks(rotation=20, ha="right")
    plt.tight_layout()

    out_path = OUTPUT_DIR / "commits_by_member.png"
    plt.savefig(out_path, dpi=200)
    plt.close()
    return out_path


def save_commits_over_time_chart(commit_df: pd.DataFrame) -> Path:
    if commit_df.empty:
        timeline = pd.DataFrame({"date": [], "commits": []})
    else:
        timeline = (
            commit_df.groupby(commit_df["date"].dt.date)
            .size()
            .reset_index(name="commits")
            .rename(columns={"date": "day"})
        )
        timeline["day"] = pd.to_datetime(timeline["day"])

    plt.figure(figsize=(10, 5))
    if not timeline.empty:
        plt.plot(timeline["day"], timeline["commits"], marker="o")
    plt.title("Commit Activity Over Time")
    plt.xlabel("Date")
    plt.ylabel("Commits")
    plt.xticks(rotation=20)
    plt.tight_layout()

    out_path = OUTPUT_DIR / "commit_activity_over_time.png"
    plt.savefig(out_path, dpi=200)
    plt.close()
    return out_path


def save_prs_by_member_chart(pr_df: pd.DataFrame) -> Path | None:
    if pr_df.empty:
        return None

    pr_df = ensure_all_members(pr_df, "prs")
    pr_df = pr_df.sort_values("prs", ascending=False).reset_index(drop=True)

    plt.figure(figsize=(10, 5))
    plt.bar(pr_df["author"], pr_df["prs"])
    plt.title("Pull Requests by Team Member")
    plt.xlabel("Team Member")
    plt.ylabel("Number of PRs")
    plt.xticks(rotation=20, ha="right")
    plt.tight_layout()

    out_path = OUTPUT_DIR / "prs_by_member.png"
    plt.savefig(out_path, dpi=200)
    plt.close()
    return out_path


def save_summary_files(commit_df: pd.DataFrame, pr_df: pd.DataFrame) -> None:
    commit_counts = (
        commit_df.groupby("author")
        .size()
        .reset_index(name="commits")
        if not commit_df.empty
        else pd.DataFrame(columns=["author", "commits"])
    )
    commit_counts = ensure_all_members(commit_counts, "commits")

    summary = commit_counts.copy()
    if not pr_df.empty:
        pr_df = ensure_all_members(pr_df, "prs")
        summary = summary.merge(pr_df, on="author", how="left")
    else:
        summary["prs"] = 0

    summary["prs"] = summary["prs"].fillna(0).astype(int)
    summary = summary.sort_values(["commits", "prs"], ascending=[False, False]).reset_index(drop=True)

    summary.to_csv(OUTPUT_DIR / "team_contribution_summary.csv", index=False)

    md_lines = []
    md_lines.append("## Team Contribution Summary")
    md_lines.append("")
    md_lines.append("| Team Member | Commits | PRs |")
    md_lines.append("|-------------|---------|-----|")
    for _, row in summary.iterrows():
        md_lines.append(f"| {row['author']} | {row['commits']} | {row['prs']} |")
    (OUTPUT_DIR / "team_contribution_summary.md").write_text("\n".join(md_lines), encoding="utf-8")


def main() -> None:
    commit_rows = get_commit_rows()
    commit_df = map_author_to_team(commit_rows)
    pr_df = get_pr_counts_by_author()

    commits_chart = save_commits_by_member_chart(commit_df)
    timeline_chart = save_commits_over_time_chart(commit_df)
    prs_chart = save_prs_by_member_chart(pr_df)
    save_summary_files(commit_df, pr_df)

    print("Generated files:")
    print(f"- {commits_chart}")
    print(f"- {timeline_chart}")
    if prs_chart:
        print(f"- {prs_chart}")
    print(f"- {OUTPUT_DIR / 'team_contribution_summary.csv'}")
    print(f"- {OUTPUT_DIR / 'team_contribution_summary.md'}")


if __name__ == "__main__":
    main()