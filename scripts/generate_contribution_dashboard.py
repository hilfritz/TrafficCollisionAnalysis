from __future__ import annotations

import json
import subprocess
from collections import Counter
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


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


def ensure_all_members(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    rows = df.to_dict("records") if not df.empty else []
    existing = set(df["author"].tolist()) if not df.empty else set()

    for member in TEAM_MEMBERS:
        if member["name"] not in existing:
            rows.append({"author": member["name"], value_col: 0})

    result = pd.DataFrame(rows)
    return result.sort_values("author").reset_index(drop=True)


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


def build_summary(commit_df: pd.DataFrame, pr_df: pd.DataFrame) -> pd.DataFrame:
    commit_counts = (
        commit_df.groupby("author").size().reset_index(name="commits")
        if not commit_df.empty
        else pd.DataFrame(columns=["author", "commits"])
    )
    commit_counts = ensure_all_members(commit_counts, "commits")

    if pr_df.empty:
        pr_df = pd.DataFrame({"author": [m["name"] for m in TEAM_MEMBERS], "prs": [0] * len(TEAM_MEMBERS)})
    else:
        pr_df = ensure_all_members(pr_df, "prs")

    summary = commit_counts.merge(pr_df, on="author", how="left")
    summary["prs"] = summary["prs"].fillna(0).astype(int)
    summary = summary.sort_values(["commits", "prs"], ascending=[False, False]).reset_index(drop=True)
    return summary


def main() -> None:
    commit_rows = get_commit_rows()
    commit_df = map_author_to_team(commit_rows)
    pr_df = get_pr_counts_by_author()
    summary = build_summary(commit_df, pr_df)

    commit_counts = summary[["author", "commits"]].copy()

    if commit_df.empty:
        timeline = pd.DataFrame(columns=["date", "commits"])
    else:
        timeline = (
            commit_df.groupby(commit_df["date"].dt.date)
            .size()
            .reset_index(name="commits")
            .rename(columns={"date": "day"})
        )
        timeline["day"] = pd.to_datetime(timeline["day"])
        timeline = timeline.sort_values("day").reset_index(drop=True)

    pr_counts = summary[["author", "prs"]].copy()

    fig = plt.figure(figsize=(16, 10))
    gs = fig.add_gridspec(2, 2, height_ratios=[1, 1.1])

    ax1 = fig.add_subplot(gs[0, 0])
    ax1.bar(commit_counts["author"], commit_counts["commits"])
    ax1.set_title("Commits by Team Member")
    ax1.set_xlabel("Team Member")
    ax1.set_ylabel("Commits")
    ax1.tick_params(axis="x", rotation=20)

    ax2 = fig.add_subplot(gs[0, 1])
    if not timeline.empty:
        ax2.plot(timeline["day"], timeline["commits"], marker="o")
    ax2.set_title("Commit Activity Over Time")
    ax2.set_xlabel("Date")
    ax2.set_ylabel("Commits")
    ax2.tick_params(axis="x", rotation=20)

    ax3 = fig.add_subplot(gs[1, 0])
    ax3.bar(pr_counts["author"], pr_counts["prs"])
    ax3.set_title("Pull Requests by Team Member")
    ax3.set_xlabel("Team Member")
    ax3.set_ylabel("PRs")
    ax3.tick_params(axis="x", rotation=20)

    ax4 = fig.add_subplot(gs[1, 1])
    ax4.axis("off")
    table_data = [["Team Member", "Commits", "PRs"]]
    for _, row in summary.iterrows():
        table_data.append([row["author"], int(row["commits"]), int(row["prs"])])

    table = ax4.table(cellText=table_data, loc="center", cellLoc="center")
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.6)
    ax4.set_title("Team Contribution Summary", pad=20)

    fig.suptitle("Repository Contribution Dashboard", fontsize=16)
    fig.tight_layout(rect=[0, 0, 1, 0.96])

    out_path = OUTPUT_DIR / "contribution_dashboard.png"
    fig.savefig(out_path, dpi=220, bbox_inches="tight")
    plt.close(fig)

    summary.to_csv(OUTPUT_DIR / "contribution_dashboard_summary.csv", index=False)

    print(f"Dashboard image written to: {out_path}")
    print(f"Summary CSV written to: {OUTPUT_DIR / 'contribution_dashboard_summary.csv'}")


if __name__ == "__main__":
    main()