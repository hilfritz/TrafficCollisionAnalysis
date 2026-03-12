# scripts/update_readme_stats.py
from __future__ import annotations

from pathlib import Path
from generate_repo_stats import build_markdown


PROJECT_ROOT = Path(__file__).resolve().parent.parent
README_PATH = PROJECT_ROOT / "README.md"

START_MARKER = "<!-- REPO-STATS:START -->"
END_MARKER = "<!-- REPO-STATS:END -->"


def main() -> None:
    readme = README_PATH.read_text(encoding="utf-8")
    stats_md = build_markdown()

    replacement = f"{START_MARKER}\n{stats_md}\n{END_MARKER}"

    if START_MARKER in readme and END_MARKER in readme:
        start_index = readme.index(START_MARKER)
        end_index = readme.index(END_MARKER) + len(END_MARKER)
        updated = readme[:start_index] + replacement + readme[end_index:]
    else:
        updated = readme.rstrip() + "\n\n" + replacement + "\n"

    README_PATH.write_text(updated, encoding="utf-8")
    print("README.md updated with repository statistics.")


if __name__ == "__main__":
    main()