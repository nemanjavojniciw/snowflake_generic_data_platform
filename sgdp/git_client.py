"""
Git Client — Commit Generated Models

Wrapper around GitPython for committing platform-generated files:
  - Bronze/silver dbt models
  - sources.yml and schema.yml
  - source_metadata.yml stubs

Handles file writes, staging, and commits with platform metadata.

Usage:
  from platform.git_client import GitClient

  git = GitClient()
  git.commit_model("models/bronze/stripe/invoices.sql", sql_content, "regen bronze: stripe.invoices")
"""

import os
import re
from pathlib import Path
from typing import Optional

try:
    from git import Repo
    from git.exc import InvalidGitRepositoryError, GitCommandError
except ImportError:
    raise ImportError("GitPython not installed. Run: pip install gitpython")


class GitClient:
    """Commit generated files to git."""

    def __init__(self, repo_path: Optional[str] = None):
        """
        Initialize git client for repo.

        Args:
            repo_path: Path to git repo root. If None, uses current directory.
        """
        if repo_path is None:
            repo_path = os.getcwd()

        try:
            # search_parent_directories=True walks up until it finds .git
            self.repo = Repo(repo_path, search_parent_directories=True)
            self.repo_path = Path(self.repo.working_tree_dir)
        except InvalidGitRepositoryError:
            raise ValueError(f"Not a git repository: {repo_path}")

    def write_file(self, relative_path: str, content: str) -> Path:
        """
        Write a file relative to repo root.

        Args:
            relative_path: Path relative to repo root (e.g. 'models/bronze/stripe/invoices.sql')
            content: File content

        Returns:
            Absolute path to written file
        """
        abs_path = self.repo_path / relative_path
        abs_path.parent.mkdir(parents=True, exist_ok=True)
        abs_path.write_text(content, encoding="utf-8")
        return abs_path

    def stage_file(self, relative_path: str) -> None:
        """
        Stage a file for commit.

        Args:
            relative_path: Path relative to repo root
        """
        try:
            self.repo.index.add([relative_path])
        except GitCommandError as e:
            raise RuntimeError(f"Failed to stage {relative_path}: {e}") from e

    def commit(
        self,
        message: str,
        files: Optional[list[str]] = None,
        allow_empty: bool = False,
    ) -> Optional[str]:
        """
        Commit staged changes.

        Args:
            message: Commit message
            files: List of files to stage before commit (auto-stages if provided)
            allow_empty: If False, skip commit if nothing staged

        Returns:
            Commit hash, or None if skipped
        """
        # Stage files if provided
        if files:
            for f in files:
                self.stage_file(f)

        # Check if anything staged
        if not self.repo.index.diff("HEAD"):
            if not allow_empty:
                print(f"  ℹ️  No changes to commit (skipped)")
                return None

        try:
            commit = self.repo.index.commit(
                message,
                author_name="Platform Generator",
                author_email="platform@local",
            )
            return commit.hexsha[:8]
        except GitCommandError as e:
            raise RuntimeError(f"Failed to commit: {e}") from e

    def commit_model(
        self,
        model_path: str,
        content: str,
        commit_message: str,
    ) -> Optional[str]:
        """
        Write a dbt model file and commit it in one step.

        Args:
            model_path: Relative path to model (e.g. 'models/bronze/stripe/invoices.sql')
            content: SQL content
            commit_message: Commit message (will be prefixed with [platform])

        Returns:
            Commit hash or None if skipped
        """
        self.write_file(model_path, content)
        full_message = f"[platform] {commit_message}"
        return self.commit(full_message, files=[model_path], allow_empty=False)

    def get_current_branch(self) -> str:
        """Get current branch name."""
        return self.repo.active_branch.name

    def get_last_commit_message(self) -> str:
        """Get message from most recent commit."""
        return self.repo.head.commit.message.strip()

    def status(self) -> dict:
        """
        Get repo status.

        Returns:
            Dict with keys: branch, dirty, staged, untracked
        """
        return {
            "branch": self.get_current_branch(),
            "dirty": self.repo.is_dirty(),
            "staged": list(self.repo.index.diff("HEAD")),
            "untracked": self.repo.untracked_files,
        }
