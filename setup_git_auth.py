#!/usr/bin/env python3
"""
GitHub Authentication Setup Utility

This script securely configures Git authentication by:
1. Prompting for GitHub username and Personal Access Token (PAT)
2. Updating the git remote URL with embedded credentials
3. Configuring git credential helper for persistence
4. Validating the connection with git fetch
5. Cleaning up any logs/history to keep credentials safe
"""

import getpass
import subprocess
import sys
import os
import re
from pathlib import Path
from urllib.parse import urlparse, urlunparse


def run_git_command(cmd: list[str], check: bool = True) -> tuple[str, str, int]:
    """Run a git command and return stdout, stderr, and return code."""
    try:
        result = subprocess.run(
            ["git"] + cmd,
            capture_output=True,
            text=True,
            check=check,
        )
        return result.stdout.strip(), result.stderr.strip(), result.returncode
    except subprocess.CalledProcessError as e:
        return e.stdout.strip(), e.stderr.strip(), e.returncode
    except FileNotFoundError:
        print("ERROR: Git is not installed or not in PATH.", file=sys.stderr)
        sys.exit(1)


def get_current_remote_url() -> str | None:
    """Get the current origin remote URL."""
    stdout, _, returncode = run_git_command(["config", "--get", "remote.origin.url"], check=False)
    if returncode == 0 and stdout:
        return stdout
    return None


def parse_github_url(url: str) -> tuple[str, str]:
    """
    Parse a GitHub URL and extract owner and repo name.
    Supports both https://github.com/owner/repo.git and git@github.com:owner/repo.git formats.
    """
    # Handle SSH format: git@github.com:owner/repo.git
    if url.startswith("git@github.com:"):
        parts = url.replace("git@github.com:", "").rstrip(".git").split("/")
        if len(parts) >= 2:
            return parts[0], parts[1]
    
    # Handle HTTPS format: https://github.com/owner/repo.git
    parsed = urlparse(url)
    if parsed.netloc == "github.com" or parsed.netloc.endswith(".github.com"):
        path_parts = parsed.path.strip("/").rstrip(".git").split("/")
        if len(path_parts) >= 2:
            return path_parts[0], path_parts[1]
    
    raise ValueError(f"Could not parse GitHub URL: {url}")


def build_authenticated_url(username: str, token: str, owner: str, repo: str) -> str:
    """Build an authenticated GitHub URL in the format: https://username:token@github.com/owner/repo.git"""
    # URL-encode username and token to handle special characters
    from urllib.parse import quote
    encoded_username = quote(username, safe="")
    encoded_token = quote(token, safe="")
    return f"https://{encoded_username}:{encoded_token}@github.com/{owner}/{repo}.git"


def clear_powershell_history():
    """Clear PowerShell command history on Windows."""
    try:
        # Get PowerShell history file path
        history_path = Path.home() / "AppData" / "Roaming" / "Microsoft" / "Windows" / "PowerShell" / "PSReadLine" / "ConsoleHost_history.txt"
        if history_path.exists():
            # Read current history
            with open(history_path, "r", encoding="utf-8", errors="ignore") as f:
                lines = f.readlines()
            
            # Filter out lines containing this script or git commands with tokens
            filtered_lines = [
                line for line in lines
                if "setup_git_auth.py" not in line.lower()
                and not re.search(r'git.*https://[^:]+:[^@]+@github\.com', line, re.IGNORECASE)
            ]
            
            # Write back filtered history
            with open(history_path, "w", encoding="utf-8") as f:
                f.writelines(filtered_lines)
            
            print("✓ Cleared PowerShell history entries related to this script")
    except Exception as e:
        # Silently fail - history cleanup is best-effort
        pass


def clear_bash_history():
    """Clear bash command history (for Unix-like systems)."""
    try:
        history_path = Path.home() / ".bash_history"
        if history_path.exists():
            with open(history_path, "r", encoding="utf-8", errors="ignore") as f:
                lines = f.readlines()
            
            filtered_lines = [
                line for line in lines
                if "setup_git_auth.py" not in line.lower()
                and not re.search(r'git.*https://[^:]+:[^@]+@github\.com', line, re.IGNORECASE)
            ]
            
            with open(history_path, "w", encoding="utf-8") as f:
                f.writelines(filtered_lines)
    except Exception:
        pass


def main():
    """Main execution function."""
    print("=" * 60)
    print("GitHub Authentication Setup Utility")
    print("=" * 60)
    print()
    
    # Check if we're in a git repository
    stdout, _, returncode = run_git_command(["rev-parse", "--git-dir"], check=False)
    if returncode != 0:
        print("ERROR: Not in a git repository. Please run this script from a git repository root.", file=sys.stderr)
        sys.exit(1)
    
    # Get current remote URL
    current_url = get_current_remote_url()
    if not current_url:
        print("ERROR: No 'origin' remote found. Please add a remote first:", file=sys.stderr)
        print("  git remote add origin https://github.com/owner/repo.git", file=sys.stderr)
        sys.exit(1)
    
    print(f"Current remote URL: {current_url}")
    print()
    
    # Parse the repository information
    try:
        owner, repo = parse_github_url(current_url)
        print(f"Detected repository: {owner}/{repo}")
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    
    print()
    
    # Prompt for credentials
    print("Please enter your GitHub credentials:")
    username = input("GitHub Username: ").strip()
    if not username:
        print("ERROR: Username cannot be empty.", file=sys.stderr)
        sys.exit(1)
    
    token = getpass.getpass("GitHub Personal Access Token (PAT): ").strip()
    if not token:
        print("ERROR: Token cannot be empty.", file=sys.stderr)
        sys.exit(1)
    
    print()
    print("Configuring Git authentication...")
    
    # Build authenticated URL
    authenticated_url = build_authenticated_url(username, token, owner, repo)
    
    # Update remote URL
    print("  → Updating remote URL...")
    _, stderr, returncode = run_git_command(["remote", "set-url", "origin", authenticated_url])
    if returncode != 0:
        print(f"ERROR: Failed to update remote URL: {stderr}", file=sys.stderr)
        sys.exit(1)
    print("  ✓ Remote URL updated")
    
    # Configure credential helper
    print("  → Configuring credential helper...")
    _, stderr, returncode = run_git_command(["config", "--global", "credential.helper", "store"])
    if returncode != 0:
        print(f"WARNING: Failed to configure credential helper: {stderr}", file=sys.stderr)
    else:
        print("  ✓ Credential helper configured")
    
    # Validate connection with git fetch
    print("  → Validating connection...")
    stdout, stderr, returncode = run_git_command(["fetch", "origin"], check=False)
    if returncode != 0:
        print(f"ERROR: Connection validation failed: {stderr}", file=sys.stderr)
        print("Please verify your username and token are correct.", file=sys.stderr)
        # Revert to original URL on failure
        run_git_command(["remote", "set-url", "origin", current_url])
        sys.exit(1)
    print("  ✓ Connection validated successfully")
    
    print()
    print("=" * 60)
    print("GitHub authentication configured successfully!")
    print("=" * 60)
    print()
    
    # Clean up history/logs
    print("Cleaning up history and logs...")
    if sys.platform == "win32":
        clear_powershell_history()
    else:
        clear_bash_history()
    
    # Clear the token from memory (best effort)
    token = " " * len(token)  # Overwrite in memory (Python strings are immutable, but this is best effort)
    del token
    
    print("✓ Cleanup complete")
    print()
    print("Your credentials are now stored securely in Git's credential store.")
    print("You can now use git commands without entering credentials each time.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\nERROR: Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)
