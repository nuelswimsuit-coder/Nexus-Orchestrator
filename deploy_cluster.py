"""
Nexus Orchestrator — Multi-Node Cluster Deployment Engine
==========================================================
Packages the project and pushes it to all registered worker nodes
(Linux + Windows) over SSH/SCP using Paramiko.

Usage:
    python deploy_cluster.py
    python deploy_cluster.py --linux-only
    python deploy_cluster.py --windows-only
    python deploy_cluster.py --skip-install   (skip pip install on Linux)
"""

from __future__ import annotations

import argparse
import io
import os
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Optional

import paramiko
from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn
from rich.rule import Rule
from rich.text import Text

# ── Bootstrap ────────────────────────────────────────────────────────────────

load_dotenv()
console = Console()

# ── Config ───────────────────────────────────────────────────────────────────

WORKER_IP: str           = os.getenv("WORKER_IP", "")
# Comma-separated list of Linux worker IPs. When set, each host is deployed in a
# loop: SSH failures on one host are logged and the next host is tried.
WORKER_IPS: str          = os.getenv("WORKER_IPS", "").strip()
SSH_USER: str            = os.getenv("WORKER_SSH_USER", "")
SSH_PASSWORD: str        = os.getenv("WORKER_SSH_PASSWORD", "")
DEPLOY_ROOT_LINUX: str   = os.getenv("WORKER_DEPLOY_ROOT_LINUX", "/home/yadmin/Desktop/Nexus-Orchestrator")
# Windows target is always the canonical desktop path per deployment spec.
DEPLOY_ROOT_WIN: str     = r"C:\Users\Yarin\Desktop\Nexus-Orchestrator"

PROJECT_ROOT: Path = Path(__file__).parent.resolve()
ARCHIVE_NAME: str  = "nexus_deploy.zip"

# Files / directories that must never be transferred to worker nodes.
IGNORE_PATTERNS: frozenset[str] = frozenset({
    ".git",
    "venv",
    ".venv",
    "__pycache__",
    ".env",         # workers keep their own local .env
    "logs",
    "*.pyc",
    "*.pyo",
    ".mypy_cache",
    ".ruff_cache",
    "dist",
    "build",
    "*.egg-info",
    ARCHIVE_NAME,
})


# ── Helpers ──────────────────────────────────────────────────────────────────

def _should_ignore(path: Path) -> bool:
    """Return True if path (relative to project root) should be excluded."""
    parts = path.parts
    for pattern in IGNORE_PATTERNS:
        if "*" in pattern:
            if path.match(pattern):
                return True
        else:
            if pattern in parts:
                return True
    return False


def _validate_env() -> bool:
    """Check that required environment variables are set."""
    missing = [var for var, val in {
        "WORKER_IP": WORKER_IP,
        "WORKER_SSH_USER": SSH_USER,
        "WORKER_SSH_PASSWORD": SSH_PASSWORD,
    }.items() if not val]

    if missing:
        console.print(
            Panel(
                f"[bold red]Missing required .env variables:[/bold red]\n"
                + "\n".join(f"  • {v}" for v in missing),
                title="[red]Configuration Error[/red]",
                border_style="red",
            )
        )
        return False
    return True


def _open_ssh(host: str, user: str, password: str, port: int = 22) -> Optional[paramiko.SSHClient]:
    """Open an SSH connection.  Returns None on failure (node offline)."""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(
            hostname=host,
            port=port,
            username=user,
            password=password,
            timeout=15,
            banner_timeout=20,
            auth_timeout=20,
            allow_agent=False,
            look_for_keys=False,
        )
        return client
    except (
        paramiko.AuthenticationException,
        paramiko.SSHException,
        OSError,
        TimeoutError,
    ) as exc:
        console.print(f"    [red]✗ Cannot reach {host}:{port} — {exc}[/red]")
        return None


def _run_remote(ssh: paramiko.SSHClient, cmd: str) -> tuple[int, str, str]:
    """Execute a command on the remote host.  Returns (exit_code, stdout, stderr)."""
    _, stdout, stderr = ssh.exec_command(cmd, timeout=300)
    exit_code = stdout.channel.recv_exit_status()
    return exit_code, stdout.read().decode(errors="replace"), stderr.read().decode(errors="replace")


def _scp_upload(ssh: paramiko.SSHClient, local_path: Path, remote_path: str) -> None:
    """Upload a single file via SFTP (compatible SCP replacement)."""
    with ssh.open_sftp() as sftp:
        sftp.put(str(local_path), remote_path)


# ── Step 1: Package ──────────────────────────────────────────────────────────

def step_package(tmp_dir: Path) -> Path:
    """Zip the project, respecting the ignore list.  Returns path to the zip."""
    zip_path = tmp_dir / ARCHIVE_NAME

    console.print()
    console.print(Rule("[bold yellow]📦  Step 1 — Packaging Project[/bold yellow]"))

    file_list: list[Path] = []
    for item in PROJECT_ROOT.rglob("*"):
        if item.is_file():
            rel = item.relative_to(PROJECT_ROOT)
            if not _should_ignore(rel):
                file_list.append(item)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[cyan]{task.completed}/{task.total} files"),
        console=console,
    ) as progress:
        task = progress.add_task("[yellow]Compressing…", total=len(file_list))

        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            for file in file_list:
                rel = file.relative_to(PROJECT_ROOT)
                zf.write(file, rel)
                progress.advance(task)

    size_kb = zip_path.stat().st_size / 1024
    console.print(
        f"    [green]✓ Archive ready:[/green] [cyan]{zip_path.name}[/cyan] "
        f"([bold]{size_kb:.1f} KB[/bold], {len(file_list)} files)"
    )
    return zip_path


# ── Step 2: Deploy → Linux ────────────────────────────────────────────────────

def step_deploy_linux(
    zip_path: Path,
    skip_install: bool = False,
    *,
    target_ip: str | None = None,
) -> bool:
    """Transfer archive to Linux worker and install dependencies."""
    host = (target_ip or WORKER_IP or "").strip()
    console.print()
    console.print(Rule(f"[bold cyan]🚀  Step 2 — Deploying to Linux Node ({host})[/bold cyan]"))

    ssh = _open_ssh(host, SSH_USER, SSH_PASSWORD)
    if ssh is None:
        console.print(
            f"    [bold red]⚠  Linux node {host} — SSH unavailable (offline or port 22 closed) — skipping.[/bold red]"
        )
        return False

    remote_zip = f"{DEPLOY_ROOT_LINUX}/{ARCHIVE_NAME}"

    try:
        # Ensure deploy root exists
        console.print(f"    [dim]→ Creating remote directory:[/dim] {DEPLOY_ROOT_LINUX}")
        code, _, err = _run_remote(ssh, f'mkdir -p "{DEPLOY_ROOT_LINUX}"')
        if code != 0:
            console.print(f"    [red]✗ mkdir failed: {err.strip()}[/red]")
            return False

        # Upload archive
        console.print(f"    [dim]→ Uploading archive to:[/dim] {remote_zip}")
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            progress.add_task("[cyan]Transferring…", total=None)
            _scp_upload(ssh, zip_path, remote_zip)
        console.print(f"    [green]✓ Upload complete[/green]")

        # Unzip (overwrite silently)
        console.print(f"    [dim]→ Extracting archive…[/dim]")
        code, out, err = _run_remote(
            ssh,
            f'cd "{DEPLOY_ROOT_LINUX}" && unzip -o "{ARCHIVE_NAME}" && rm -f "{ARCHIVE_NAME}"',
        )
        if code != 0:
            console.print(f"    [red]✗ Extraction failed (exit {code}): {err.strip()}[/red]")
            return False
        console.print("    [green]✓ Extraction complete[/green]")

        # pip install
        if not skip_install:
            console.print("    [dim]→ Running pip install -r requirements.txt…[/dim]")
            pip_cmd = (
                f'cd "{DEPLOY_ROOT_LINUX}" && '
                f'python3 -m pip install --quiet -r requirements.txt 2>&1'
            )
            code, out, err = _run_remote(ssh, pip_cmd)
            if code != 0:
                console.print(
                    f"    [yellow]⚠ pip install exited {code} — check requirements manually.[/yellow]\n"
                    f"    [dim]{err.strip()[:300]}[/dim]"
                )
            else:
                console.print("    [green]✓ Dependencies installed[/green]")
        else:
            console.print("    [dim]  pip install skipped (--skip-install)[/dim]")

        console.print(f"    [bold green]✓ Linux node sync complete[/bold green]")
        return True

    except Exception as exc:
        console.print(f"    [bold red]✗ Unexpected error on Linux node: {exc}[/bold red]")
        return False
    finally:
        ssh.close()


# ── Step 3: Deploy → Windows ─────────────────────────────────────────────────

def step_deploy_windows(zip_path: Path) -> bool:
    """Transfer archive to Windows worker and extract via PowerShell."""
    console.print()
    console.print(Rule(f"[bold magenta]🚀  Step 3 — Deploying to Windows Node ({WORKER_IP})[/bold magenta]"))

    # Windows SSH port is 22 by default (OpenSSH for Windows)
    ssh = _open_ssh(WORKER_IP, SSH_USER, SSH_PASSWORD, port=22)
    if ssh is None:
        # Try alternate common port for Windows SSH
        console.print("    [dim]  Retrying on port 2222…[/dim]")
        ssh = _open_ssh(WORKER_IP, SSH_USER, SSH_PASSWORD, port=2222)

    if ssh is None:
        console.print("    [bold red]⚠  Windows node is OFFLINE or SSH not available — skipping.[/bold red]")
        console.print(
            "    [yellow]ℹ  To enable SSH on Windows: install OpenSSH Server via "
            "'Settings → Optional Features'.[/yellow]"
        )
        return False

    # Normalise Windows path for remote commands (use PowerShell escaping)
    win_deploy_root = DEPLOY_ROOT_WIN
    remote_zip_win  = win_deploy_root + "\\" + ARCHIVE_NAME

    try:
        # Ensure deploy root exists
        console.print(f"    [dim]→ Creating remote directory:[/dim] {win_deploy_root}")
        code, _, err = _run_remote(
            ssh,
            f'powershell -Command "New-Item -ItemType Directory -Force -Path \'{win_deploy_root}\'"',
        )
        if code != 0:
            console.print(f"    [red]✗ Directory creation failed: {err.strip()}[/red]")
            return False

        # Upload archive
        console.print(f"    [dim]→ Uploading archive to:[/dim] {remote_zip_win}")
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            progress.add_task("[magenta]Transferring…", total=None)
            _scp_upload(ssh, zip_path, remote_zip_win.replace("\\", "/"))
        console.print("    [green]✓ Upload complete[/green]")

        # Extract using PowerShell Expand-Archive
        console.print("    [dim]→ Extracting archive via PowerShell…[/dim]")
        ps_cmd = (
            f"powershell -Command \""
            f"Expand-Archive -Path '{remote_zip_win}' "
            f"-DestinationPath '{win_deploy_root}' -Force; "
            f"Remove-Item -Path '{remote_zip_win}' -Force\""
        )
        code, out, err = _run_remote(ssh, ps_cmd)
        if code != 0:
            console.print(f"    [red]✗ Extraction failed (exit {code}): {err.strip()}[/red]")
            return False
        console.print("    [green]✓ Extraction complete[/green]")

        console.print(f"    [bold green]✓ Windows node sync complete[/bold green]")
        return True

    except Exception as exc:
        console.print(f"    [bold red]✗ Unexpected error on Windows node: {exc}[/bold red]")
        return False
    finally:
        ssh.close()


# ── Step 4: Post-deploy summary ───────────────────────────────────────────────

def step_finalize(linux_ok: bool, windows_ok: bool) -> None:
    console.print()
    console.print(Rule("[bold green]✅  Step 4 — Cluster Sync Report[/bold green]"))

    rows: list[tuple[str, str, str]] = [
        ("Linux Worker",   DEPLOY_ROOT_LINUX, "[bold green]✓ Synced[/bold green]" if linux_ok   else "[bold red]✗ Offline / Failed[/bold red]"),
        ("Windows Worker", DEPLOY_ROOT_WIN,   "[bold green]✓ Synced[/bold green]" if windows_ok else "[bold red]✗ Offline / Failed[/bold red]"),
    ]

    for node, path, status in rows:
        console.print(f"    {status}  [bold]{node}[/bold]  [dim]{path}[/dim]")

    console.print()

    if linux_ok or windows_ok:
        console.print(
            Panel(
                "[bold green]✅  Cluster sync finished successfully for at least one node.[/bold green]\n\n"
                "Worker processes are now running the latest code on the hosts that synced.\n"
                "[yellow]→ Use the Dashboard's [bold]'Restart Cluster'[/bold] button (or "
                "re-run the worker scripts) to apply changes.[/yellow]",
                title="[green]Deployment Successful[/green]",
                border_style="green",
            )
        )
    else:
        console.print(
            Panel(
                "[bold red]All nodes were unreachable.[/bold red]\n\n"
                "• Verify SSH credentials in [cyan].env[/cyan]\n"
                "• Confirm nodes are online and reachable at [cyan]WORKER_IP[/cyan]\n"
                "• Ensure SSH service is running on each node",
                title="[red]Deployment Failed[/red]",
                border_style="red",
            )
        )


# ── CLI Entry-point ───────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Nexus Orchestrator — Multi-Node Cluster Deployment Engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--linux-only",    action="store_true", help="Deploy to Linux node only")
    parser.add_argument("--windows-only",  action="store_true", help="Deploy to Windows node only")
    parser.add_argument("--skip-install",  action="store_true", help="Skip pip install on Linux node")
    args = parser.parse_args()

    # ── Banner ────────────────────────────────────────────────────────────────
    console.print()
    console.print(
        Panel(
            Text.assemble(
                ("Nexus Orchestrator\n", "bold white"),
                ("Multi-Node Cluster Deployment Engine\n\n", "bold cyan"),
                ("Target  → ", "dim"),    (f"{WORKER_IP}\n", "cyan"),
                ("Linux   → ", "dim"),    (f"{DEPLOY_ROOT_LINUX}\n", "green"),
                ("Windows → ", "dim"),    (f"{DEPLOY_ROOT_WIN}", "magenta"),
            ),
            title="[bold blue]🛰  Nexus Deploy[/bold blue]",
            border_style="blue",
        )
    )

    if not _validate_env():
        sys.exit(1)

    # ── Resolve deployment targets ────────────────────────────────────────────
    deploy_linux   = not args.windows_only
    deploy_windows = not args.linux_only

    linux_ok   = False
    windows_ok = False

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)

        # Step 1 — always package
        zip_path = step_package(tmp_dir)

        # Step 2 — Linux (optional multi-host via WORKER_IPS)
        if deploy_linux:
            if WORKER_IPS:
                parts = [p.strip() for p in WORKER_IPS.split(",") if p.strip()]
                console.print(
                    f"\n    [cyan]WORKER_IPS[/cyan] set — deploying Linux to {len(parts)} host(s) sequentially."
                )
                linux_ok = False
                for idx, ip in enumerate(parts):
                    console.print(f"\n    [dim]— Linux target {idx + 1}/{len(parts)}: {ip}[/dim]")
                    if step_deploy_linux(zip_path, skip_install=args.skip_install, target_ip=ip):
                        linux_ok = True
                    else:
                        console.print(
                            f"    [yellow]↪ Continuing with next worker after failure on {ip}[/yellow]"
                        )
            else:
                linux_ok = step_deploy_linux(zip_path, skip_install=args.skip_install)
        else:
            console.print("\n    [dim]Linux deployment skipped (--windows-only)[/dim]")

        # Step 3 — Windows
        if deploy_windows:
            windows_ok = step_deploy_windows(zip_path)
        else:
            console.print("\n    [dim]Windows deployment skipped (--linux-only)[/dim]")

    # Step 4 — Summary
    step_finalize(linux_ok, windows_ok)

    exit_code = 0 if (linux_ok or windows_ok) else 1
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
