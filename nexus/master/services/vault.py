"""
Secrets Vault — centralised, in-memory secret store for the Master Node.

Security model
--------------
Secrets (API keys, tokens, passwords) live ONLY on the Master.  Workers
never have direct access to the vault.  Instead, the Vault's `inject()`
method is called at dispatch time: it copies the relevant secrets into the
`injected_secrets` field of the TaskPayload immediately before the payload
is serialised and sent to Redis.

The `injected_secrets` field is:
  - Populated here, in-memory, on the Master.
  - Transmitted over Redis as part of the ARQ job payload (Redis should be
    on a private network or use TLS + AUTH in production).
  - Available to the worker handler via `task.injected_secrets["KEY_NAME"]`.
  - Excluded from `model_dump()` (the default serialiser) so secrets never
    appear in logs or API responses.
  - Garbage-collected with the TaskPayload object after the job completes.

Vault backends
--------------
The `Vault` class is intentionally backend-agnostic.  The default backend
is `EnvVaultBackend`, which reads secrets from environment variables (fine
for development and single-machine deployments).

To integrate a real secrets manager, implement `VaultBackend` and swap it
in at startup:

    from nexus.master.services.vault import Vault
    from my_integrations import HashiCorpVaultBackend

    vault = Vault(backend=HashiCorpVaultBackend(addr="https://vault.internal"))

Task-to-secret mapping
----------------------
Each task type declares which secret keys it needs in TASK_SECRET_KEYS.
The Vault injects only those keys — not the entire secret store — into each
payload.  This enforces least-privilege at the task level.

    TASK_SECRET_KEYS = {
        "llm.summarise": ["OPENAI_API_KEY"],
        "github.push":   ["GITHUB_TOKEN", "GITHUB_APP_ID"],
    }
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import Any

import structlog

from nexus.shared.schemas import TaskPayload

log = structlog.get_logger(__name__)


# ── Vault backend protocol ─────────────────────────────────────────────────────

class VaultBackend(ABC):
    """Abstract base for secret storage backends."""

    @abstractmethod
    def get(self, key: str) -> str | None:
        """Return the secret value for `key`, or None if not found."""

    @abstractmethod
    def set(self, key: str, value: str) -> None:
        """Store or update a secret."""

    @abstractmethod
    def delete(self, key: str) -> None:
        """Remove a secret."""

    @abstractmethod
    def list_keys(self) -> list[str]:
        """Return all registered secret key names (not values)."""


class EnvVaultBackend(VaultBackend):
    """
    Development backend: reads secrets from environment variables.

    Secrets are prefixed with NEXUS_SECRET_ to avoid collisions.
    Example: NEXUS_SECRET_OPENAI_API_KEY=sk-...

    For production, replace with HashiCorpVaultBackend, AWSSecretsBackend,
    or AzureKeyVaultBackend.
    """

    PREFIX = "NEXUS_SECRET_"

    def __init__(self) -> None:
        # In-memory overlay: set() writes here, shadowing env vars.
        self._overlay: dict[str, str] = {}

    def get(self, key: str) -> str | None:
        if key in self._overlay:
            return self._overlay[key]
        prefixed = os.environ.get(f"{self.PREFIX}{key}")
        if prefixed is not None and prefixed.strip() != "":
            return prefixed
        # Fall back to the conventional name (e.g. OPENAI_API_KEY in .env) so
        # operators are not forced to duplicate keys as NEXUS_SECRET_* only.
        plain = os.environ.get(key)
        if plain is not None and plain.strip() != "":
            return plain
        return None

    def set(self, key: str, value: str) -> None:
        self._overlay[key] = value

    def delete(self, key: str) -> None:
        self._overlay.pop(key, None)

    def list_keys(self) -> list[str]:
        env_keys = [
            k[len(self.PREFIX):]
            for k in os.environ
            if k.startswith(self.PREFIX)
        ]
        return list(set(env_keys) | set(self._overlay.keys()))


class InMemoryVaultBackend(VaultBackend):
    """
    Pure in-memory backend — useful for tests and ephemeral deployments.
    Secrets are lost on process restart.
    """

    def __init__(self, initial: dict[str, str] | None = None) -> None:
        self._store: dict[str, str] = dict(initial or {})

    def get(self, key: str) -> str | None:
        return self._store.get(key)

    def set(self, key: str, value: str) -> None:
        self._store[key] = value

    def delete(self, key: str) -> None:
        self._store.pop(key, None)

    def list_keys(self) -> list[str]:
        return list(self._store.keys())


class EncryptedFileVaultBackend(VaultBackend):
    """
    Production backend: Fernet-encrypted secrets stored in a local file.

    Secrets are AES-128-CBC encrypted at rest using the `cryptography`
    library's Fernet symmetric encryption.  The encryption key is derived
    from NEXUS_VAULT_KEY env var (a 32-byte URL-safe base64 string).

    Generate a key once and store it in .env:
        python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

    File format: JSON dict of {key: fernet_token_str}

    Security properties:
    - Each value is independently encrypted — a leaked file reveals nothing
      without the key.
    - The key itself is never written to disk by this class.
    - Suitable for single-machine deployments where the key is in memory.
    """

    def __init__(
        self,
        vault_path: str | None = None,
        encryption_key: str | None = None,
    ) -> None:
        import base64

        from cryptography.fernet import Fernet  # type: ignore[import-untyped]

        self._path = vault_path or os.path.join(
            os.path.dirname(__file__), ".vault.enc"
        )
        raw_key = encryption_key or os.getenv("NEXUS_VAULT_KEY", "")
        if not raw_key:
            log.warning(
                "encrypted_vault_no_key",
                hint="Set NEXUS_VAULT_KEY in .env — vault will use plaintext fallback",
            )
            self._fernet: Any = None
        else:
            try:
                self._fernet = Fernet(raw_key.encode())
            except Exception:
                # Try padding if key is raw bytes
                padded = base64.urlsafe_b64encode(raw_key.encode()[:32].ljust(32, b"\x00"))
                self._fernet = Fernet(padded)

        self._store: dict[str, str] = {}
        self._load()
        log.info(
            "encrypted_vault_init",
            path=self._path,
            encrypted=self._fernet is not None,
            keys=len(self._store),
        )

    def _load(self) -> None:
        import json
        if not os.path.exists(self._path):
            return
        try:
            with open(self._path, encoding="utf-8") as f:
                raw = json.load(f)
            for k, v in raw.items():
                if self._fernet:
                    try:
                        self._store[k] = self._fernet.decrypt(v.encode()).decode()
                    except Exception:
                        self._store[k] = v  # fallback: store as-is
                else:
                    self._store[k] = v
        except Exception as exc:
            log.error("encrypted_vault_load_error", error=str(exc))

    def _save(self) -> None:
        import json
        try:
            encrypted: dict[str, str] = {}
            for k, v in self._store.items():
                if self._fernet:
                    encrypted[k] = self._fernet.encrypt(v.encode()).decode()
                else:
                    encrypted[k] = v
            with open(self._path, "w", encoding="utf-8") as f:
                json.dump(encrypted, f, indent=2)
        except Exception as exc:
            log.error("encrypted_vault_save_error", error=str(exc))

    def get(self, key: str) -> str | None:
        return self._store.get(key)

    def set(self, key: str, value: str) -> None:
        self._store[key] = value
        self._save()

    def delete(self, key: str) -> None:
        self._store.pop(key, None)
        self._save()

    def list_keys(self) -> list[str]:
        return list(self._store.keys())


# ── Task → secret key mapping ──────────────────────────────────────────────────
# Maps task_type prefixes to the secret keys they require.
# The Vault injects only the listed keys into each payload.
# Use dot-notation prefixes: "llm" matches "llm.summarise", "llm.translate", etc.
#
# Extend this dict as you add new task types that need credentials.

TASK_SECRET_KEYS: dict[str, list[str]] = {
    # Example integrations — add real keys as you build handlers:
    "llm":      ["OPENAI_API_KEY", "ANTHROPIC_API_KEY"],
    "github":   ["GITHUB_TOKEN"],
    "slack":    ["SLACK_BOT_TOKEN"],
    "whatsapp": ["WHATSAPP_API_TOKEN", "WHATSAPP_PHONE_ID"],
    "system":   [],  # built-in smoke-test tasks need no secrets
    # Content Factory — Gemini API key injected at dispatch time
    "telegram.content_factory": ["GEMINI_API_KEY", "TELEFIX_BOT_TOKEN",
                                  "TELEFIX_API_ID", "TELEFIX_API_HASH"],
    "retention.guardian": [
        "TELEFIX_API_ID",
        "TELEFIX_API_HASH",
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_ADMIN_CHAT_ID",
    ],
    "swarm": ["GEMINI_API_KEY", "TELEFIX_API_ID", "TELEFIX_API_HASH"],
}


# ── Vault ──────────────────────────────────────────────────────────────────────

class Vault:
    """
    Central secrets manager for the Master Node.

    Usage
    -----
        vault = Vault()                          # uses EnvVaultBackend
        vault.set("OPENAI_API_KEY", "sk-...")    # store a secret at runtime

        # At dispatch time (called by Dispatcher):
        enriched_task = vault.inject(task)
        # enriched_task.injected_secrets == {"OPENAI_API_KEY": "sk-..."}
    """

    def __init__(self, backend: VaultBackend | None = None) -> None:
        self._backend = backend or EnvVaultBackend()
        log.info("vault_initialised", backend=type(self._backend).__name__)

    # ── Secret management ──────────────────────────────────────────────────────

    def set(self, key: str, value: str) -> None:
        """Store or update a secret in the vault."""
        self._backend.set(key, value)
        log.info("vault_secret_set", key=key)

    def delete(self, key: str) -> None:
        """Remove a secret from the vault."""
        self._backend.delete(key)
        log.info("vault_secret_deleted", key=key)

    def list_keys(self) -> list[str]:
        """Return all registered secret key names (values are never returned)."""
        return self._backend.list_keys()

    # ── Injection ──────────────────────────────────────────────────────────────

    def inject(self, task: TaskPayload) -> TaskPayload:
        """
        Return a new TaskPayload with `injected_secrets` populated.

        Looks up which secret keys the task's type requires (via
        TASK_SECRET_KEYS), fetches their values from the backend, and
        returns a new (frozen) TaskPayload with those secrets attached.

        Missing secrets are logged as warnings but do NOT block dispatch —
        the handler itself should raise a clear error if a required secret
        is absent, rather than failing silently here.

        The original `task` object is never mutated (TaskPayload is frozen).
        """
        required_keys = self._resolve_required_keys(task.task_type)

        if not required_keys:
            return task

        secrets: dict[str, str] = {}
        missing: list[str] = []

        for key in required_keys:
            value = self._backend.get(key)
            if value is not None:
                secrets[key] = value
            else:
                missing.append(key)

        if missing:
            log.warning(
                "vault_secrets_missing",
                task_type=task.task_type,
                task_id=task.task_id,
                missing_keys=missing,
            )

        if not secrets:
            return task

        # TaskPayload is frozen — use model_copy to produce a new instance.
        enriched = task.model_copy(update={"injected_secrets": secrets})
        log.debug(
            "vault_secrets_injected",
            task_id=task.task_id,
            task_type=task.task_type,
            injected_keys=list(secrets.keys()),
        )
        return enriched

    # ── Internal ───────────────────────────────────────────────────────────────

    def _resolve_required_keys(self, task_type: str) -> list[str]:
        """
        Find the secret keys for `task_type` by matching the longest
        prefix in TASK_SECRET_KEYS.

        "llm.summarise" → matches "llm" → returns ["OPENAI_API_KEY", ...]
        "system.echo"   → matches "system" → returns []
        "unknown.task"  → no match → returns []
        """
        parts = task_type.split(".")
        for length in range(len(parts), 0, -1):
            prefix = ".".join(parts[:length])
            if prefix in TASK_SECRET_KEYS:
                return TASK_SECRET_KEYS[prefix]
        return []

    def load_env_file(self, env_path: str, key_mapping: dict[str, str] | None = None) -> int:
        """
        Load secrets from an external .env file into the vault.

        Reads KEY=VALUE lines from `env_path` and stores them under their
        original names (or remapped names if `key_mapping` is provided).

        Parameters
        ----------
        env_path    : Absolute path to the .env file to load.
        key_mapping : Optional dict mapping original key → vault key.
                      e.g. {"BOT_TOKEN": "TELEFIX_BOT_TOKEN"}
                      Keys not in the mapping are stored under their original name.

        Returns the number of secrets successfully loaded.

        Example — load Mangement Ahu secrets at master startup:
            vault.load_env_file(
                r"C:\\Users\\Yarin\\Desktop\\Mangement Ahu\\.env",
                key_mapping={
                    "BOT_TOKEN":  "TELEFIX_BOT_TOKEN",
                    "API_ID":     "TELEFIX_API_ID",
                    "API_HASH":   "TELEFIX_API_HASH",
                },
            )
        """
        import os as _os

        if not _os.path.exists(env_path):
            log.warning("vault_env_file_not_found", path=env_path)
            return 0

        loaded = 0
        try:
            with open(env_path, encoding="utf-8") as f:
                for raw_line in f:
                    line = raw_line.strip()
                    # Skip blank lines and comments
                    if not line or line.startswith("#"):
                        continue
                    if "=" not in line:
                        continue
                    original_key, _, raw_value = line.partition("=")
                    original_key = original_key.strip()
                    # Strip surrounding quotes from value
                    value = raw_value.strip().strip('"').strip("'")
                    if not original_key or not value:
                        continue
                    vault_key = (key_mapping or {}).get(original_key, original_key)
                    self._backend.set(vault_key, value)
                    loaded += 1

            log.info(
                "vault_env_file_loaded",
                path=env_path,
                secrets_loaded=loaded,
            )
        except Exception as exc:
            log.error("vault_env_file_error", path=env_path, error=str(exc))

        return loaded

    def register_task_secrets(self, task_type_prefix: str, keys: list[str]) -> None:
        """
        Register which secret keys a task type prefix requires.

        Call this at startup to extend the default TASK_SECRET_KEYS mapping
        without editing this file.
        """
        TASK_SECRET_KEYS[task_type_prefix] = keys
        log.info(
            "vault_task_secrets_registered",
            prefix=task_type_prefix,
            keys=keys,
        )

    def audit_summary(self) -> dict[str, Any]:
        """Return a non-sensitive summary for health checks and logging."""
        return {
            "backend": type(self._backend).__name__,
            "registered_keys": len(self.list_keys()),
            "task_type_mappings": len(TASK_SECRET_KEYS),
        }
