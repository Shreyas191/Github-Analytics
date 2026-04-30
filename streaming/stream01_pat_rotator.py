"""
STREAM-01: GitHub PAT Round-Robin Rotator
GitHub Developer Ecosystem Analytics — Streaming Pipeline
Shreyas Kaldate (sk12898)

Each PAT gives 5,000 req/hr authenticated vs 60/hr unauthenticated.
With 4 PATs rotating: 20,000 req/hr total capacity.
"""

from __future__ import annotations

import time
import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class PATEntry:
    token: str
    alias: str                        # e.g. "shreyas", "hariharan"
    remaining: int = 5000             # GitHub resets this each hour
    reset_at: float = 0.0             # Unix timestamp when limit resets
    blocked: bool = False             # True if rate-limited right now


class PATRotator:
    """
    Round-robin rotator across multiple GitHub PATs.

    Usage:
        rotator = PATRotator([
            ("ghp_abc...", "shreyas"),
            ("ghp_def...", "hariharan"),
            ("ghp_ghi...", "tanusha"),
            ("ghp_jkl...", "vikram"),
        ])
        token = rotator.get_token()
        rotator.update_from_headers(token, response.headers)
    """

    def __init__(self, pat_list: list[tuple[str, str]]):
        """
        pat_list: list of (token_string, alias) tuples
        """
        if not pat_list:
            raise ValueError("At least one PAT is required.")
        self.pats: list[PATEntry] = [PATEntry(token=t, alias=a) for t, a in pat_list]
        self._index = 0
        logger.info(f"PATRotator initialized with {len(self.pats)} PATs: "
                    f"{[p.alias for p in self.pats]}")

    def get_token(self) -> str:
        """
        Return the next available PAT token, skipping blocked ones.
        Blocks the calling thread if ALL PATs are rate-limited (waits for earliest reset).
        """
        now = time.time()

        # Unblock any PATs whose reset window has passed
        for pat in self.pats:
            if pat.blocked and now >= pat.reset_at:
                pat.blocked = False
                pat.remaining = 5000
                logger.info(f"PAT '{pat.alias}' rate limit reset — back in rotation.")

        available = [p for p in self.pats if not p.blocked]

        if not available:
            # All PATs exhausted — sleep until the earliest reset
            earliest_reset = min(p.reset_at for p in self.pats)
            wait = max(0, earliest_reset - time.time()) + 5  # +5s buffer
            logger.warning(f"All PATs rate-limited. Sleeping {wait:.0f}s until reset.")
            time.sleep(wait)
            return self.get_token()  # Retry after sleep

        # Round-robin across available PATs only
        self._index = self._index % len(self.pats)
        for _ in range(len(self.pats)):
            pat = self.pats[self._index]
            self._index = (self._index + 1) % len(self.pats)
            if not pat.blocked:
                return pat.token

        # Fallback (shouldn't reach here given the check above)
        return available[0].token

    def update_from_headers(self, token: str, headers: dict):
        """
        Call this after every GitHub API response to track rate limit state.
        GitHub returns these headers on every response:
            X-RateLimit-Remaining: 4823
            X-RateLimit-Reset: 1718123456   (Unix timestamp)
        """
        pat = self._find_pat(token)
        if not pat:
            return

        remaining = headers.get("X-RateLimit-Remaining")
        reset_at = headers.get("X-RateLimit-Reset")

        if remaining is not None:
            pat.remaining = int(remaining)
        if reset_at is not None:
            pat.reset_at = float(reset_at)

        # Block this PAT if it's nearly exhausted (keep a buffer of 50 calls)
        if pat.remaining <= 50:
            pat.blocked = True
            wait_min = max(0, pat.reset_at - time.time()) / 60
            logger.warning(
                f"PAT '{pat.alias}' nearly exhausted ({pat.remaining} remaining). "
                f"Blocking for ~{wait_min:.1f} min until reset."
            )

    def mark_rate_limited(self, token: str, reset_at: float):
        """
        Call this when you receive a 403/429 rate limit response.
        Immediately blocks the PAT until its reset timestamp.
        """
        pat = self._find_pat(token)
        if pat:
            pat.blocked = True
            pat.reset_at = reset_at
            wait_min = max(0, reset_at - time.time()) / 60
            logger.warning(
                f"PAT '{pat.alias}' rate-limited (403/429). "
                f"Blocked for ~{wait_min:.1f} min."
            )

    def status(self) -> list[dict]:
        """Return current status of all PATs — useful for logging."""
        now = time.time()
        return [
            {
                "alias": p.alias,
                "remaining": p.remaining,
                "blocked": p.blocked,
                "resets_in_sec": max(0, p.reset_at - now) if p.blocked else None,
            }
            for p in self.pats
        ]

    def _find_pat(self, token: str) -> PATEntry | None:
        for p in self.pats:
            if p.token == token:
                return p
        return None


# ---------------------------------------------------------------------------
# Quick smoke test — run this file directly to verify rotation works
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    # Replace with real tokens before running
    PATS = [
        ("ghp_SHREYAS_TOKEN_HERE",    "shreyas"),
        ("ghp_HARIHARAN_TOKEN_HERE",  "hariharan"),
        ("ghp_TANUSHA_TOKEN_HERE",    "tanusha"),
        ("ghp_VIKRAM_TOKEN_HERE",     "vikram"),
    ]

    rotator = PATRotator(PATS)

    print("\n--- Rotation test (10 calls) ---")
    for i in range(10):
        token = rotator.get_token()
        alias = rotator._find_pat(token).alias
        print(f"  Call {i+1:2d} → using PAT: {alias}")

    print("\n--- Simulating PAT 'shreyas' getting rate-limited ---")
    shreyas_token = PATS[0][0]
    rotator.mark_rate_limited(shreyas_token, reset_at=time.time() + 3600)

    print("\n--- Rotation after blocking (should skip shreyas) ---")
    for i in range(6):
        token = rotator.get_token()
        alias = rotator._find_pat(token).alias
        print(f"  Call {i+1} → using PAT: {alias}")

    print("\n--- Status ---")
    for s in rotator.status():
        print(f"  {s}")
