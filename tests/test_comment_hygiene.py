"""Automated guard against review-round and process crumbs in comments.

``CODING_STANDARDS.md`` §2.1(2) forbids review-round numbers, reviewer finding
codes, ledger item numbers and unanchored scenario labels in production code
comments, docstrings and test banners: whoever opens the file has only the file
in front of them, so a comment must state the operational rule, never the audit
round that requested it. This module turns that rule into a mechanical check
that runs on every local run and in CI, where no reviewer is present.

It is a floor, not the whole rule. Three deliberate limits:

* No-dash single-letter codes (``B1``, ``D1``, ``C2``) are not matched. They
  collide with legitimate domain shorthand in this codebase, where ``P1``/``P3``
  name the stream-integrity pillars. Those stay on the reviewer checklist in
  ``ENGINEERING_REVIEW_AND_AUDIT_STANDARDS.md`` §3.4 (Tier 1, item 4).
* A ticket tag is accepted when it rides on a real explanation, which is the
  form §2.1(3) permits as a secondary tag; only a bare tag is reported.
* Only comments and docstrings are read. String literals are not comments, so
  operator-facing text is never rewritten by a comment rule.

Pre-existing debt is grandfathered in ``comment_hygiene_baseline.json``. The
baseline is a debt ledger, not an allowlist: it can only shrink. A new
occurrence fails, and a baseline entry that no longer exists also fails, so the
ledger is updated in the same commit that pays the debt down.

Regenerate the baseline only for debt that already existed:

    python tests/test_comment_hygiene.py --update-baseline
"""

from __future__ import annotations

import ast
import io
import json
import re
import sys
import tokenize
import unittest
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASELINE_PATH = Path(__file__).resolve().parent / "comment_hygiene_baseline.json"

EXCLUDE_DIRS = {
    ".git",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".tox",
    "__pycache__",
    "build",
    "dist",
    ".venv",
    "venv",
    "node_modules",
}

# Operator-facing config examples are never imported as recorder code.
EXCLUDE_NAME_PREFIXES = (
    "recorder_static_user_config",
    "recorder_dynamic_user_config",
)

# Every unambiguous process-crumb marker named in CODING_STANDARDS.md §2.1(2).
CRUMB_PATTERNS = (
    ("review-round", re.compile(r"\bRound\s*#?\d+\b", re.IGNORECASE)),
    ("review-round", re.compile(r"\bR[12]\b")),
    ("finding-code", re.compile(r"\bTD-?\d{1,2}[a-z]?\b")),
    ("finding-code", re.compile(r"\b(?:B|P|D|C|N)-\d{1,2}[a-z]?\b")),
    ("review-severity", re.compile(r"\bBLOCKER\s*#?\d+\b")),
    ("ledger-item", re.compile(r"\bItem\s*#?\d+\b", re.IGNORECASE)),
    ("ledger-part", re.compile(r"\bPart\s+(?:I{1,3}|IV|V)\b")),
)

TICKET_PATTERN = re.compile(r"\bBUG-\d{2,4}\b")

# §2.1(3): a ticket ID may close a comment only as a secondary tag on a real
# explanation. Fewer words than this, once the markers are removed, is bare.
MIN_EXPLANATION_WORDS = 4

# §3.1 also bans review artefacts inside identifiers (``td1_guard``,
# ``scenario_7_fallback``), where the meaning leaks into the call sites.
ARTEFACT_NAME = re.compile(
    r"(?:^|_)(?:round_?\d+|r[12]|td_?\d+|blocker_?\d+|item_?\d+|scenario_?\d+|bug_?\d+)",
    re.IGNORECASE,
)


def _python_files() -> list[Path]:
    files: list[Path] = []
    for path in ROOT.rglob("*.py"):
        if any(part in EXCLUDE_DIRS for part in path.parts):
            continue
        if path.name.startswith(EXCLUDE_NAME_PREFIXES):
            continue
        if path.resolve() == Path(__file__).resolve():
            continue
        files.append(path)
    return sorted(files)


def _comment_and_docstring_texts(path: Path) -> list[tuple[str, str, int]]:
    """Every comment and docstring in one file as ``(kind, text, line)``."""
    source = path.read_text(encoding="utf-8", errors="replace")
    texts: list[tuple[str, str, int]] = []
    try:
        stream = io.StringIO(source)
        for token in tokenize.generate_tokens(stream.readline):
            if token.type == tokenize.COMMENT:
                texts.append(("comment", token.string, token.start[0]))
    except (tokenize.TokenError, IndentationError):
        # A file that cannot be tokenised has a syntax error of its own; this
        # guard does not report the same broken file twice.
        pass
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return texts
    for node in ast.walk(tree):
        if isinstance(
            node,
            (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef),
        ):
            docstring = ast.get_docstring(node, clean=False)
            if docstring:
                texts.append(("docstring", docstring, node.body[0].lineno))
    return texts


def _normalise(text: str) -> str:
    return " ".join(text.split())


def _strip_markers(text: str) -> str:
    stripped = TICKET_PATTERN.sub(" ", text)
    for _, pattern in CRUMB_PATTERNS:
        stripped = pattern.sub(" ", stripped)
    return stripped


def _is_bare_ticket(text: str) -> bool:
    words = re.findall(r"[A-Za-z]{2,}", _strip_markers(text))
    return len(words) < MIN_EXPLANATION_WORDS


def _scan() -> dict[tuple[str, str, str], list[str]]:
    """Map ``(file, kind, normalised text)`` to its ``line: rules`` occurrences."""
    found: dict[tuple[str, str, str], list[str]] = {}
    for path in _python_files():
        rel_path = path.relative_to(ROOT).as_posix()
        for kind, text, line in _comment_and_docstring_texts(path):
            rules = sorted(
                {name for name, pattern in CRUMB_PATTERNS if pattern.search(text)}
            )
            if TICKET_PATTERN.search(text) and _is_bare_ticket(text):
                rules.append("bare-ticket")
            if not rules:
                continue
            key = (rel_path, kind, _normalise(text))
            found.setdefault(key, []).append(f"line {line}: [{', '.join(rules)}]")
    return found


def _baseline() -> Counter:
    if not BASELINE_PATH.exists():
        raise FileNotFoundError(
            f"{BASELINE_PATH} is missing. Regenerate it from the pre-existing debt with "
            "`python tests/test_comment_hygiene.py --update-baseline`."
        )
    payload = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    return Counter(
        {
            (entry["file"], entry["kind"], entry["text"]): int(entry["count"])
            for entry in payload["entries"]
        }
    )


def _write_baseline(counts: Counter) -> None:
    entries = [
        {"file": file, "kind": kind, "text": text, "count": count}
        for (file, kind, text), count in sorted(counts.items())
    ]
    payload = {
        "_comment": (
            "Pre-existing process-crumb debt, measured when the guard was introduced. "
            "This file is a debt ledger, not an allowlist: entries may only be deleted, "
            "and only in the commit that rewrites the comment. Ledger owner: "
            "Recorder-Docs/RECORDER NEXT STEPS/"
            "RECORDER_FUTURE_WORKSTREAM_1_ENGINEERING_FOUNDATION_AND_REFACTORING.md §2.6."
        ),
        "entries": entries,
    }
    BASELINE_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {len(entries)} baseline entries to {BASELINE_PATH.name}")


class CommentHygieneTests(unittest.TestCase):
    """Enforces ``CODING_STANDARDS.md`` §2 against new review crumbs."""

    def test_no_new_process_crumbs_in_comments_or_docstrings(self):
        actual = _scan()
        baseline = _baseline()
        new = []
        for key, occurrences in sorted(actual.items()):
            allowed = baseline.get(key, 0)
            if len(occurrences) <= allowed:
                continue
            rel_path, kind, text = key
            new.append(
                f"{rel_path} ({kind}) {len(occurrences)}x {occurrences}: {text[:200]}"
            )
        self.assertEqual(
            new,
            [],
            "New review crumbs in comments or docstrings (CODING_STANDARDS.md §2.1(2)).\n"
            "Rewrite each one into the operational rule it stands for. Do not add it to\n"
            "comment_hygiene_baseline.json, which records only debt that already existed:\n  "
            + "\n  ".join(new),
        )

    def test_hygiene_baseline_has_no_stale_entries(self):
        actual = _scan()
        stale = [
            f"{rel_path} ({kind}): {text[:200]}"
            for (rel_path, kind, text), count in sorted(_baseline().items())
            if len(actual.get((rel_path, kind, text), [])) < count
        ]
        self.assertEqual(
            stale,
            [],
            "The hygiene baseline lists debt that is no longer there. Delete these entries\n"
            "in the same commit that rewrote or removed the comment, so the ledger keeps\n"
            "shrinking (comment_hygiene_baseline.json):\n  " + "\n  ".join(stale),
        )

    def test_no_process_artefacts_in_function_or_class_names(self):
        offenders = []
        for path in _python_files():
            try:
                tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
            except SyntaxError:
                continue
            rel_path = path.relative_to(ROOT).as_posix()
            for node in ast.walk(tree):
                if isinstance(
                    node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
                ) and ARTEFACT_NAME.search(node.name):
                    offenders.append(f"{rel_path}:{node.lineno} {node.name}")
        self.assertEqual(
            offenders,
            [],
            "Function or class names carrying review artefacts (CODING_STANDARDS.md §3.1).\n"
            "Name them after the behaviour they verify:\n  " + "\n  ".join(offenders),
        )


if __name__ == "__main__":
    if "--update-baseline" in sys.argv:
        _write_baseline(Counter({key: len(value) for key, value in _scan().items()}))
    else:
        unittest.main()
