"""Regression checks for BUG-012 retained-backup renewal scheduling.

Covers the five hunks approved for Stage 3B in
``Recorder-Docs/SPECIFICATIONS/2026-10-25_BUG-012_RETAINED_BACKUP_POLLING_3A_SPEC_OUT_R2.md``:
the renewal card tells the truth about the next check, a discarded backup
re-arms the clock at once, and a backup that expires while a check is in
progress is dropped the same way through both doors.

Also covers the Round 1 audit remediations approved for Stage 5B and carried
in ``..._3B_SPEC_OUT_R3.md``: the backup and the candidate pool are judged at a
clock read taken *after* the quality probes (TD-1, TD-1b), the successful door
reports a lost backup at ``WARN`` like the failed door (P-1), and the
discard-then-scan ordering inside one pass is proven rather than inferred
(P-3, audit §6.4).

The recorder reads machine-specific configuration at import time, so each hunk
is compiled from ``record_dynamic.py``'s AST and executed against stub globals.
No recorder, network, or thread is started.
"""

from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

RECORDER = Path(__file__).resolve().parents[1] / "record_dynamic.py"
TARGET = 1_000_000.0


def _parse():
    return ast.parse(RECORDER.read_text(encoding="utf-8"))


def _monitor(tree):
    return next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "monitor_nm3u8dl_playlist_renewal"
    )


def _ifs(node):
    return [child for child in ast.walk(node) if isinstance(child, ast.If)]


def _test_id(node):
    test = getattr(node, "test", None)
    return test.id if isinstance(test, ast.Name) else None


def _assigns(node, target):
    for child in ast.walk(node):
        if isinstance(child, ast.Assign):
            for item in child.targets:
                if isinstance(item, ast.Name) and item.id == target:
                    return True
    return False


def _logs(node, needle):
    for child in ast.walk(node):
        if (
            isinstance(child, ast.Call)
            and isinstance(child.func, ast.Name)
            and child.func.id == "log"
        ):
            for arg in child.args:
                if (
                    isinstance(arg, ast.Constant)
                    and isinstance(arg.value, str)
                    and needle in arg.value
                ):
                    return True
    return False


def _compile(statements):
    module = ast.Module(body=list(statements), type_ignores=[])
    return compile(
        ast.fix_missing_locations(module),
        str(RECORDER),
        "exec",
    )


def _only(nodes, needle):
    """Return the one ``if`` whose ``log`` text carries ``needle``."""
    found = [node for node in nodes if _logs(node, needle)]
    if len(found) != 1:
        raise AssertionError(
            f"expected exactly one block logging {needle!r}, found {len(found)}"
        )
    return found[0]


def _by_condition(nodes, name):
    return [node for node in nodes if _test_id(node) == name]


def _calls(node, name):
    for child in ast.walk(node):
        if (
            isinstance(child, ast.Call)
            and isinstance(child.func, ast.Name)
            and child.func.id == name
        ):
            return True
    return False


def _statements_around(tree, first_target, last_condition):
    """Slice one statement body from an assignment through a trailing ``if``."""
    stack = [tree]
    while stack:
        node = stack.pop()
        for _, value in ast.iter_fields(node):
            if not isinstance(value, list):
                continue
            if not all(isinstance(item, ast.stmt) for item in value):
                continue
            targets = [
                item.targets[0].id
                for item in value
                if isinstance(item, ast.Assign)
                and isinstance(item.targets[0], ast.Name)
            ]
            if first_target not in targets:
                continue
            last_index = next(
                (
                    index
                    for index, item in enumerate(value)
                    if _test_id(item) == last_condition
                ),
                None,
            )
            if last_index is not None:
                first_index = next(
                    index
                    for index, item in enumerate(value)
                    if isinstance(item, ast.Assign)
                    and isinstance(item.targets[0], ast.Name)
                    and item.targets[0].id == first_target
                )
                return value[first_index:last_index + 1]
        stack.extend(ast.iter_child_nodes(node))
    raise AssertionError(
        f"no body contains {first_target!r} followed by {last_condition!r}"
    )


class RenewalScheduleTests(unittest.TestCase):
    """One case per approved hunk; each proves the scenario in the contract."""

    @classmethod
    def setUpClass(cls):
        cls.tree = _parse()
        cls.monitor = _monitor(cls.tree)
        cls.text = RECORDER.read_text(encoding="utf-8")

        # §3.2 — the loop-top discard block ("expired before rollover").
        cls.discard_code = _compile(
            [_only(_ifs(cls.monitor), "expired before")]
        )

        # §3.3 — the failed-check door. Only this door assigns held_source.
        doors = [
            node
            for node in _by_condition(
                _ifs(cls.monitor), "authorization_check_due"
            )
            if _calls(node, "log_nm3u8dl_playlist_renewal_check")
        ]
        failed = [node for node in doors if _assigns(node, "held_source")]
        healthy = [node for node in doors if not _assigns(node, "held_source")]
        if len(failed) != 1 or len(healthy) != 1:
            raise AssertionError(
                f"expected one card-printing block per door, "
                f"found failed={len(failed)} healthy={len(healthy)}"
            )
        cls.failed_check_code = _compile(failed)
        cls.rearm_code = _compile(healthy)

        # §3.4 — the post-scan cleanup plus its decision chain. The slice
        # starts at the post-probe clock read so the TD-1 / TD-1b anchor is
        # inside the compiled fragment instead of being stubbed from outside.
        cls.post_scan_code = _compile(
            _statements_around(
                cls.monitor, "retained_now_ts", "stable_candidates"
            )
        )

        # The mature cadence helper and the card printer, unchanged.
        functions = {
            node.name: node
            for node in ast.walk(cls.monitor)
            if isinstance(node, ast.FunctionDef)
        }
        cls.cadence_code = _compile(
            [functions["calculate_next_renewal_check"]]
        )
        cls.card_function = next(
            node
            for node in cls.tree.body
            if isinstance(node, ast.FunctionDef)
            and node.name == "log_nm3u8dl_playlist_renewal_check"
        )

    # -- Scenario 1 ---------------------------------------------------------
    def _run_rearm(self, locked):
        card = Mock()
        env = {
            "authorization_check_due": True,
            "replacement_locked": locked,
            "check_completed_ts": TARGET - 180,
            "risk_check_start_ts": TARGET - 300,
            "risk_interval_sec": 60,
            "normal_interval_sec": 300,
            "running_target": TARGET,
            "authorization_renewal_enabled": True,
            "source_results": [],
            "running_expiry": TARGET + 21600,
            "candidate": None,
            "decision": "stub decision",
            "decision_level": "WARN",
            "next_renewal_check_ts": None,
            "state": SimpleNamespace(nm3u8dl_pending_source=None),
            "log_nm3u8dl_playlist_renewal_check": card,
        }
        exec(self.cadence_code, env)
        exec(self.rearm_code, env)
        return env, card

    def test_locked_backup_rearms_clock(self):
        for locked in (True, False):
            with self.subTest(replacement_locked=locked):
                env, card = self._run_rearm(locked)
                next_ts = env["next_renewal_check_ts"]
                self.assertIsNotNone(next_ts)
                self.assertEqual(next_ts, env["check_completed_ts"] + 60)
                # The card receives the same clock the state carries, so a
                # held backup can never be reported as "not scheduled".
                self.assertEqual(
                    card.call_args.kwargs["next_check_ts"], next_ts
                )
                self.assertEqual(
                    card.call_args.kwargs["replacement_locked"], locked
                )

    # -- Scenario 2 ---------------------------------------------------------
    def _cadence(self, **overrides):
        env = {
            "authorization_renewal_enabled": True,
            "normal_interval_sec": 300,
            "risk_interval_sec": 60,
            "risk_check_start_ts": TARGET - 300,
            "running_target": TARGET,
        }
        env.update(overrides)
        exec(self.cadence_code, env)
        return env["calculate_next_renewal_check"]

    def test_schedule_cadence_from_seed_to_ceiling(self):
        calculate = self._cadence()
        rows = (
            (TARGET - 1800, TARGET - 1800 + 300),
            (TARGET - 301, TARGET - 300),
            (TARGET - 250, TARGET - 250 + 60),
            (TARGET - 1, TARGET),
            (TARGET + 120, TARGET + 120 + 60),
        )
        for probe, expected in rows:
            with self.subTest(probe=probe):
                next_ts = calculate(probe)
                self.assertAlmostEqual(next_ts, expected, places=6)
                # The step is strictly positive for every probe; it has no
                # minimum, so nothing here asserts a floor.
                self.assertGreater(next_ts, probe)

        configurations = {
            "normal": dict(
                risk_check_start_ts=TARGET - 300, running_target=TARGET
            ),
            "risk window inside the last minute": dict(
                risk_check_start_ts=TARGET - 60, running_target=TARGET
            ),
            "window opens after rollover": dict(
                risk_check_start_ts=TARGET - 60, running_target=TARGET - 120
            ),
            "window equals rollover": dict(
                risk_check_start_ts=TARGET, running_target=TARGET
            ),
        }
        sweep = [
            TARGET - 10800 + 30 * step
            for step in range(int((10800 + 600) / 30) + 1)
        ]
        for label, overrides in configurations.items():
            calculate = self._cadence(**overrides)
            for probe in sweep:
                next_ts = calculate(probe)
                self.assertIsNotNone(next_ts, msg=f"{label} {probe}")
                self.assertGreater(next_ts, probe, msg=f"{label} {probe}")

        # The no-floor probes prove a "minimum interval" claim would be wrong.
        no_floor = (TARGET - 300 - 0.001, TARGET - 300 - 0.1, TARGET - 300 - 0.5)
        calculate = self._cadence()
        for probe in no_floor:
            with self.subTest(no_floor_probe=probe):
                next_ts = calculate(probe)
                self.assertGreater(next_ts, probe)
                self.assertAlmostEqual(next_ts, TARGET - 300, places=6)

        disabled = self._cadence(authorization_renewal_enabled=False)
        for probe in sweep:
            self.assertIsNone(disabled(probe), msg=f"disabled {probe}")

    # -- Scenario 3 ---------------------------------------------------------
    def _run_discard(self, now, window_start, enabled=True):
        held = {
            "stream_url": "http://backup",
            "playlist_url": "http://backup/list.m3u8",
            "expiry": now - 1,
        }
        logs = []

        def record(message, level="INFO"):
            logs.append((message, level))

        env = {
            "state": SimpleNamespace(nm3u8dl_pending_source=dict(held)),
            "pending_source": dict(held),
            "pending_expiry": now - 1,
            "now": now,
            "normal_check_start_ts": window_start,
            "authorization_renewal_enabled": enabled,
            "next_renewal_check_ts": now + 3600,
            "log": record,
        }
        exec(self.discard_code, env)
        return env, logs

    def test_expired_backup_rearm_clamped_to_lead_window(self):
        now = TARGET

        # Inside the lead window the very next pass must scan, replacing the
        # far-future tick that used to survive a locked backup.
        env, logs = self._run_discard(now, window_start=now - 600)
        self.assertIsNone(env["state"].nm3u8dl_pending_source)
        self.assertEqual(env["next_renewal_check_ts"], now)
        self.assertTrue(
            any("discarding it" in text and level == "WARN" for text, level in logs)
        )

        # Three hours before the window the clock waits for the opening
        # rather than polling every five minutes all evening.
        env, logs = self._run_discard(now, window_start=now + 10800)
        self.assertIsNone(env["state"].nm3u8dl_pending_source)
        self.assertEqual(env["next_renewal_check_ts"], now + 10800)
        self.assertTrue(
            any("discarding it" in text and level == "WARN" for text, level in logs)
        )

        # Polling disabled: the backup still goes, the clock is not ours.
        env, _ = self._run_discard(now, window_start=now - 600, enabled=False)
        self.assertIsNone(env["state"].nm3u8dl_pending_source)
        self.assertEqual(env["next_renewal_check_ts"], now + 3600)

    # -- Scenario 4 ---------------------------------------------------------
    def test_card_has_no_lock_related_next_check_state(self):
        # The removed state cannot come back as a card string.
        self.assertNotIn("retained replacement locked", self.text)

        def is_none_check(node):
            return (
                isinstance(node.test, ast.Compare)
                and isinstance(node.test.left, ast.Name)
                and node.test.left.id == "next_check_ts"
                and len(node.test.ops) == 1
                and isinstance(node.test.ops[0], ast.Is)
                and len(node.test.comparators) == 1
                and isinstance(node.test.comparators[0], ast.Constant)
                and node.test.comparators[0].value is None
            )

        branches = [
            node
            for node in _ifs(self.card_function)
            if is_none_check(node)
        ]
        self.assertEqual(len(branches), 1)
        branch = branches[0]
        # Exactly two branches: no third, lock-conditioned wording.
        self.assertTrue(branch.orelse)
        self.assertFalse(
            len(branch.orelse) == 1 and isinstance(branch.orelse[0], ast.If)
        )
        self.assertFalse(_calls(branch, "replacement_locked"))
        for child in ast.walk(branch):
            if isinstance(child, ast.Name) and child.id == "replacement_locked":
                self.fail("the next-check text consults replacement_locked")

    # -- Scenario 5 ---------------------------------------------------------
    def _run_failed_check(self, held, usable, stable):
        card = Mock()
        logs = []
        check_completed_ts = TARGET - 100

        def record(message, level="INFO"):
            logs.append((message, level))

        env = {
            "authorization_check_due": True,
            "state": SimpleNamespace(
                nm3u8dl_pending_source=None if held is None else dict(held)
            ),
            "running_expiry": check_completed_ts + 21600,
            "source_results": [],
            "check_completed_ts": check_completed_ts,
            "authorization_renewal_enabled": True,
            "normal_interval_sec": 300,
            "risk_interval_sec": 60,
            "risk_check_start_ts": TARGET - 300,
            "running_target": TARGET,
            "is_nm3u8dl_usable_replacement": (
                lambda running, url, expiry, now_ts=None: usable
            ),
            "is_nm3u8dl_stable_replacement": (
                lambda running, expiry: stable
            ),
            "log": record,
            "log_nm3u8dl_playlist_renewal_check": card,
        }
        exec(self.cadence_code, env)
        expected_clock = env["calculate_next_renewal_check"](check_completed_ts)
        exec(self.failed_check_code, env)
        card_kwargs = card.call_args.kwargs
        self.assertEqual(card_kwargs["next_check_ts"], expected_clock)
        self.assertIsNotNone(card_kwargs["next_check_ts"])
        self.assertEqual(card_kwargs["decision_level"], "WARN")
        return env, card_kwargs, logs

    def test_failed_check_preserves_held_status(self):
        # (a) usable and stable: a locked backup, honestly reported.
        held = {
            "stream_url": "http://backup",
            "playlist_url": "http://backup/list.m3u8",
            "expiry": TARGET + 64800,
        }
        env, kwargs, _ = self._run_failed_check(held, usable=True, stable=True)
        self.assertIs(kwargs["replacement_locked"], True)
        self.assertIsNotNone(kwargs["retained_source"])
        self.assertIn("playlist check failed", kwargs["decision"])
        self.assertIn("remains safe and locked", kwargs["decision"])
        self.assertNotIn("no usable authorization found", kwargs["decision"])

        # (b) usable but short-lived: a stopgap, reported as a fallback.
        env, kwargs, _ = self._run_failed_check(held, usable=True, stable=False)
        self.assertIs(kwargs["replacement_locked"], False)
        self.assertIsNotNone(kwargs["retained_source"])
        self.assertIn("fallback retained", kwargs["decision"])

        # (c) it died during the check: dropped here, and said out loud.
        expired = dict(held)
        env, kwargs, logs = self._run_failed_check(
            expired, usable=False, stable=True
        )
        self.assertIsNone(env["state"].nm3u8dl_pending_source)
        self.assertIsNone(kwargs["retained_source"])
        self.assertIs(kwargs["replacement_locked"], False)
        self.assertIn("expired during this check; discarded", kwargs["decision"])
        self.assertTrue(
            any(
                "discarding it" in text and level == "WARN"
                for text, level in logs
            )
        )

        # No held backup at all: the original wording, untouched.
        _, kwargs, _ = self._run_failed_check(None, usable=False, stable=False)
        self.assertEqual(
            kwargs["decision"],
            "no usable authorization found; current recording continues",
        )

    # -- Scenario 6 ---------------------------------------------------------
    def _run_post_scan(
        self, held, replacement_candidates, scan_time=None, probe_clock=None
    ):
        """Run the §3.4 block with the scan clock and the probe clock apart.

        ``scan_time`` is the ``check_time`` sample taken before the quality
        probes; ``probe_clock`` is what ``time.time()`` returns when the block
        runs after them. They differ only in TD-1's window.
        """
        logs = []
        check_time = TARGET if scan_time is None else scan_time
        clock = TARGET if probe_clock is None else probe_clock

        def record(message, level="INFO"):
            logs.append((message, level))

        env = {
            "time": SimpleNamespace(time=lambda: clock),
            "state": SimpleNamespace(nm3u8dl_pending_source=dict(held)),
            "candidate_pool": replacement_candidates,
            "resolution": {"stream_url": "http://scan", "expiry": None},
            "running_expiry": check_time + 21600,
            "check_time": check_time,
            "running_source": {"stream_url": "http://running"},
            "is_nm3u8dl_usable_replacement": (
                lambda running, url, expiry, now_ts=None: (
                    expiry is not None and expiry > now_ts
                )
            ),
            "is_nm3u8dl_stable_replacement": (
                lambda running, expiry: False
            ),
            "get_nm3u8dl_candidate_quality_rank": (
                lambda item: item.get("rank", 0)
            ),
            "NM3U8DL_NEW_SOURCE_MIN_REMAINING_MIN": 30,
            "log": record,
            "decision": "unset",
            "decision_level": "INFO",
        }
        exec(self.post_scan_code, env)
        return env, logs

    def test_successful_scan_discards_backup_expired_mid_check(self):
        # The scan found nothing better, and the backup died during it.
        expired = {
            "stream_url": "http://backup",
            "playlist_url": "http://backup/list.m3u8",
            "expiry": TARGET - 1,
        }
        env, logs = self._run_post_scan(expired, replacement_candidates=[])
        decision = env["decision"]
        self.assertIsNone(env["state"].nm3u8dl_pending_source)
        self.assertIn("expired during this check; discarded", decision)
        # The two false lines the fix removes, and the mislabel the review's
        # own cleanup would have produced.
        self.assertNotIn("fallback retained", decision)
        self.assertNotIn("does not extend the running authorization", decision)
        self.assertIs(env["replacement_locked"], False)
        # P-1: the successful door reports the loss at WARN, exactly as the
        # failed door does for the same event.
        self.assertEqual(env["decision_level"], "WARN")
        self.assertTrue(
            any(
                "discarding it" in text and level == "WARN"
                for text, level in logs
            )
        )

        # A fresh candidate is still selected; cleanup must not disturb it.
        fresh = {
            "stream_url": "http://fresh",
            "playlist_url": "http://fresh/list.m3u8",
            "expiry": TARGET + 7200,
            "rank": 2,
        }
        env, _ = self._run_post_scan(
            expired, replacement_candidates=[fresh]
        )
        selected = env["state"].nm3u8dl_pending_source
        # The scan copies the chosen candidate and attaches its metadata, so
        # compare the identifying fields rather than the whole dictionary.
        self.assertEqual(selected["stream_url"], fresh["stream_url"])
        self.assertEqual(selected["expiry"], fresh["expiry"])
        self.assertIn("candidate_count", selected)
        self.assertIn("fallback replacement retained", env["decision"])


    # -- Scenario 7 (Round 1 audit TD-1 / TD-1b / P-1) -----------------------
    def test_backup_expiring_during_quality_probes_is_discarded(self):
        """The backup is judged after the probes, not before them."""
        scan_time = TARGET - 120
        probe_clock = TARGET
        held = {
            "stream_url": "http://backup",
            "playlist_url": "http://backup/list.m3u8",
            "expiry": TARGET - 60,
        }
        # The window this case proves: alive at the scan's own clock, dead by
        # the clock read after the IDET quality probes.
        self.assertGreater(held["expiry"], scan_time)
        self.assertLessEqual(held["expiry"], probe_clock)

        env, logs = self._run_post_scan(
            held, [], scan_time=scan_time, probe_clock=probe_clock
        )
        decision = env["decision"]
        self.assertIsNone(env["state"].nm3u8dl_pending_source)
        self.assertIn("expired during this check; discarded", decision)
        self.assertNotIn("fallback retained", decision)
        self.assertIs(env["replacement_locked"], False)
        self.assertEqual(env["decision_level"], "WARN")
        self.assertEqual(
            [text for text, level in logs if level == "WARN"],
            [
                "Retained authorization replacement expired during "
                "this check → discarding it..."
            ],
        )

        # TD-1b: a pool candidate that died during the probes is not handed
        # over as the new retained source either. With the stale clock it
        # would pass the filter and be re-installed as a live fallback.
        dead_candidate = {
            "stream_url": "http://dead-candidate",
            "playlist_url": "http://dead-candidate/list.m3u8",
            "expiry": TARGET - 30,
            "rank": 2,
        }
        self.assertGreater(dead_candidate["expiry"], scan_time)
        self.assertLessEqual(dead_candidate["expiry"], probe_clock)
        env, _ = self._run_post_scan(
            held,
            [dead_candidate],
            scan_time=scan_time,
            probe_clock=probe_clock,
        )
        self.assertIsNone(env["state"].nm3u8dl_pending_source)
        self.assertIn("expired during this check; discarded", env["decision"])
        self.assertEqual(env["decision_level"], "WARN")


    # -- Scenario 8 (Round 1 audit P-3, §6.4) --------------------------------
    def test_discard_then_scan_in_one_pass_arms_fresh_cadence(self):
        """Hunk 2's clamp and hunk 3's re-arm run in the same pass."""
        now = TARGET - 1

        # Inside the lead window the loop-top discard clamps the clock to
        # "now", so the pass that follows scans immediately.
        env, _ = self._run_discard(now, window_start=now - 600)
        self.assertIsNone(env["state"].nm3u8dl_pending_source)
        self.assertEqual(env["next_renewal_check_ts"], now)

        # The scan in that same pass completes 0.2 s later and re-arms from
        # its own completion time — the clamp must not survive the pass.
        card = Mock()
        env.update(
            authorization_check_due=True,
            normal_interval_sec=300,
            risk_interval_sec=60,
            risk_check_start_ts=TARGET - 300,
            running_target=TARGET,
            running_expiry=TARGET + 21600,
            source_results=[],
            candidate=None,
            decision="scan completed",
            decision_level="INFO",
            replacement_locked=False,
            log_nm3u8dl_playlist_renewal_check=card,
        )
        check_completed_ts = now + 0.2
        env["check_completed_ts"] = check_completed_ts
        exec(self.cadence_code, env)
        exec(self.rearm_code, env)
        self.assertGreaterEqual(check_completed_ts, env["risk_check_start_ts"])
        # Inside the risk window the step is 60 s, capped at the rollover
        # instant: min(check_completed + 60, running_target).
        expected_next = min(check_completed_ts + 60, TARGET)
        self.assertEqual(env["next_renewal_check_ts"], expected_next)
        self.assertNotEqual(env["next_renewal_check_ts"], now)
        self.assertGreater(env["next_renewal_check_ts"], now)
        self.assertEqual(
            card.call_args.kwargs["next_check_ts"],
            env["next_renewal_check_ts"],
        )

        # Outside the lead window the clamp schedules the opening in the
        # future, so the pass that follows does NOT scan: the same-pass
        # supersede only happens inside the window.
        early = TARGET - 11100
        env, _ = self._run_discard(early, window_start=TARGET - 300)
        self.assertEqual(env["next_renewal_check_ts"], TARGET - 300)
        self.assertLess(early, env["next_renewal_check_ts"])

        # Both doors lose a backup at the same severity and with the same
        # sentence stem, so neither can be quietly downgraded later (P-1,
        # stated as an equality between the two doors).
        expired_held = {
            "stream_url": "http://backup",
            "playlist_url": "http://backup/list.m3u8",
            "expiry": TARGET - 1,
        }
        _, failed_kwargs, _ = self._run_failed_check(
            expired_held, usable=False, stable=True
        )
        scan_env, _ = self._run_post_scan(expired_held, [])
        self.assertEqual(scan_env["decision_level"], "WARN")
        self.assertEqual(
            failed_kwargs["decision_level"], scan_env["decision_level"]
        )
        stem = "retained replacement expired during this check; discarded"
        self.assertIn(stem, failed_kwargs["decision"])
        self.assertIn(stem, scan_env["decision"])


if __name__ == "__main__":
    unittest.main()
