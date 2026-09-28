CHECKPOINT 0 - SAFETY BASELINE

Purpose
-------
These tests protect important behavior in the current mature record_dynamic.py
before its source-selection logic is reorganized in Checkpoint 1.

They do not use the internet and they do not start a recording.
They do not modify record_dynamic.py.

What is protected
-----------------
- event matching: AND/OR behavior and required/rejected/preferred qualifiers
- fixed TV-channel matching remains different from flexible event matching
- normal minimum-lifetime preference when joining a source
- fallback to the best shorter-lived source when no safer source exists
- preferred-qualifier influence
- mature video-quality ordering
- provider/profile handling of unknown expiry
- stricter lifetime/preference rules for optional quality upgrades
- 720p50 -> 1080p50 upgrade behavior
- mature failover playback/session fingerprint behavior
- mature recording-local manual source-rejection signature behavior

How to run on Windows
---------------------
1. Put the checkpoint0_tests folder directly beside record_dynamic.py.
2. Double-click run_checkpoint_0_tests.bat.
3. The final line should say: CHECKPOINT 0 TESTS: PASS

Nothing is recorded during this test.
