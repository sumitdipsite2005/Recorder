# Recorder regression tests

The repository now keeps two layers of offline regression protection:

- `checkpoint0_tests/`: 26 mature-recorder safety-baseline tests.
- `tests/`: 592 Checkpoint 1/2 tests for shared source logic, provider-lane identity, Coordinator Inspect/Watch behavior, and architecture boundaries.

618 tests in total, 617 on a machine without the operator's local
`tests/test_privacy_guardrails.py` (that file is gitignored). Update these
counts when a suite's total changes.

No real playlist URLs are contacted and no recording is started.

Run all regression tests from the repository root:

```bat
run_all_tests.bat
```

Or run the suites directly:

```bat
python -m unittest -v checkpoint0_tests/test_recorder_safety_baseline.py
python -m unittest discover -s tests -v
```
