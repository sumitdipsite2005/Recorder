# Recorder

Live-stream recording system with playlist discovery, source probing, identity-based coordination, automatic worker launch, recovery, and terminal monitoring.

The current Recorder workflow is designed to support unattended event recording: the Coordinator watches configured playlist sources, discovers canonical stream identities, and can automatically launch one recording worker per eligible identity.

## Main components

- `record_dynamic.py` — mature dynamic recorder. Handles source selection, recording, renewal, recovery, quality upgrades, alarms, validation, logging, and finalization.
- `recorder_event_coordinator.py` — terminal Coordinator for discovering identities and launching/managing identity workers.
- `recorder_identity_worker.py` — worker entry point for one identity-bound recording session.
- `recorder_coordinator/` — Coordinator configuration, acquisition, snapshot, launch, worker, and terminal logic.
- `recorder_runtime/` — runtime paths, registry ownership, identity status, sound state, and launch contracts.
- `recorder_source/` — shared playlist parsing, matching, identity, probing, quality, transport, and source-selection logic.
- `tests/` and `checkpoint0_tests/` — regression and safety coverage.

## Coordinator modes

Targets use one of two identity policies:

- **MANUAL** — qualifying identities appear in the dashboard and are launched by the user.
- **ALL_IDENTITIES** — qualifying identities are launched automatically.

Both policies share the same identity registry. A canonical identity can only be owned by one active worker in a Coordinator session.

The active identity-worker ceiling is **20 total workers** across MANUAL and ALL_IDENTITIES.

## User configuration

Runtime configuration is kept outside the repository in:

`recorder_dynamic_user_config.py`

On the supported Windows/macOS setup, Recorder resolves the normal OneDrive Recorder configuration location automatically. A different Coordinator config can be supplied explicitly:

```bash
python recorder_event_coordinator.py --config "/path/to/recorder_dynamic_user_config.py"
```

To start the Coordinator using the normal configuration location:

```bash
python recorder_event_coordinator.py
```

The standalone mature dynamic recorder can be started with:

```bash
python record_dynamic.py
```

## Coordinator dashboard

The Coordinator dashboard shows:

- active recordings
- ALL_IDENTITIES targets
- MANUAL targets
- canonical identities
- quality groups and source rows
- current probe/working state
- source and row update information
- registry/worker state
- transient change notifications

Useful controls are shown in the terminal footer, including manual recording selection, information, sound control, refresh, and exit.

## Sounds

Optional local Coordinator sounds live in:

```text
sounds/refresh.wav
sounds/launch.wav
```

WAV files are intentionally ignored by Git and remain local to each Recorder machine.

- `refresh.wav` — meaningful dashboard update notification
- `launch.wav` — successful ALL_IDENTITIES worker-launch notification

Sound notifications respect the Coordinator sound-snooze controls.

## Tests

GitHub Actions runs on pushes to both `main` and `dev`, and on pull requests.

The CI suite currently runs:

```bash
python -m unittest -v checkpoint0_tests/test_recorder_safety_baseline.py
python -m unittest discover -s tests -v
```

On Windows, the repository also includes:

```text
run_all_tests.bat
```

## Branches

- **main** — stable Recorder baseline
- **dev** — ongoing development and testing

Normal development should happen on `dev` and be promoted to `main` through a pull request after the regression suite is green.

## Current project state

The identity-based Coordinator and unattended Auto workflow are implemented and in active use. The completed baseline includes identity discovery, MANUAL and ALL_IDENTITIES policies, duplicate prevention, worker ownership, automatic launch, recovery integration, dashboard monitoring, sound controls, and the shared 20-worker ceiling.

Further Recorder improvements can continue on `dev` without changing the stable `main` baseline until they are ready to be promoted.
