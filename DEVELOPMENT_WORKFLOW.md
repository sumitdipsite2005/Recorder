# Recorder Development Workflow

This document defines the repository workflow for Recorder so development history remains useful, stable branches remain clean, and important release practices do not depend on conversation history.

## Branch roles

- `main` is the stable project baseline.
- `dev` is the integration branch for accepted development work.
- Substantial features and fixes should not be developed directly on `main` or `dev`.

## Feature and fix development

Start substantial work from the current `dev` branch using a temporary branch, for example:

```text
feature/<name>
fix/<name>
chore/<name>
```

Iterative development commits stay on that temporary branch.

Small corrections, experiments, review changes, and intermediate commits are expected there and do not need to be artificially minimized.

## Accepting completed work

When the feature or fix has been reviewed, tested, and accepted:

1. Open a pull request from the temporary branch into `dev`.
2. Squash-merge the pull request so `dev` receives one meaningful commit for the accepted unit of work.
3. Verify the integrated result on `dev`.
4. Delete the temporary branch when it is no longer needed.

The purpose is to preserve detailed iteration while work is in progress without carrying every experimental commit into the long-lived integration history.

## Promotion to main

`dev` should move to `main` only after the integrated work is considered stable and accepted.

Promotion should happen through a deliberate review step. `main` is not an active development branch.

## Repository enforcement

GitHub branch protections or repository rules should enforce the important parts of this workflow where practical:

- protect `main` from direct development changes;
- protect `dev` from substantial direct development changes;
- require changes to reach protected branches through pull requests;
- use squash merge for temporary feature/fix/chore branches into `dev`;
- keep required tests/checks passing before protected-branch integration when those checks are available.

Repository settings are part of the workflow and should be reviewed if branch structure or CI changes.

## Releases and versioning

A normal promotion to `main` does **not** automatically constitute a formal product release.

When Recorder reaches a milestone intended to be identified or published as a formal release, explicitly choose the release version and create the corresponding Git tag/release rather than publishing an unnamed release by accident.

Version numbers such as `v1.0.0`, `v1.1.0`, or `v2.0.0` should represent meaningful release milestones. Versioning does not need to be assigned during ordinary feature development.

Before creating a formal release, verify:

- the intended code is stable on `main`;
- tests and required validation have passed;
- documentation reflects the released behavior;
- the version number has been deliberately chosen;
- the corresponding Git tag/release is created from the correct `main` commit.

## Existing history

The repository's existing commit history predates this workflow and should not be rewritten merely to reduce commit count.

This workflow applies from the point at which it is adopted.
