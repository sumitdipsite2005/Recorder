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

Promotion happens through a pull request from `dev` to `main`. `main` is not an active development branch.

For normal promotion, use a regular merge so the accepted commits already present on `dev` remain traceable on `main`. Because GitHub creates a merge commit for that promotion, `dev` may then appear to be one or more commits behind `main` even though it is not missing the promoted product changes. That is expected and should not be "fixed" merely to make the branch counters match.

## Repository enforcement

The following GitHub rulesets are active:

### Protect main

- Targets only the default branch, `main`.
- Requires changes to reach `main` through a pull request.
- Requires 0 approving reviews, which keeps the workflow usable for a solo maintainer.
- Allows merge, squash, or rebase at the GitHub ruleset level.
- Prevents deletion of `main`.
- Prevents non-fast-forward history rewrites of `main`.

### Protect dev

- Targets only `dev`.
- Requires changes to reach `dev` through a pull request.
- Requires 0 approving reviews.
- Allows **squash merge only**.
- Prevents deletion of `dev`.
- Prevents non-fast-forward history rewrites of `dev`.

Temporary feature/fix/chore branches are intentionally left unrestricted so iterative work can proceed freely there.

When required CI checks are added or changed in the future, the protection rules should be reviewed so protected-branch integration continues to reflect the project's actual validation requirements.

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
