# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v2.1.0 (2025-04-28)

### Feat

- **print**: display storage class and size of backups

### Refactor

- **cvar**: only use TZINFO cvar, not local
- **util**: break global contextvars into module
- **util**: delete some dead code
- **types**: migrate boto3-stubs -> types-boto3

## v2.0.0 (2025-03-12)

### BREAKING CHANGE

- if a source is newer than its snapshots, we unconditionally create a snapshot without
  user confirmation, even in an interactive terminal. new and old snapshots will be
  evaluated together with backups, and the user will be prompted for action. the user
  will have an option to undo the newly-created snapshots.
- drop support for python 3.8

### Feat

- **deps**: remove dependency on btrfsutil
- **update**: enable multiple values for remote, snapshot_dir, preserve and pipe_through
- **python**: support and test python 3.13

### Fix

- **upload**: wait for all processes in pipeline
- **typing**: breaking change from boto3-stubs

### Refactor

- **planner**: delete the old assessor logic
- **planner**: integrate the planner system
- **planner**: add an update planner
- **util**: pull describe_time_span into its own file
- **resolver**: make resolver generic
- **piper**: add a command filter context manager
- **ioctl**: add a btrfs ioctl interface

## v1.0.0 (2024-09-12)

### Added

- First official release
- Programmatic interface still to be stabilized
