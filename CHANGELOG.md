# Changelog

## 0.4.0 - 2026-08-31

### Added

- Named broker queues and native float priorities with atomic highest-priority dequeue.
- Runner heartbeat metadata for consumed queues.

### Breaking

- The broker storage shape changed for named queues and priorities. Existing
  pre-0.4 broker documents are not migrated; clear or recreate broker data
  before upgrading.

### Changed

- Requires Pynenc 0.4.0 and Python 3.12 or newer.
