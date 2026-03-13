v0.4.0 (2026-03-13)
===================

Feat
----

- save Consul index on clean shutdown
- per-job event accumulation via python-nomad event stream
- replace polling with Nomad event stream, group events by job
- src layout, pyupgrade, Block Kit formatting (#2)

Fix
---

- debounce Terminated and treat Started as terminal
- suppress duplicate reports after terminal event
- handle urllib3-wrapped ReadTimeout in stream window
- handle stale/unreadable Consul index value gracefully
