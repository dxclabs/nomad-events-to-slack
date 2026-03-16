CHANGELOG
###############

v0.5.0 (2026-03-16)
===================

Feat
----

- key TERMINAL_EVENT_TYPES by job type
- show deployment health on Started events in Slack
- treat deployment_healthy=False as a terminal state
- capture job_type, deployment health and task states per job
- replace polling with Nomad event stream, group events by job (#3)

Fix
---

- don't treat Started as terminal while health check is pending

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

0.3.0 - 2026-03-12
==================

chore: move app.py => src/app.py
chore: fix linting, typing and other ruff errors in app.py
refactor: change slack post to Block Kit messages
ci: tighten ruff tests in workflow
chore: ruff reformat app.py
chore: adjust .gitignore and .gitattributes
