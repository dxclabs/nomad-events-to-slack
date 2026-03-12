Here's a summary of everything we discussed:

  Fixes

  1. Slack webhook error handling (post_message_to_slack)
  The function raises a generic ConnectionError on any non-"ok" response, including Slack 429 rate limit responses,
  which kills the process. Needs to:
  - Inspect the HTTP status code
  - Handle 429 specifically — read the Retry-After header and back off
  - Distinguish transient errors (retry) from real config errors (fail loudly)

  2. Consul index storage
  Currently stores base64-encoded JSON of the full event, which grows unboundedly. Should just store a single integer
  (the Nomad event index) as a plain string, alongside a timestamp — e.g. {"index": 12345, "updated":
  "2026-03-12T10:00:00"}. The timestamp enables the t-3 days safety net (see enhancements).

  ---
  Enhancements

  3. Rate limiting on burst
  On startup it fires all queued events as fast as possible, overwhelming Slack's ~1 message/sec limit. Add a token
  bucket or simple delay between messages when processing a backlog.

  4. Event filtering
  - Drop noisy intermediate events: Received, Task Setup, Driver (downloading image)
  - For service jobs: show Started and bad terminations
  - For batch/periodic jobs: show failures only — suppress clean completions entirely
  - Surface OOM kills clearly (Nomad sets OOMKilled: true in event details on a Terminated event) — flag these
  prominently in red

  5. Replace polling with Nomad event stream
  Currently polls all allocations every 30 seconds. The /v1/event/stream API is a long-polling HTTP endpoint that pushes
   events as they happen. Benefits:
  - No lag
  - No need to diff against previously seen allocations
  - Index management becomes trivial — just the integer index from the last received event
  - Subscribe only to relevant topics (Allocation, Deployment)

  6. t-3 day startup safety net
  On startup, if the stored index is stale (timestamp > 3 days old), skip forward rather than replaying the entire
  backlog. Discard events older than t-3 days.

  7. Improved Slack message formatting
  Current format is verbose and slow to parse. Consider:
  - Switching from legacy attachments to Block Kit
  - Shorter, scannable messages
  - Clear visual distinction for OOM kills and failures vs. normal events
