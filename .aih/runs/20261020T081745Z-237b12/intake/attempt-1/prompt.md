You are the intake step of a coding harness. Your only job is to turn the user's request into a
short list of concrete intents, or to ask the few questions without which that is not possible.
You may read the repository to understand terms the request uses, but do not plan or design.

## Request

Add a "draining" scheduling state for Dags (issue #22006): an operator stops new scheduled runs
while the work already in flight finishes, and the Dag ends up paused on its own.

Pausing a Dag today is abrupt. Running task instances finish, but queued and downstream tasks of
the current runs are stranded until the Dag is unpaused, and nothing says "stop starting new runs,
let the current ones complete". That bites during upgrades and maintenance windows; the workaround
in the wild is to set every schedule to None, wait, do the work, and put the schedules back. The
design agreed on the issue: a third mode next to paused and unpaused, "drain, then pause", that is
transient and switches to paused by itself when the drain is complete. Pause and unpause stay
available from the API and the CLI as today, and existing clients that only know `is_paused` must
keep working unchanged.

Behaviour to deliver:

- States. A Dag's scheduling state is `active`, `draining` or `paused` (enum `DagSchedulingState`
  in `airflow.utils.state`, with exactly those string values). `draining` is transient and always
  converges to `paused`; the resting states remain `active` and `paused`. A Dag is never paused and
  draining at the same time.
- Model and migration. `DagModel` gets a non-null boolean column `is_draining` (default false,
  indexed, with the check constraint `NOT (is_paused AND is_draining)`), a read-only property
  `scheduling_state` derived from the two flags, and `set_scheduling_state(state)` that sets both
  flags consistently. The Alembic migration belongs to 3.4.0 (migrations reference updated) and
  also adds an index on `backfill (dag_id, completed_at)`, which the scheduler needs to find a
  backfill that is accepted but not yet initialised. `is_paused` keeps its meaning everywhere.
- Scheduler, while a Dag is draining. No new scheduler-created runs: the Dag is excluded from the
  Dags needing a run (scheduled runs), an asset event is still recorded but does not queue the Dag
  (whether it is scheduled on the asset directly, through an alias, or by asset name or URI), and
  no run is created for pending partitioned-asset events of the Dag. Task instances of its queued
  and running runs keep being scheduled and queued exactly as for an active Dag (unlike a paused
  Dag), and its unfinished runs keep being examined. Explicit triggers (manual, operator-triggered,
  asset materialisation, backfill) remain available while draining, as they are for a paused Dag,
  and simply extend the drain until the resulting work finishes.
- Finalisation. The scheduler runs a step `_finalize_draining_dags(session)` on
  `SchedulerJobRunner` every 5 seconds in its loop. It moves a draining Dag to `paused` once the
  Dag has no unfinished run (queued or running) and no backfill that is accepted but not yet
  initialised (a backfill row without associated Dag runs: the backfill is committed before its
  runs exist), and it records an audit `Log` entry with event `drain_completed` for that Dag, owner
  `scheduler`. Dags are processed in `dag_id` order with the usual per-query limit and row locks.
- Automatic pausing after the configured number of consecutive failed runs pauses the Dag and
  clears draining.
- Public REST API. `DAGResponse` and `DAGDetailsResponse` gain `scheduling_state` (default
  `active`). `PATCH /dags/{dag_id}` and `PATCH /dags` accept either `is_paused` (boolean) or
  `scheduling_state`: exactly one of the two must be non-null, otherwise 422 (both present, both
  null, or the only field sent as null). Generated clients send the field they are not using as an
  explicit null, which is accepted. `update_mask` may name `is_paused` or `scheduling_state`, one
  of them; a mask that names the other field than the one the body carries is a 422. The response
  reflects the new state; the bulk PATCH sets both flags for every matched Dag. Clients that send
  `is_paused` behave as before (true pauses, false activates, and both clear draining).
- UI API (`/ui/dags`). A `scheduling_state` filter (`active`, `draining`, `paused`); the existing
  `paused` filter keeps its meaning (`paused=false` includes draining Dags). Every Dag in the list
  carries `has_unfinished_runs`, true when any queued or running run exists, regardless of
  `dag_runs_limit` (which only limits the latest runs shown). The dashboard's `active_dag_count`
  does not count draining Dags.
- CLI. `airflow dags pause` and `airflow dags unpause` set `paused` and `active` respectively and
  clear draining; a draining Dag counts as a Dag the command has to change. `dags details`
  includes `scheduling_state`.
- Documentation: the Dag concepts page and the FAQ describe draining; a newsfragment announces it.

Out of scope: the React UI under `airflow-core/src/airflow/ui` is handled in a separate change; do
not touch it. Do not change unrelated behaviour.

Add or extend the unit tests for the model, the scheduler, the asset manager, the public and UI
REST routes and the CLI. The existing test suites must stay green.



## Output

Return JSON matching the schema you were given:

- `status`: `ready` when the intents are unambiguous enough to plan from; `needs_clarification`
  when a reasonable engineer would have to ask before starting.
- `intents`: one entry per distinct thing the user wants, each with a short `key` (kebab-case)
  and one sentence of `text` that a later reviewer can check against the final code. Split
  compound requests. Do not invent requirements the user did not state.
- `constraints`: explicit limits the user set ("do not change X", "keep the public API").
- `questions`: at most 3, only when `status` is `needs_clarification`, each with a one-line
  `why`. Ask only what changes what you would build.

## When you must ask

Return `needs_clarification` whenever any of these holds; do not resolve them with assumptions:

- The request names a goal without a checkable target ("faster", "better", "cleaner",
  "more robust") and does not say which code path, which measurement or which threshold.
- Two reasonable engineers would build materially different things from the same words.
- The request does not say what must not change, and the obvious implementation would alter
  existing public behaviour.
- A term in the request matches nothing in the repository.

Return `ready` when the request names the concrete behaviour, place or artifact and the only open
points are implementation details you would decide yourself. A small assumption that does not
change what gets built may be stated in the intent text instead of asked. Inventing scope the
user did not mention (benchmarks, documentation, refactors) is never acceptable: either ask or
leave it out.

When `status` is `ready`, `questions` must be empty. When it is `needs_clarification`, still fill
`intents` with your best current understanding.
