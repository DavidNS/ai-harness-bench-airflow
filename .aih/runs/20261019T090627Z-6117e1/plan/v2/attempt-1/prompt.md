You are planning a change in this repository. You can read files but not edit them or run
commands. Another session will implement your plan, one task at a time, each in a fresh context, and a
harness will check the result against it mechanically. Explore the repository as you see fit;
only your final JSON answer is used.

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

## Intents (use these ids exactly)

- `INTENT1` (scheduling-state-enum): Add enum `DagSchedulingState` in `airflow.utils.state` with exactly the string values `active`, `draining`, `paused`.
- `INTENT2` (model-and-migration): Add non-null boolean `DagModel.is_draining` (default false, indexed, check constraint `NOT (is_paused AND is_draining)`), read-only `scheduling_state` property and `set_scheduling_state(state)` setting both flags consistently, with a 3.4.0 Alembic migration (also adding index on `backfill (dag_id, completed_at)`) and the migrations reference updated.
- `INTENT3` (scheduler-no-new-runs): While a Dag is draining, exclude it from Dags needing scheduled runs, record asset events without queuing the Dag (direct, alias, name/URI), and create no run for its pending partitioned-asset events.
- `INTENT4` (scheduler-continues-inflight): For a draining Dag, keep scheduling/queuing task instances of queued and running runs and examining unfinished runs as for an active Dag; explicit triggers (manual, operator, asset materialisation, backfill) remain allowed.
- `INTENT5` (drain-finalisation): Add `SchedulerJobRunner._finalize_draining_dags(session)` run every 5 seconds in the scheduler loop, moving a draining Dag to paused when it has no queued/running run and no accepted-but-uninitialised backfill (backfill row without Dag runs), writing a `Log` entry with event `drain_completed`, owner `scheduler`, processing in `dag_id` order with per-query limit and row locks.
- `INTENT6` (auto-pause-clears-draining): Automatic pausing after the configured consecutive failed runs pauses the Dag and clears draining.
- `INTENT7` (public-api): Add `scheduling_state` (default `active`) to `DAGResponse` and `DAGDetailsResponse`; `PATCH /dags/{dag_id}` and `PATCH /dags` require exactly one non-null of `is_paused`/`scheduling_state` (else 422, explicit null for the unused field accepted), `update_mask` may name one of them and a mismatch with the body is 422, responses reflect the new state, bulk PATCH sets both flags, and `is_paused` clients behave as before (clearing draining).
- `INTENT8` (ui-api): In `/ui/dags` add a `scheduling_state` filter (existing `paused` filter unchanged, `paused=false` includes draining), add `has_unfinished_runs` to every listed Dag independent of `dag_runs_limit`, and exclude draining Dags from dashboard `active_dag_count`.
- `INTENT9` (cli): `airflow dags pause`/`unpause` set paused/active and clear draining (a draining Dag counts as needing change), and `dags details` includes `scheduling_state`.
- `INTENT10` (docs-newsfragment): Document draining in the Dag concepts page and FAQ and add a newsfragment announcing it.
- `INTENT11` (tests): Add or extend unit tests for the model, scheduler, asset manager, public and UI REST routes and CLI, keeping existing suites green.

## Constraints

- Do not touch the React UI under airflow-core/src/airflow/ui
- Do not change unrelated behaviour
- `is_paused` keeps its meaning everywhere and existing clients that only know `is_paused` must keep working unchanged
- Pause and unpause remain available from API and CLI
- A Dag is never both paused and draining
- Draining is transient and always converges to paused

## Repository facts

- Test files are those matching: airflow-core/tests/**/*.py
- Tests are run by the harness, not by the implementer.

## Why the previous plan was rejected

Migration 0134 must add an index on backfill (dag_id, completed_at). For the ORM metadata to match the migration, the Backfill model has to declare that index, and models/backfill.py is not in the editable file list. Please add it so I can add Index("idx_backfill_dag_id_completed_at", dag_id, completed_at) to Backfill.__table_args__.. Files mentioned: airflow-core/src/airflow/models/backfill.py. Tasks already committed in the worktree (do not redo them): none.


## What the plan must contain

Return JSON matching the schema you were given.

- `intents`: every intent id above, exactly once, with a `resolution`:
  - `change`: something must change. `evidence` is optional but welcome.
  - `already_satisfied`: the repository already does it. Requires `evidence` that proves it.
  - `out_of_scope`: you propose not doing it. Requires a `rationale`; a human will decide.
- `evidence`: `path` relative to the repository root, `start`/`end` line numbers (1-based,
  inclusive) and a short `quote` that appears literally within those lines. The harness checks
  the file, the range and the quote; a wrong quote fails the plan.
- `criteria`: acceptance criteria, each tied to an `intent`. `kind` is `behavior` when a test
  can prove it, `other` otherwise (documentation, naming). Write them so that a test can be
  named for each `behavior` criterion.
- `tasks`: the units of work. **A task is an increment that could be released on its own: the
  change that would ship as one pull request.** A request that ships as one PR is ONE task, however
  many files it touches; splitting it into steps costs a fresh context per step, loses the design
  decisions between them and produces nothing releasable in between. Use several tasks only when
  the request contains pieces that could be merged and released independently of each other
  (then `depends_on` says which must land first). Never split by layer, by file or by phase.
  - `files`: every file the task may touch, with `role` (`test` if it matches the test globs,
    `production` otherwise) and `action`. The implementer is blocked from touching anything
    else. Declaring a file you will not need is cheap; forgetting one costs a replan.
  - `tests`: the test files (existing or to be created) that prove this task's criteria. Every
    `behavior` criterion must be covered by at least one task with at least one test. **Every
    path in `tests` must also appear in that task's `files`** (with `role: test`); a test path
    missing from `files` fails the plan.
  - `criteria`: the criteria keys this task satisfies.
  - `depends_on`: keys of tasks that must be committed first. No cycles. Independent tasks must
    not share a production file.
  - Existing tests that assert the old behaviour must be listed in the `files` (role `test`) and
    `tests` of the task that changes that behaviour; the implementer is blocked from touching any
    test file the task does not declare.
  - In the task `summary`, state the order of work the implementer should follow when it matters
    (add the new thing, migrate its callers and their tests, deprecate or remove the old thing
    last: many suites turn warnings into errors).
- `summary` on the plan and on each task: one or two sentences for a human reviewer.

Do not include anything the user did not ask for (docs, refactors, extra features) unless an
intent requires it; if you think it is needed, say so in the plan summary instead.
