import {
  OrchestratorMcpFailure,
  OrchestratorMcpTarget,
  OrchestratorMcpTaskRole,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";

const DEFAULT_TIMEOUT_MS = 60 * 60 * 1_000;

const WorkflowId = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9_-]{1,64}$/));

export const WorkflowAgent = Schema.Struct({
  id: WorkflowId,
  task: TrimmedNonEmptyString,
  target: Schema.optional(OrchestratorMcpTarget),
  role: Schema.optional(OrchestratorMcpTaskRole),
  isolation: Schema.optional(Schema.Literals(["shared", "worktree"])),
  resultSchema: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
});
export type WorkflowAgent = typeof WorkflowAgent.Type;

const AgentStage = Schema.Struct({ id: WorkflowId, agents: Schema.NonEmptyArray(WorkflowAgent) });
type AgentStage = typeof AgentStage.Type;

const RepeatStage = Schema.Struct({
  id: WorkflowId,
  repeat: Schema.Struct({
    stages: Schema.NonEmptyArray(AgentStage),
    judge: WorkflowId,
    until: Schema.Struct({
      path: TrimmedNonEmptyString,
      equals: Schema.Union([Schema.String, Schema.Number, Schema.Boolean, Schema.Null]),
    }),
    max: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 10 })),
    onExhausted: Schema.optional(Schema.Literals(["fail", "pass"])),
  }),
});
type RepeatStage = typeof RepeatStage.Type;

export const WorkflowSpec = Schema.Struct({
  name: TrimmedNonEmptyString,
  stages: Schema.NonEmptyArray(Schema.Union([AgentStage, RepeatStage])),
  timeoutMs: Schema.optional(Schema.Int.check(Schema.isGreaterThan(0))),
});
export type WorkflowSpec = typeof WorkflowSpec.Type;

export const WorkflowAgentOutcome = Schema.Struct({
  status: Schema.Literals(["completed", "failed", "cancelled", "interrupted", "invalid_output"]),
  result: Schema.NullOr(Schema.String),
  structuredResult: Schema.optional(Schema.Unknown),
});
export type WorkflowAgentOutcome = typeof WorkflowAgentOutcome.Type;

const WorkflowLaunch = Schema.Struct({
  stage: WorkflowId,
  round: Schema.Int,
  agent: WorkflowId,
  taskId: Schema.NullOr(Schema.String),
  cancelRequested: Schema.Boolean,
  outcome: Schema.NullOr(WorkflowAgentOutcome),
  queuedAt: Schema.optional(Schema.Number),
  startedAt: Schema.optional(Schema.Number),
  completedAt: Schema.optional(Schema.Number),
});
type WorkflowLaunch = typeof WorkflowLaunch.Type;

const WorkflowRunStatus = Schema.Literals(["completed", "failed", "cancelled", "timed_out"]);

/** Everything a restarted server needs to continue a run: the TLA model's durable row. */
export const WorkflowSnapshot = Schema.Struct({
  status: Schema.Union([Schema.Literal("running"), WorkflowRunStatus]),
  cursor: Schema.Int,
  round: Schema.Int,
  phase: Schema.Int,
  deadline: Schema.Number,
  stop: Schema.Boolean,
  launches: Schema.Array(WorkflowLaunch),
  exits: Schema.Array(
    Schema.Struct({
      stage: WorkflowId,
      reason: Schema.Literals(["accepted", "exhausted"]),
      round: Schema.Int,
    }),
  ),
});
export type WorkflowSnapshot = typeof WorkflowSnapshot.Type;

export interface WorkflowAgentStart {
  readonly key: string;
  readonly stage: string;
  readonly round: number;
  readonly agent: WorkflowAgent;
  readonly task: string;
}

/**
 * The operations a run drives. `start` must be idempotent per `key`: after a crash between
 * persisting a launch and recording its task id, the runner calls it again with the same key.
 */
export interface WorkflowPort {
  readonly start: (launch: WorkflowAgentStart) => Effect.Effect<string, OrchestratorMcpFailure>;
  readonly await: (taskId: string) => Effect.Effect<WorkflowAgentOutcome, OrchestratorMcpFailure>;
  readonly cancel: (taskId: string) => Effect.Effect<void, OrchestratorMcpFailure>;
  readonly persist: (snapshot: WorkflowSnapshot) => Effect.Effect<void, OrchestratorMcpFailure>;
}

type AgentResult = {
  readonly id: string;
  readonly status: WorkflowAgentOutcome["status"] | "running" | "skipped";
  readonly taskId: string | null;
  readonly result: string | null;
  readonly structuredResult?: unknown;
};
type StageResult = { readonly id: string; readonly agents: ReadonlyArray<AgentResult> };
export type WorkflowResult = {
  readonly name: string;
  readonly status: WorkflowSnapshot["status"];
  readonly stages: ReadonlyArray<
    | StageResult
    | {
        readonly id: string;
        readonly exit: "accepted" | "exhausted" | null;
        readonly rounds: ReadonlyArray<{
          readonly round: number;
          readonly stages: ReadonlyArray<StageResult>;
        }>;
      }
  >;
};

export const launchKey = (stage: string, round: number, agent: string) =>
  `${stage}.${agent}#${round}`;

const REFERENCE = /\{\{\s*([A-Za-z0-9_-]+)(?:\.([A-Za-z0-9_-]+))?\s*\}\}/g;
const CANCELLED: WorkflowAgentOutcome = { status: "cancelled", result: null };

const invalid = (message: string) =>
  new OrchestratorMcpFailure({ code: "invalid_request", message });

// Braces that name no stage stay literal, so a task can still show `{{x}}` syntax.
function referenceError(spec: WorkflowSpec): string | null {
  const ids = new Set<string>();
  const repeats = new Set<string>();
  const order: Array<{ stage: AgentStage; repeat: RepeatStage | null }> = [];
  for (const stage of spec.stages) {
    const inner = "agents" in stage ? [stage] : stage.repeat.stages;
    for (const id of [stage.id, ...("agents" in stage ? [] : inner.map((s) => s.id))]) {
      if (id === "feedback") return `"feedback" is reserved and cannot name a stage.`;
      if (ids.has(id)) return `Duplicate stage id "${id}".`;
      ids.add(id);
    }
    if (!("agents" in stage)) {
      repeats.add(stage.id);
      const judge = inner.at(-1)?.agents.find((agent) => agent.id === stage.repeat.judge);
      if (judge === undefined) {
        return `Repeat "${stage.id}": judge "${stage.repeat.judge}" must be an agent of its last stage.`;
      }
      if (judge.resultSchema === undefined) {
        return `Repeat "${stage.id}": judge "${judge.id}" needs a resultSchema for until.`;
      }
    }
    for (const s of inner) order.push({ stage: s, repeat: "agents" in stage ? null : stage });
  }
  const index = new Map(order.map(({ stage }, i) => [stage.id, i]));
  for (const [position, { stage, repeat }] of order.entries()) {
    const agentIds = new Set<string>();
    for (const agent of stage.agents) {
      if (agentIds.has(agent.id)) return `Duplicate agent id "${stage.id}.${agent.id}".`;
      agentIds.add(agent.id);
      for (const [text, stageRef, agentRef] of agent.task.matchAll(REFERENCE)) {
        const where = `"${stage.id}.${agent.id}" uses ${text}`;
        if (stageRef === "feedback") {
          if (repeat === null || agentRef !== undefined) return `${where} outside a repeat.`;
          continue;
        }
        if (stageRef === undefined) continue;
        if (repeats.has(stageRef)) return `${where}; name one of the repeat's stages instead.`;
        const target = index.get(stageRef);
        if (target === undefined) continue;
        if (target >= position) return `${where}, which has not run yet.`;
        const known = order[target]?.stage.agents.some((a) => a.id === agentRef) ?? false;
        if (agentRef !== undefined && !known) return `${where}, which names no agent there.`;
      }
    }
  }
  return null;
}

const decodeSpec = Schema.decodeUnknownEffect(WorkflowSpec);

/** Decodes a spec and rejects unknown or forward references before anything starts. */
export const decodeWorkflowSpec = Effect.fn("WorkflowRun.decodeSpec")(function* (input: unknown) {
  const spec = yield* decodeSpec(input).pipe(Effect.mapError((error) => invalid(error.message)));
  const error = referenceError(spec);
  if (error !== null) return yield* invalid(error);
  return spec;
});

const findLaunch = (state: WorkflowSnapshot, stage: string, round: number, agent: string) =>
  state.launches.find((l) => l.stage === stage && l.round === round && l.agent === agent);

const valueOf = (outcome: WorkflowAgentOutcome) =>
  outcome.structuredResult === undefined ? outcome.result : outcome.structuredResult;

const asText = (value: unknown) => (typeof value === "string" ? value : JSON.stringify(value));

export const agentStages = (spec: WorkflowSpec) =>
  spec.stages.flatMap((stage) => ("agents" in stage ? [stage] : stage.repeat.stages));

/** Every stage the run shows, in order: a repeat's stages once per round it reached, round 1 up front. */
export function workflowPhases(
  spec: WorkflowSpec,
  state: Pick<WorkflowSnapshot, "cursor" | "round" | "exits">,
): ReadonlyArray<{ readonly stage: AgentStage; readonly round: number; readonly title: string }> {
  return spec.stages.flatMap((stage, position) => {
    if ("agents" in stage) return [{ stage, round: 1, title: stage.id }];
    const rounds =
      position === state.cursor
        ? state.round
        : (state.exits.find((exit) => exit.stage === stage.id)?.round ?? 1);
    return Array.from({ length: rounds }, (_, index) =>
      stage.repeat.stages.map((inner) => ({
        stage: inner,
        round: index + 1,
        title: `${inner.id} · round ${index + 1}`,
      })),
    ).flat();
  });
}

function renderTask(spec: WorkflowSpec, state: WorkflowSnapshot, template: string): string {
  return template.replace(REFERENCE, (match, stageRef: string, agentRef?: string) => {
    if (stageRef === "feedback") {
      const stage = spec.stages[state.cursor];
      if (stage === undefined || "agents" in stage) return match;
      const judges = stage.repeat.stages.at(-1)?.id ?? "";
      const judge = findLaunch(state, judges, state.round - 1, stage.repeat.judge);
      return JSON.stringify(judge?.outcome?.structuredResult ?? null);
    }
    if (!agentStages(spec).some((stage) => stage.id === stageRef)) return match;
    const latest = new Map<string, unknown>();
    for (const launch of state.launches) {
      if (launch.stage === stageRef && launch.outcome?.status === "completed") {
        latest.set(launch.agent, valueOf(launch.outcome));
      }
    }
    if (agentRef !== undefined) return asText(latest.get(agentRef) ?? null);
    return JSON.stringify(Object.fromEntries(latest));
  });
}

function accepts(until: RepeatStage["repeat"]["until"], value: unknown): boolean {
  let current = value;
  for (const segment of until.path.split(".")) {
    if (typeof current !== "object" || current === null) return false;
    current = (current as Record<string, unknown>)[segment];
  }
  return current === until.equals;
}

type Step =
  | { readonly _tag: "finish"; readonly status: typeof WorkflowRunStatus.Type }
  | { readonly _tag: "launch"; readonly stage: string; readonly agent: WorkflowAgent }
  | { readonly _tag: "advance"; readonly state: WorkflowSnapshot }
  | { readonly _tag: "wait" };

const finish = (status: typeof WorkflowRunStatus.Type): Step => ({ _tag: "finish", status });
const advance = (state: WorkflowSnapshot): Step => ({ _tag: "advance", state });

/** The TLA model's runner actions, as a pure choice over the persisted state. */
function decide(spec: WorkflowSpec, state: WorkflowSnapshot, now: number): Step {
  if (state.stop) return finish("cancelled");
  if (now >= state.deadline) return finish("timed_out");
  if (state.launches.some((l) => l.outcome !== null && l.outcome.status !== "completed")) {
    return finish("failed");
  }
  const stage = spec.stages[state.cursor];
  if (stage === undefined) return finish("completed");
  const phase = "agents" in stage ? stage : stage.repeat.stages[state.phase];
  if (phase === undefined) return finish("failed");
  const launches = phase.agents.map((agent) => ({
    agent,
    launch: findLaunch(state, phase.id, state.round, agent.id),
  }));
  const unlaunched = launches.find(({ launch }) => launch === undefined);
  if (unlaunched !== undefined) return { _tag: "launch", stage: phase.id, agent: unlaunched.agent };
  if (launches.some(({ launch }) => launch?.outcome === null)) return { _tag: "wait" };

  const next = { ...state, cursor: state.cursor + 1, round: 1, phase: 0 };
  if ("agents" in stage) return advance(next);
  const { repeat } = stage;
  if (state.phase < repeat.stages.length - 1) return advance({ ...state, phase: state.phase + 1 });
  const exit = (reason: "accepted" | "exhausted") => ({
    ...next,
    exits: [...state.exits, { stage: stage.id, reason, round: state.round }],
  });
  const judge = findLaunch(state, phase.id, state.round, repeat.judge);
  if (accepts(repeat.until, judge?.outcome?.structuredResult)) return advance(exit("accepted"));
  if (state.round < repeat.max) return advance({ ...state, round: state.round + 1, phase: 0 });
  return repeat.onExhausted === "pass" ? advance(exit("exhausted")) : finish("failed");
}

const findAgent = (spec: WorkflowSpec, launch: WorkflowLaunch) =>
  agentStages(spec)
    .find((stage) => stage.id === launch.stage)
    ?.agents.find((agent) => agent.id === launch.agent);

type WorkflowEvent =
  | { readonly _tag: "settled"; readonly key: string; readonly outcome: WorkflowAgentOutcome }
  | { readonly _tag: "tick" };

/** Continues from the last persisted snapshot: re-awaits live tasks, never relaunches one. */
export const resumeWorkflow = Effect.fn("WorkflowRun.resume")(function* (
  port: WorkflowPort,
  spec: WorkflowSpec,
  initial: WorkflowSnapshot,
) {
  const events = yield* Queue.unbounded<WorkflowEvent>();
  let state = initial;
  const save = (next: WorkflowSnapshot) => {
    state = next;
    return port.persist(next);
  };
  const keyOf = (l: WorkflowLaunch) => launchKey(l.stage, l.round, l.agent);
  const update = (key: string, patch: Partial<WorkflowLaunch>) =>
    save({
      ...state,
      launches: state.launches.map((l) => (keyOf(l) === key ? { ...l, ...patch } : l)),
    });
  const cancelRequested = Effect.suspend(() =>
    Effect.forEach(
      state.launches.flatMap((l) => (l.cancelRequested && l.taskId !== null ? [l.taskId] : [])),
      (taskId) => Effect.ignore(port.cancel(taskId)),
      { discard: true },
    ),
  );

  if (state.status !== "running") {
    yield* cancelRequested;
    return workflowResult(spec, state);
  }

  const watch = (key: string, taskId: string) =>
    port.await(taskId).pipe(
      Effect.catch((error) =>
        Effect.succeed<WorkflowAgentOutcome>({ status: "failed", result: error.message }),
      ),
      Effect.flatMap((outcome) => Queue.offer(events, { _tag: "settled", key, outcome })),
      Effect.forkScoped,
    );
  const startTask = Effect.fn("WorkflowRun.startAgent")(function* (launch: WorkflowLaunch) {
    const key = keyOf(launch);
    const agent = findAgent(spec, launch);
    if (agent === undefined) {
      return yield* update(key, { outcome: { status: "failed", result: "Not in the spec." } });
    }
    const task = renderTask(spec, state, agent.task);
    const started = yield* port
      .start({ key, stage: launch.stage, round: launch.round, agent, task })
      .pipe(Effect.result);
    const now = yield* Clock.currentTimeMillis;
    if (started._tag === "Failure") {
      const outcome = { status: "failed", result: started.failure.message } as const;
      return yield* update(key, { outcome, completedAt: now });
    }
    yield* update(key, { taskId: started.success, startedAt: now });
    yield* watch(key, started.success);
  });

  const remaining = state.deadline - (yield* Clock.currentTimeMillis);
  if (remaining > 0) {
    yield* Effect.sleep(remaining).pipe(
      Effect.andThen(Queue.offer(events, { _tag: "tick" })),
      Effect.forkScoped,
    );
  }
  for (const launch of state.launches) {
    if (launch.outcome !== null) continue;
    if (launch.taskId === null) yield* startTask(launch);
    else yield* watch(keyOf(launch), launch.taskId);
  }

  const apply = (event: WorkflowEvent) => {
    if (event._tag === "tick") return Effect.void;
    const launch = state.launches.find((l) => keyOf(l) === event.key);
    return launch === undefined || launch.outcome !== null
      ? Effect.void
      : Effect.flatMap(Clock.currentTimeMillis, (completedAt) =>
          update(event.key, { outcome: event.outcome, completedAt }),
        );
  };

  while (state.status === "running") {
    let pending = yield* Queue.poll(events);
    while (Option.isSome(pending)) {
      yield* apply(pending.value);
      pending = yield* Queue.poll(events);
    }
    const step = decide(spec, state, yield* Clock.currentTimeMillis);
    switch (step._tag) {
      case "finish":
        yield* save({
          ...state,
          status: step.status,
          launches: state.launches.map((l) =>
            l.outcome === null ? { ...l, cancelRequested: true, outcome: CANCELLED } : l,
          ),
        });
        yield* cancelRequested;
        break;
      case "launch": {
        const launch: WorkflowLaunch = {
          stage: step.stage,
          round: state.round,
          agent: step.agent.id,
          taskId: null,
          cancelRequested: false,
          outcome: null,
          queuedAt: yield* Clock.currentTimeMillis,
        };
        yield* save({ ...state, launches: [...state.launches, launch] });
        yield* startTask(launch);
        break;
      }
      case "advance":
        state = step.state;
        break;
      case "wait":
        yield* apply(yield* Queue.take(events));
    }
  }
  return workflowResult(spec, state);
}, Effect.scoped);

export const initialSnapshot = (spec: WorkflowSpec, now: number): WorkflowSnapshot => ({
  status: "running",
  cursor: 0,
  round: 1,
  phase: 0,
  deadline: now + (spec.timeoutMs ?? DEFAULT_TIMEOUT_MS),
  stop: false,
  launches: [],
  exits: [],
});

export function workflowResult(spec: WorkflowSpec, state: WorkflowSnapshot): WorkflowResult {
  const agents = (stage: AgentStage, round: number): StageResult => ({
    id: stage.id,
    agents: stage.agents.map((agent): AgentResult => {
      const launch = findLaunch(state, stage.id, round, agent.id);
      if (launch === undefined) {
        return { id: agent.id, status: "skipped", taskId: null, result: null };
      }
      const { outcome } = launch;
      return {
        id: agent.id,
        status: outcome?.status ?? "running",
        taskId: launch.taskId,
        result: outcome?.result ?? null,
        ...(outcome?.structuredResult === undefined
          ? {}
          : { structuredResult: outcome.structuredResult }),
      };
    }),
  });
  return {
    name: spec.name,
    status: state.status,
    stages: spec.stages.map((stage) => {
      if ("agents" in stage) return agents(stage, 1);
      const ids = new Set(stage.repeat.stages.map((s) => s.id));
      const rounds = Math.max(
        1,
        ...state.launches.filter((l) => ids.has(l.stage)).map((l) => l.round),
      );
      return {
        id: stage.id,
        exit: state.exits.find((e) => e.stage === stage.id)?.reason ?? null,
        rounds: Array.from({ length: rounds }, (_, i) => ({
          round: i + 1,
          stages: stage.repeat.stages.map((s) => agents(s, i + 1)),
        })),
      };
    }),
  };
}
