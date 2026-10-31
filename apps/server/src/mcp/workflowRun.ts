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

const RoundCount = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 10 }));

const RepeatStage = Schema.Struct({
  id: WorkflowId,
  repeat: Schema.Struct({
    stages: Schema.NonEmptyArray(AgentStage),
    judge: WorkflowId,
    until: Schema.Struct({
      path: TrimmedNonEmptyString,
      equals: Schema.Union([Schema.String, Schema.Number, Schema.Boolean, Schema.Null]),
    }),
    max: RoundCount,
    onExhausted: Schema.optional(Schema.Literals(["fail", "pass"])),
    dry: Schema.optional(RoundCount),
    templates: Schema.optional(
      Schema.Record(
        Schema.String,
        Schema.Struct({
          task: WorkflowAgent.fields.task,
          target: WorkflowAgent.fields.target,
          resultSchema: WorkflowAgent.fields.resultSchema,
        }),
      ),
    ),
  }),
});
type RepeatStage = typeof RepeatStage.Type;

/** What a judge's structured result may ask of the next round, beyond `until`. */
const Growth = Schema.Struct({
  addAgents: Schema.optional(
    Schema.Array(
      Schema.Struct({
        key: WorkflowId,
        template: Schema.String,
        input: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
        stage: Schema.optional(Schema.String),
      }),
    ),
  ),
  rerun: Schema.optional(Schema.Array(Schema.String)),
});
const decodeGrowth = Schema.decodeUnknownOption(Growth);

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
      reason: Schema.Literals(["accepted", "exhausted", "dry"]),
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
        readonly exit: WorkflowSnapshot["exits"][number]["reason"] | null;
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
  const order: Array<{
    stage: {
      readonly id: string;
      readonly agents: ReadonlyArray<Pick<WorkflowAgent, "id" | "task">>;
    };
    repeat: RepeatStage | null;
  }> = [];
  for (const stage of spec.stages) {
    const inner = "agents" in stage ? [stage] : stage.repeat.stages;
    for (const id of [stage.id, ...("agents" in stage ? [] : inner.map((s) => s.id))]) {
      if (id === "feedback" || id === "input")
        return `"${id}" is reserved and cannot name a stage.`;
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
    // Templates check as if after the repeat's last stage: a grown agent may join any of them.
    if (!("agents" in stage)) {
      const agents = Object.entries(stage.repeat.templates ?? {}).map(([id, { task }]) => ({
        id,
        task,
      }));
      order.push({ stage: { id: stage.id, agents }, repeat: stage });
    }
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

const judgedIn = (state: WorkflowSnapshot, stage: RepeatStage, round: number) =>
  findLaunch(state, stage.repeat.stages.at(-1)?.id ?? "", round, stage.repeat.judge)?.outcome
    ?.structuredResult;

/** The judge's requests for the next round; null when it names a template or stage not in the repeat. */
function growthOf(stage: RepeatStage, value: unknown) {
  const growth = Option.getOrNull(decodeGrowth(value));
  if (growth === null) return null;
  const { stages, templates = {} } = stage.repeat;
  const ids = stages.map((s) => s.id);
  const adds = (growth.addAgents ?? []).map((add) => ({
    ...add,
    stage: add.stage ?? stages[0].id,
  }));
  const known =
    adds.every((add) => ids.includes(add.stage) && Object.hasOwn(templates, add.template)) &&
    (growth.rerun ?? []).every((id) => ids.includes(id));
  return known ? { adds, rerun: growth.rerun ?? [] } : null;
}

const isGrown = (stage: RepeatStage, launch: WorkflowLaunch) =>
  stage.repeat.stages.some(
    (s) => s.id === launch.stage && !s.agents.some((agent) => agent.id === launch.agent),
  );

/** Round 1 and the judge's stage always run; later rounds run what the last judge named, or all. */
function runsAgain(stage: RepeatStage, state: WorkflowSnapshot, phaseId: string) {
  if (state.round === 1 || phaseId === stage.repeat.stages.at(-1)?.id) return true;
  const rerun = growthOf(stage, judgedIn(state, stage, state.round - 1))?.rerun ?? [];
  return rerun.length === 0 || rerun.includes(phaseId);
}

/** Consecutive dry votes ending at this round: rounds that grew nothing and asked for no rerun. */
function dryStreak(stage: RepeatStage, state: WorkflowSnapshot, dryNow: boolean) {
  const wasDry = (round: number) =>
    !state.launches.some((l) => l.round === round + 1 && isGrown(stage, l)) &&
    !growthOf(stage, judgedIn(state, stage, round))?.rerun.length;
  let streak = dryNow ? 1 : 0;
  while (streak > 0 && streak < state.round && wasDry(state.round - streak)) streak += 1;
  return streak;
}

function renderTask(
  spec: WorkflowSpec,
  state: WorkflowSnapshot,
  template: string,
  input?: Readonly<Record<string, unknown>>,
): string {
  return template.replace(REFERENCE, (match, stageRef: string, agentRef?: string) => {
    if (stageRef === "feedback") {
      const stage = spec.stages[state.cursor];
      if (stage === undefined || "agents" in stage) return match;
      return JSON.stringify(judgedIn(state, stage, state.round - 1) ?? null);
    }
    if (stageRef === "input" && input !== undefined) {
      return agentRef === undefined ? JSON.stringify(input) : asText(input[agentRef] ?? null);
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
  | { readonly _tag: "start" | "record"; readonly launch: WorkflowLaunch }
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
  const loop = "agents" in stage ? null : stage;
  const phase = "agents" in stage ? stage : stage.repeat.stages[state.phase];
  if (phase === undefined) return finish("failed");
  if (loop === null || runsAgain(loop, state, phase.id)) {
    const unlaunched = phase.agents.find(
      (agent) => findLaunch(state, phase.id, state.round, agent.id) === undefined,
    );
    if (unlaunched !== undefined) return { _tag: "launch", stage: phase.id, agent: unlaunched };
  }
  // Grown agents, and launches a crash cut off before their task id was recorded.
  const current = state.launches.filter((l) => l.stage === phase.id && l.round === state.round);
  const intent = current.find((l) => l.taskId === null && l.outcome === null);
  if (intent !== undefined) return { _tag: "start", launch: intent };
  if (current.some((l) => l.outcome === null)) return { _tag: "wait" };

  const next = { ...state, cursor: state.cursor + 1, round: 1, phase: 0 };
  if (loop === null) return advance(next);
  const { repeat } = loop;
  if (state.phase < repeat.stages.length - 1) return advance({ ...state, phase: state.phase + 1 });
  const exit = (reason: WorkflowSnapshot["exits"][number]["reason"]) => ({
    ...next,
    exits: [...state.exits, { stage: loop.id, reason, round: state.round }],
  });
  const value = judgedIn(state, loop, state.round);
  if (accepts(repeat.until, value)) return advance(exit("accepted"));
  // A missing or malformed judge result fails the run; it never counts as a dry vote.
  const growth = growthOf(loop, value);
  if (growth === null) return finish("failed");
  const round = state.round + 1;
  const recorded = state.launches.filter((l) => l.round === round && isGrown(loop, l));
  const seen = new Set([
    ...repeat.stages.flatMap((s) => s.agents.map((agent) => agent.id)),
    ...state.launches.filter((l) => l.round < round && isGrown(loop, l)).map((l) => l.agent),
  ]);
  const grown = growth.adds.filter(({ key }) => {
    const fresh = !seen.has(key);
    seen.add(key);
    return fresh;
  });
  const dryNow = grown.length === 0 && growth.rerun.length === 0;
  if (repeat.dry !== undefined && dryStreak(loop, state, dryNow) >= repeat.dry) {
    return advance(exit("dry"));
  }
  if (state.round === repeat.max) {
    return repeat.onExhausted === "pass" ? advance(exit("exhausted")) : finish("failed");
  }
  // One row per update keeps each within the wire budget; all land before the next round starts.
  const add = grown[recorded.length];
  if (add === undefined) return advance({ ...state, round, phase: 0 });
  return {
    _tag: "record",
    launch: {
      stage: add.stage,
      round,
      agent: add.key,
      taskId: null,
      cancelRequested: false,
      outcome: null,
    },
  };
}

/** A spec agent, or a grown one built from its template and the input its judge gave it. */
function findAgent(spec: WorkflowSpec, state: WorkflowSnapshot, launch: WorkflowLaunch) {
  for (const stage of spec.stages) {
    const phase = ("agents" in stage ? [stage] : stage.repeat.stages).find(
      (s) => s.id === launch.stage,
    );
    if (phase === undefined) continue;
    const agent = phase.agents.find((a) => a.id === launch.agent);
    if (agent !== undefined || "agents" in stage) return agent && { agent };
    const add = growthOf(stage, judgedIn(state, stage, launch.round - 1))?.adds.find(
      (a) => a.key === launch.agent,
    );
    const template = add && stage.repeat.templates?.[add.template];
    if (add === undefined || template === undefined) return undefined;
    return { agent: { ...template, id: add.key }, input: add.input ?? {} };
  }
  return undefined;
}

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
    const found = findAgent(spec, state, launch);
    if (found === undefined) {
      return yield* update(key, { outcome: { status: "failed", result: "Not in the spec." } });
    }
    const { agent } = found;
    const task = renderTask(spec, state, agent.task, found.input);
    const started = yield* port
      .start({ key, stage: launch.stage, round: launch.round, agent, task })
      .pipe(Effect.result);
    if (started._tag === "Failure") {
      const outcome = { status: "failed", result: started.failure.message } as const;
      return yield* update(key, { outcome });
    }
    yield* update(key, { taskId: started.success });
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
    if (launch.outcome === null && launch.taskId !== null)
      yield* watch(keyOf(launch), launch.taskId);
  }

  const apply = (event: WorkflowEvent) => {
    if (event._tag === "tick") return Effect.void;
    const launch = state.launches.find((l) => keyOf(l) === event.key);
    return launch === undefined || launch.outcome !== null
      ? Effect.void
      : update(event.key, { outcome: event.outcome });
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
        };
        yield* save({ ...state, launches: [...state.launches, launch] });
        yield* startTask(launch);
        break;
      }
      case "start":
        yield* startTask(step.launch);
        break;
      case "record":
        yield* save({ ...state, launches: [...state.launches, step.launch] });
        break;
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
  const agentResult = (id: string, launch: WorkflowLaunch | undefined): AgentResult => {
    if (launch === undefined) return { id, status: "skipped", taskId: null, result: null };
    const { outcome } = launch;
    return {
      id,
      status: outcome?.status ?? "running",
      taskId: launch.taskId,
      result: outcome?.result ?? null,
      ...(outcome?.structuredResult === undefined
        ? {}
        : { structuredResult: outcome.structuredResult }),
    };
  };
  const agents = (stage: AgentStage, round: number): StageResult => ({
    id: stage.id,
    agents: [
      ...stage.agents.map((agent) =>
        agentResult(agent.id, findLaunch(state, stage.id, round, agent.id)),
      ),
      ...state.launches
        .filter(
          (l) =>
            l.stage === stage.id &&
            l.round === round &&
            !stage.agents.some((agent) => agent.id === l.agent),
        )
        .map((l) => agentResult(l.agent, l)),
    ],
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
