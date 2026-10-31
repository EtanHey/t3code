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

const isWorkflowId = Schema.is(WorkflowId);

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
  error: Schema.optional(Schema.String),
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
  readonly error?: string;
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

const isRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

interface Growth {
  readonly adds: ReadonlyArray<{
    readonly key: string;
    readonly template: string;
    readonly stage: string;
    readonly task: string | undefined;
    readonly input: Readonly<Record<string, unknown>>;
  }>;
  readonly rerun: ReadonlyArray<string>;
}

/**
 * The judge's requests for the next round, or why they cannot be met. A missing key derives from
 * the judge's round and the entry's position, so dedup and resume see the same key every time.
 */
function parseGrowth(stage: RepeatStage, round: number, value: unknown): Growth | string {
  if (value === undefined) return "no structured result";
  if (!isRecord(value)) return "result is not an object";
  const { stages, templates = {} } = stage.repeat;
  const ids = stages.map((s) => s.id);
  const { addAgents = [], rerun = [] } = value;
  if (!Array.isArray(addAgents)) return "addAgents must be a list";
  if (!Array.isArray(rerun)) return "rerun must be a list";
  const adds: Array<Growth["adds"][number]> = [];
  for (const [index, add] of addAgents.entries()) {
    const at = `addAgents[${index}]`;
    if (!isRecord(add)) return `${at}: not an object`;
    const { template, task, input = {}, stage: target = ids[0] } = add;
    if (typeof template !== "string" || !Object.hasOwn(templates, template)) {
      return `${at}: unknown template ${JSON.stringify(template ?? null)}`;
    }
    const key =
      add.key ?? `${template.replace(/[^A-Za-z0-9_-]/g, "-").slice(0, 48)}-${round}-${index}`;
    if (!isWorkflowId(key)) return `${at}: bad key`;
    if (typeof target !== "string" || !ids.includes(target)) {
      return `${at}: unknown stage ${JSON.stringify(target)}`;
    }
    if (task !== undefined && (typeof task !== "string" || task.trim() === "")) {
      return `${at}: task must be text`;
    }
    if (!isRecord(input)) return `${at}: input must be an object`;
    adds.push({ key, template, stage: target, task, input });
  }
  const unknown = rerun.find((id) => typeof id !== "string" || !ids.includes(id));
  if (unknown !== undefined) return `rerun: unknown stage ${JSON.stringify(unknown)}`;
  return { adds, rerun };
}

const growthIn = (stage: RepeatStage, state: WorkflowSnapshot, round: number) => {
  const growth = parseGrowth(stage, round, judgedIn(state, stage, round));
  return typeof growth === "string" ? null : growth;
};

const isGrown = (stage: RepeatStage, launch: WorkflowLaunch) =>
  stage.repeat.stages.some(
    (s) => s.id === launch.stage && !s.agents.some((agent) => agent.id === launch.agent),
  );

/** Round 1 and the judge's stage always run; later rounds run what the last judge named, or all. */
function runsAgain(stage: RepeatStage, state: WorkflowSnapshot, phaseId: string, round: number) {
  if (round === 1 || phaseId === stage.repeat.stages.at(-1)?.id) return true;
  const rerun = growthIn(stage, state, round - 1)?.rerun ?? [];
  return rerun.length === 0 || rerun.includes(phaseId);
}

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

/** A phase's agents: its spec agents unless the last judge left the stage out, then those grown into it. */
export function phaseAgents(
  spec: WorkflowSpec,
  state: WorkflowSnapshot,
  stage: AgentStage,
  round: number,
): ReadonlyArray<string> {
  const loop = spec.stages.find(
    (s): s is RepeatStage =>
      !("agents" in s) && s.repeat.stages.some((inner) => inner.id === stage.id),
  );
  if (loop === undefined) return stage.agents.map((agent) => agent.id);
  const grown = state.launches.filter(
    (l) => l.stage === stage.id && l.round === round && isGrown(loop, l),
  );
  return [
    ...(runsAgain(loop, state, stage.id, round) ? stage.agents.map((agent) => agent.id) : []),
    ...grown.map((l) => l.agent),
  ];
}

/** The agents the last judge grew into `round`; keys are unique across the run, so seen keys are
 * every spec agent and every launch outside this round's grown batch. */
function grownFor(spec: WorkflowSpec, stage: RepeatStage, state: WorkflowSnapshot, round: number) {
  const growth = growthIn(stage, state, round - 1);
  const batch = state.launches.filter((l) => l.round === round && isGrown(stage, l));
  const seen = new Set([
    ...agentStages(spec).flatMap((s) => s.agents.map((agent) => agent.id)),
    ...state.launches.filter((l) => !batch.includes(l)).map((l) => l.agent),
  ]);
  return (growth?.adds ?? []).filter(({ key }) => {
    const fresh = !seen.has(key);
    seen.add(key);
    return fresh;
  });
}

/** Consecutive dry votes ending at this round: rounds that grew nothing and asked for no rerun. */
function dryStreak(
  spec: WorkflowSpec,
  stage: RepeatStage,
  state: WorkflowSnapshot,
  dryNow: boolean,
) {
  const wasDry = (round: number) =>
    grownFor(spec, stage, state, round + 1).length === 0 &&
    !growthIn(stage, state, round)?.rerun.length;
  let streak = dryNow ? 1 : 0;
  while (streak > 0 && streak < state.round && wasDry(state.round - streak)) streak += 1;
  return streak;
}

/** delegate_task's prompt cap, in bytes; a task is measured before it is built. */
const TASK_BYTES = 120_000;

/** Null when the task would render past TASK_BYTES. `visible` limits the stages it may read. */
function renderTask(
  spec: WorkflowSpec,
  state: WorkflowSnapshot,
  template: string,
  input?: Readonly<Record<string, unknown>>,
  visible?: ReadonlySet<string>,
): string | null {
  const resolve = (match: string, stageRef: string, agentRef?: string) => {
    if (stageRef === "feedback") {
      const stage = spec.stages[state.cursor];
      if (stage === undefined || "agents" in stage) return match;
      return JSON.stringify(judgedIn(state, stage, state.round - 1) ?? null);
    }
    if (stageRef === "input" && input !== undefined) {
      return agentRef === undefined ? JSON.stringify(input) : asText(input[agentRef] ?? null);
    }
    if (visible !== undefined && !visible.has(stageRef)) return match;
    if (!agentStages(spec).some((stage) => stage.id === stageRef)) return match;
    const latest = new Map<string, unknown>();
    for (const launch of state.launches) {
      if (launch.stage === stageRef && launch.outcome?.status === "completed") {
        latest.set(launch.agent, valueOf(launch.outcome));
      }
    }
    if (agentRef !== undefined) return asText(latest.get(agentRef) ?? null);
    return JSON.stringify(Object.fromEntries(latest));
  };
  const values = new Map<string, { readonly text: string; readonly bytes: number }>();
  let bytes = Buffer.byteLength(template.replace(REFERENCE, ""));
  for (const [match, stageRef, agentRef] of template.matchAll(REFERENCE)) {
    let value = values.get(match);
    if (value === undefined) {
      const resolved = resolve(match, stageRef!, agentRef);
      value = { text: resolved, bytes: Buffer.byteLength(resolved) };
      values.set(match, value);
    }
    bytes += value.bytes;
    if (bytes > TASK_BYTES) return null;
  }
  return template.replace(REFERENCE, (match) => values.get(match)!.text);
}

/** A judge-authored task reads only the repeat's stages and those its judge's own task read. */
function judgeView(stage: RepeatStage): ReadonlySet<string> {
  const judge = stage.repeat.stages.at(-1)?.agents.find((a) => a.id === stage.repeat.judge);
  return new Set([
    ...stage.repeat.stages.map((s) => s.id),
    ...[...(judge?.task.matchAll(REFERENCE) ?? [])].map(([, stageRef]) => stageRef!),
  ]);
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
  | {
      readonly _tag: "finish";
      readonly status: typeof WorkflowRunStatus.Type;
      readonly error?: string;
    }
  | { readonly _tag: "launch"; readonly stage: string; readonly agent: WorkflowAgent }
  | { readonly _tag: "start" | "record"; readonly launch: WorkflowLaunch }
  | { readonly _tag: "advance"; readonly state: WorkflowSnapshot }
  | { readonly _tag: "wait" };

const finish = (status: typeof WorkflowRunStatus.Type, error?: string): Step => ({
  _tag: "finish",
  status,
  ...(error === undefined ? {} : { error }),
});
const advance = (state: WorkflowSnapshot): Step => ({ _tag: "advance", state });

const jsonBytes = (text: string) => Buffer.byteLength(JSON.stringify(text)) - 2;

/** The error rides the run's last row update, so it is cut to a budget of JSON-encoded bytes. */
const ERROR_BYTES = 200;
const clip = (text: string) => {
  const flat = text.replace(/\s+/g, " ").trim();
  if (jsonBytes(flat) <= ERROR_BYTES) return flat;
  let cut = "";
  let bytes = jsonBytes("…");
  for (const char of flat) {
    bytes += jsonBytes(char);
    if (bytes > ERROR_BYTES) break;
    cut += char;
  }
  return `${cut}…`;
};

/** A launch that ended the run by not completing; the run's own cancels at its end never count. */
const brokenLaunch = (state: WorkflowSnapshot) =>
  state.launches.find(
    (l) => l.outcome !== null && l.outcome.status !== "completed" && !l.cancelRequested,
  );

/** The row a failed run's error belongs to: the agent that failed, else the judge that ended it. */
export function blamedLaunch(spec: WorkflowSpec, state: WorkflowSnapshot) {
  if (state.status !== "failed" || state.error === undefined) return undefined;
  const broken = brokenLaunch(state);
  if (broken !== undefined) return broken;
  const stage = spec.stages[state.cursor];
  if (stage === undefined || "agents" in stage) return undefined;
  return findLaunch(state, stage.repeat.stages.at(-1)?.id ?? "", state.round, stage.repeat.judge);
}

/** The TLA model's runner actions, as a pure choice over the persisted state. */
function decide(spec: WorkflowSpec, state: WorkflowSnapshot, now: number): Step {
  if (state.stop) return finish("cancelled");
  if (now >= state.deadline) return finish("timed_out");
  const broken = brokenLaunch(state);
  if (broken?.outcome) {
    const { status, result } = broken.outcome;
    const reason = result?.trim() ? `${status}: ${result}` : status;
    return finish("failed", clip(`round ${broken.round} ${broken.agent}: ${reason}`));
  }
  const stage = spec.stages[state.cursor];
  if (stage === undefined) return finish("completed");
  const loop = "agents" in stage ? null : stage;
  const phase = "agents" in stage ? stage : stage.repeat.stages[state.phase];
  if (phase === undefined) return finish("failed");
  if (loop !== null && state.round > 1) {
    // One row per update keeps each within the wire budget; all land before the round starts.
    const recorded = state.launches.filter((l) => l.round === state.round && isGrown(loop, l));
    const add = grownFor(spec, loop, state, state.round).find(
      ({ key }) => !recorded.some((l) => l.agent === key),
    );
    if (add !== undefined) {
      return {
        _tag: "record",
        launch: {
          stage: add.stage,
          round: state.round,
          agent: add.key,
          taskId: null,
          cancelRequested: false,
          outcome: null,
        },
      };
    }
  }
  if (loop === null || runsAgain(loop, state, phase.id, state.round)) {
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
  // A missing or malformed judge result fails the run; it never accepts or counts as dry.
  const growth = parseGrowth(loop, state.round, value);
  if (typeof growth === "string") {
    return finish("failed", clip(`round ${state.round} ${repeat.judge}: ${growth}`));
  }
  if (accepts(repeat.until, value)) return advance(exit("accepted"));
  const grown = grownFor(spec, loop, state, state.round + 1);
  const dryNow = grown.length === 0 && growth.rerun.length === 0;
  if (repeat.dry !== undefined && dryStreak(spec, loop, state, dryNow) >= repeat.dry) {
    return advance(exit("dry"));
  }
  if (state.round === repeat.max) {
    return repeat.onExhausted === "pass"
      ? advance(exit("exhausted"))
      : finish("failed", `${loop.id}: not accepted in ${state.round} rounds`);
  }
  return advance({ ...state, round: state.round + 1, phase: 0 });
}

/** A spec agent, or a grown one built from its template, with the task and input its judge gave it. */
function findAgent(spec: WorkflowSpec, state: WorkflowSnapshot, launch: WorkflowLaunch) {
  for (const stage of spec.stages) {
    const phase = ("agents" in stage ? [stage] : stage.repeat.stages).find(
      (s) => s.id === launch.stage,
    );
    if (phase === undefined) continue;
    const agent = phase.agents.find((a) => a.id === launch.agent);
    if (agent !== undefined || "agents" in stage) return agent && { agent };
    const add = growthIn(stage, state, launch.round - 1)?.adds.find((a) => a.key === launch.agent);
    const template = add && stage.repeat.templates?.[add.template];
    if (add === undefined || template === undefined) return undefined;
    return {
      agent: { ...template, id: add.key, task: add.task ?? template.task },
      input: add.input,
      visible: add.task === undefined ? undefined : judgeView(stage),
    };
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
    const task = renderTask(spec, state, agent.task, found.input, found.visible);
    if (task === null) {
      const result = `Its task renders to more than ${TASK_BYTES} bytes.`;
      return yield* update(key, { outcome: { status: "failed", result } });
    }
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
          ...(step.error === undefined ? {} : { error: step.error }),
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
    ...(state.error === undefined ? {} : { error: state.error }),
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
