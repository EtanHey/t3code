import {
  CommandId,
  isOrchestrationV2WorkActive,
  type EnvironmentId,
  type ModelSelection,
  NodeId,
  type OrchestrationV2Run,
  type OrchestrationV2SubagentWorkflow,
  type OrchestrationV2WorkflowAgent,
  type OrchestrationV2WorkflowRunState,
  OrchestratorMcpFailure,
  type OrchestratorMcpDelegateTaskResult,
  type ThreadId,
} from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import { forkParked } from "../serverActivation.ts";
import {
  type McpInvocationScope,
  type McpThreadInvocationScope,
  requireThreadScope,
} from "./McpInvocationContext.ts";
import * as OrchestratorMcpService from "./OrchestratorMcpService.ts";
import {
  agentStages,
  decodeWorkflowSpec,
  initialSnapshot,
  phaseAgents,
  resumeWorkflow,
  WorkflowAgentOutcome,
  type WorkflowPort,
  type WorkflowResult,
  workflowResult,
  type WorkflowSnapshot,
  WorkflowSpec,
  workflowPhases,
} from "./workflowRun.ts";

export class WorkflowRuns extends Context.Service<
  WorkflowRuns,
  {
    /** Records the run on the caller's thread and drives it here; the run's end wakes the caller. */
    readonly run: (
      scope: McpInvocationScope,
      input: unknown,
    ) => Effect.Effect<{ readonly subagentId: NodeId }, OrchestratorMcpFailure>;
    readonly status: (
      scope: McpInvocationScope,
      subagentId: NodeId,
    ) => Effect.Effect<WorkflowResult, OrchestratorMcpFailure>;
    /** Continues every running workflow run, as after a restart. */
    readonly start: () => Effect.Effect<
      void,
      never,
      Scope.Scope | ProjectionStore.ProjectionStoreV2 | ServerEnvironment.ServerEnvironment
    >;
  }
>()("t3/mcp/WorkflowRuns") {}

const Json = Schema.fromJsonString(Schema.Unknown);
const encodeJson = Schema.encodeSync(Json);
const decodeJson = Schema.decodeEffect(Json);
const encodeSpec = Schema.encodeSync(Schema.fromJsonString(WorkflowSpec));

const failure = (code: OrchestratorMcpFailure["code"], message: string) =>
  new OrchestratorMcpFailure({ code, message });

const unavailable = (error: { readonly message: string }) =>
  failure("orchestration_error", error.message);

const isOutcomeStatus = Schema.is(WorkflowAgentOutcome.fields.status);

/** delegate_task keeps `completed` when the reply misses its resultSchema; a workflow fails it. */
const outcomeOf = (result: OrchestratorMcpDelegateTaskResult): WorkflowAgentOutcome =>
  result.status === "completed" && result.invalidOutput !== undefined
    ? { status: "invalid_output", result: result.invalidOutput }
    : {
        status: isOutcomeStatus(result.status) ? result.status : "failed",
        result: result.summary,
        ...(result.structuredResult === undefined
          ? {}
          : { structuredResult: result.structuredResult }),
      };

/** The row layout this build writes. A row without it is an older build's and is never resumed. */
const ROW_VERSION = 2;

const LEGACY_ROW = "This run was recorded by an older T3 build, so it cannot be resumed.";

type AgentTarget = Pick<OrchestrationV2WorkflowAgent, "providerInstanceId" | "model" | "effort">;

/** What a launched agent's child has recorded: where it runs, and when its runs started and ended. */
type ChildFacts = AgentTarget & Pick<OrchestrationV2WorkflowAgent, "startedAt" | "completedAt">;

interface Child {
  readonly threadId: ThreadId;
  readonly facts: ChildFacts;
}

const factsOfAgent = ({
  providerInstanceId,
  model,
  effort,
  startedAt,
  completedAt,
}: OrchestrationV2WorkflowAgent): ChildFacts => ({
  ...(providerInstanceId === undefined ? {} : { providerInstanceId }),
  ...(model === undefined ? {} : { model }),
  ...(effort === undefined ? {} : { effort }),
  ...(startedAt === undefined ? {} : { startedAt }),
  ...(completedAt === undefined ? {} : { completedAt }),
});

/** Read from the child's own run records, never from when the runner noticed a change. */
const factsOfRuns = (runs: ReadonlyArray<OrchestrationV2Run>): ChildFacts => {
  const ordered = runs.toSorted((left, right) => left.ordinal - right.ordinal);
  const first = ordered[0];
  const last = ordered.at(-1);
  if (first === undefined || last === undefined) return {};
  // Until the child's run starts, its request stands in so a live row can show elapsed time.
  return {
    ...targetOf(last.modelSelection),
    startedAt: DateTime.toEpochMillis(first.startedAt ?? first.requestedAt),
    ...(last.completedAt === null ? {} : { completedAt: DateTime.toEpochMillis(last.completedAt) }),
  };
};

const targetOf = (selection: ModelSelection): AgentTarget => {
  const effort =
    getModelSelectionStringOptionValue(selection, "reasoningEffort") ??
    getModelSelectionStringOptionValue(selection, "effort");
  return {
    providerInstanceId: selection.instanceId,
    model: selection.model,
    ...(effort?.trim() ? { effort: effort.trim() } : {}),
  };
};

const targetKey = (stage: string, agent: string) => `${stage}.${agent}`;

const legacyResult = (spec: WorkflowSpec, status: WorkflowResult["status"]): WorkflowResult => ({
  name: spec.name,
  status,
  error: LEGACY_ROW,
  stages: [],
});

/**
 * The row lists every agent of every phase so far, launched or not, in phase order (results stay
 * on the child tasks), and keeps the rest of the snapshot as `workflowRun`.
 */
function toRow(
  spec: WorkflowSpec,
  snapshot: WorkflowSnapshot,
  children: ReadonlyMap<string, Child>,
  targets: ReadonlyMap<string, AgentTarget>,
): {
  readonly workflow: OrchestrationV2SubagentWorkflow;
  readonly workflowRun: OrchestrationV2WorkflowRunState;
} {
  const { launches, ...state } = snapshot;
  const phases = workflowPhases(spec, snapshot);
  const agents = phases.flatMap(({ stage, round }, phaseIndex) =>
    phaseAgents(spec, snapshot, stage, round).map((agent) => ({
      stage: stage.id,
      agent,
      round,
      phaseIndex,
    })),
  );
  return {
    workflow: {
      name: spec.name,
      phases: phases.map(({ title }, index) => ({ index, title })),
      agents: agents.map(({ stage, agent, round, phaseIndex }, index) => {
        const base = { index, label: agent, phaseIndex };
        const planned = targets.get(targetKey(stage, agent));
        const launch = launches.find(
          (l) => l.stage === stage && l.round === round && l.agent === agent,
        );
        if (launch === undefined) return { ...base, ...planned, state: "queued" as const };
        const child = launch.taskId === null ? undefined : children.get(launch.taskId);
        // A launch the run cancels as it ends stays live here; clients show the run's end for it.
        const live = launch.outcome === null || launch.cancelRequested;
        const { queuedAt } = launch;
        const { startedAt, completedAt: endedAt, ...ran } = child?.facts ?? {};
        const completedAt = live ? undefined : endedAt;
        // What the child ran replaces the plan whole: a model without an effort shows none.
        const target = ran.providerInstanceId === undefined ? planned : ran;
        return {
          ...base,
          ...target,
          ...(child === undefined ? {} : { childThreadId: child.threadId }),
          state: live
            ? launch.taskId === null
              ? "queued"
              : "running"
            : launch.outcome?.status === "completed"
              ? "completed"
              : "failed",
          ...(queuedAt === undefined ? {} : { queuedAt }),
          ...(startedAt === undefined ? {} : { startedAt }),
          ...(completedAt === undefined ? {} : { completedAt }),
          ...(startedAt === undefined || completedAt === undefined
            ? {}
            : { durationMs: Math.max(0, completedAt - startedAt) }),
        };
      }),
    },
    workflowRun: { ...state, version: ROW_VERSION },
  };
}

const make = Effect.gen(function* () {
  const mcp = yield* OrchestratorMcpService.OrchestratorMcpService;
  const threads = yield* ThreadManagementService.ThreadManagementService;
  const crypto = yield* Crypto.Crypto;
  const runners = yield* Scope.Scope;
  const driving = new Set<NodeId>();

  const commandId = (kind: string) =>
    crypto.randomUUIDv4().pipe(
      Effect.orDie,
      Effect.map((id) => CommandId.make(`command:workflow-${kind}:${id}`)),
    );

  const readRun = Effect.fn("WorkflowRuns.readRun")(function* (
    environmentId: EnvironmentId,
    threadId: ThreadId,
    taskId: NodeId,
  ) {
    const { subagents } = yield* threads
      .getThreadRecords(threadId, ["subagents"])
      .pipe(Effect.mapError(unavailable));
    const row = subagents.find((task) => task.id === taskId && task.workflowRun !== undefined);
    if (row === undefined) {
      return yield* failure("task_not_found", `${taskId} is not a workflow run on ${threadId}.`);
    }
    const spec = yield* decodeJson(row.prompt).pipe(
      Effect.mapError(() => failure("orchestration_error", "The workflow spec is unreadable.")),
      Effect.flatMap(decodeWorkflowSpec),
    );
    const live = subagents.flatMap((child) =>
      child.parentNodeId === row.id && isOrchestrationV2WorkActive(child.status) ? [child.id] : [],
    );
    const workflowRun = row.workflowRun!;
    const legacy = workflowRun.version !== ROW_VERSION;
    // An older build's agents index another layout; expanding them would misplace every launch.
    const recorded = legacy ? [] : (row.workflow?.agents ?? []);
    const children = new Map(
      subagents.flatMap((child): Array<[string, Child]> => {
        if (child.parentNodeId !== row.id || child.childThreadId === null) return [];
        const agent = recorded.find((agent) => agent.childThreadId === child.childThreadId);
        const facts = agent === undefined ? {} : factsOfAgent(agent);
        return [[child.id, { threadId: child.childThreadId, facts }]];
      }),
    );
    // Children act as the run, so their task ids stay stable across restarts.
    const scope: McpThreadInvocationScope = {
      environmentId,
      requestNamespace: row.id,
      thread: {
        threadId,
        providerSessionId: `workflow:${row.id}`,
        providerInstanceId: row.providerInstanceId,
      },
      client: undefined,
      capabilities: new Set(["orchestration"]),
      issuedAt: 0,
    };
    const phases = workflowPhases(spec, workflowRun);
    const agents = recorded.flatMap((agent) => {
      const phase = phases[agent.phaseIndex ?? 0];
      return phase === undefined ? [] : [{ agent, stage: phase.stage.id, round: phase.round }];
    });
    // A later round's agents are planned with what round 1 ran.
    const targets = new Map<string, AgentTarget>();
    for (const { agent, stage } of agents) {
      const key = targetKey(stage, agent.label);
      if (targets.has(key)) continue;
      const { startedAt: _startedAt, completedAt: _completedAt, ...target } = factsOfAgent(agent);
      targets.set(key, target);
    }
    return { row, spec, workflowRun, legacy, children, live, scope, agents, targets };
  });

  /** Rebuilds the runner's snapshot; finished launches read their outcome from their child task. */
  const snapshotOf = Effect.fn("WorkflowRuns.snapshotOf")(function* (
    run: Effect.Success<ReturnType<typeof readRun>>,
  ) {
    const taskIds = new Map([...run.children].map(([taskId, child]) => [child.threadId, taskId]));
    // A queued agent with no queue time is one the run has not launched yet.
    const launched = run.agents.filter(
      ({ agent }) => agent.state !== "queued" || agent.queuedAt !== undefined,
    );
    const launches = yield* Effect.forEach(launched, ({ agent, stage, round }) =>
      Effect.gen(function* () {
        const taskId =
          agent.childThreadId === undefined ? null : (taskIds.get(agent.childThreadId) ?? null);
        const outcome: WorkflowAgentOutcome | null =
          (agent.state === "queued" || agent.state === "running") &&
          run.row.workflowRun?.status === "running"
            ? null
            : taskId === null
              ? { status: "failed", result: null }
              : outcomeOf(yield* mcp.taskStatus(run.scope, NodeId.make(taskId)));
        return {
          stage,
          round,
          agent: agent.label,
          taskId,
          cancelRequested: false,
          outcome,
          ...(agent.queuedAt === undefined ? {} : { queuedAt: agent.queuedAt }),
        };
      }),
    );
    const { version: _version, ...state } = run.workflowRun;
    return { ...state, launches } satisfies WorkflowSnapshot;
  });

  const drive = Effect.fn("WorkflowRuns.drive")(function* (
    environmentId: EnvironmentId,
    threadId: ThreadId,
    taskId: NodeId,
  ) {
    const run = yield* readRun(environmentId, threadId, taskId);
    const cancelLive = Effect.forEach(
      run.live,
      (child) => Effect.ignore(mcp.cancelTask(run.scope, { taskId: child })),
      { discard: true },
    );
    // A restart can cut off an ended run's cancels; its recorded end and wake stay as they are.
    if (run.workflowRun.status !== "running") return yield* cancelLive;
    const { row, spec, children, scope, targets } = run;
    if (run.legacy) {
      yield* cancelLive;
      return yield* threads
        .dispatch({
          type: "workflow_run.update",
          commandId: yield* commandId("legacy"),
          parentThreadId: threadId,
          taskId: row.id,
          workflowRun: { ...run.workflowRun, status: "failed" },
          result: encodeJson(legacyResult(spec, "failed")),
        })
        .pipe(Effect.asVoid, Effect.mapError(unavailable));
    }
    if (row.runId === null) return;
    const owner = { taskId: row.id, runId: row.runId };
    const learn = (childTaskId: string, threadId: ThreadId) =>
      threads.getThreadRecords(threadId, ["runs"]).pipe(
        Effect.map(({ runs }) => children.set(childTaskId, { threadId, facts: factsOfRuns(runs) })),
        Effect.ignore,
      );
    const port: WorkflowPort = {
      start: ({ key, agent: { id: _id, task: _template, ...options }, task }) =>
        mcp
          .delegateTask(scope, {
            ...options,
            task,
            title: key,
            mode: "async",
            clientRequestId: key,
            workflow: owner,
          })
          .pipe(
            Effect.tap((started) =>
              started.childThreadId === null
                ? Effect.void
                : learn(started.taskId, started.childThreadId),
            ),
            Effect.map((started) => started.taskId),
          ),
      await: (childTaskId) =>
        mcp.awaitTask(scope, NodeId.make(childTaskId)).pipe(
          Effect.tap(() => {
            const child = children.get(childTaskId);
            return child === undefined ? Effect.void : learn(childTaskId, child.threadId);
          }),
          Effect.map(outcomeOf),
        ),
      cancel: (childTaskId) =>
        mcp.cancelTask(scope, { taskId: NodeId.make(childTaskId) }).pipe(Effect.asVoid),
      persist: (snapshot) =>
        // The run's last row is final: read every child's runs once more, so none keeps a stand-in.
        (snapshot.status === "running"
          ? Effect.void
          : Effect.forEach(
              [...children],
              ([childTaskId, child]) => learn(childTaskId, child.threadId),
              { discard: true },
            )
        ).pipe(
          Effect.andThen(commandId("update")),
          Effect.flatMap((id) =>
            threads.dispatch({
              type: "workflow_run.update",
              commandId: id,
              parentThreadId: threadId,
              taskId: row.id,
              ...toRow(spec, snapshot, children, targets),
              ...(snapshot.status === "running"
                ? {}
                : { result: encodeJson(workflowResult(spec, snapshot)) }),
            }),
          ),
          Effect.asVoid,
          Effect.mapError(unavailable),
        ),
    };
    yield* resumeWorkflow(port, spec, yield* snapshotOf(run));
  });

  /** At most one runner per run in this server. */
  const follow = (environmentId: EnvironmentId, threadId: ThreadId, taskId: NodeId) =>
    Effect.suspend(() => {
      if (driving.has(taskId)) return Effect.void;
      driving.add(taskId);
      return drive(environmentId, threadId, taskId).pipe(
        Effect.ignoreCause({ log: true }),
        Effect.ensuring(Effect.sync(() => driving.delete(taskId))),
      );
    });

  const run = Effect.fn("WorkflowRuns.run")(function* (
    callerScope: McpInvocationScope,
    input: unknown,
  ) {
    const scope = yield* requireThreadScope(callerScope, "t3_workflow_run");
    const spec = yield* decodeWorkflowSpec(input);
    const targets = new Map<string, AgentTarget>();
    for (const stage of agentStages(spec)) {
      for (const agent of stage.agents) {
        if (agent.target?.environmentId !== undefined) {
          return yield* failure(
            "invalid_request",
            `Agent "${agent.id}": workflow agents run here. Omit target.environmentId.`,
          );
        }
        const selection = yield* mcp
          .checkTarget(scope, agent.target)
          .pipe(
            Effect.mapError((error) =>
              failure(error.code, `Agent "${agent.id}": ${error.message}`),
            ),
          );
        targets.set(targetKey(stage.id, agent.id), targetOf(selection));
      }
    }
    const parent = yield* threads
      .getThreadRecords(scope.thread.threadId, ["runs", "providerThreads"])
      .pipe(Effect.mapError(unavailable));
    const parentRun = parent.runs
      .filter(ThreadManagementService.isActiveRun)
      .toSorted((left, right) => right.ordinal - left.ordinal)[0];
    const driver = parent.providerThreads.find(
      (thread) => thread.id === parentRun?.providerThreadId,
    )?.driver;
    if (
      parentRun?.rootNodeId == null ||
      driver === undefined ||
      parentRun.providerInstanceId !== scope.thread.providerInstanceId
    ) {
      return yield* failure(
        "parent_not_active",
        "A workflow run needs an active run owned by this MCP provider session.",
      );
    }
    const recorded = yield* threads
      .dispatch({
        type: "workflow_run.record",
        commandId: yield* commandId("run"),
        parentThreadId: scope.thread.threadId,
        parentRunId: parentRun.id,
        parentNodeId: parentRun.rootNodeId,
        task: encodeSpec(spec),
        title: spec.name,
        driver,
        modelSelection: parentRun.modelSelection,
        ...toRow(spec, initialSnapshot(spec, yield* Clock.currentTimeMillis), new Map(), targets),
      })
      .pipe(Effect.mapError(unavailable));
    const row = recorded.storedEvents.flatMap(({ event }) =>
      event.type === "subagent.updated" ? [event.payload] : [],
    )[0];
    if (row === undefined) {
      return yield* failure("orchestration_error", "The workflow run was not recorded.");
    }
    yield* follow(scope.environmentId, scope.thread.threadId, row.id).pipe(Effect.forkIn(runners));
    return { subagentId: row.id };
  });

  const status = Effect.fn("WorkflowRuns.status")(function* (
    callerScope: McpInvocationScope,
    subagentId: NodeId,
  ) {
    const scope = yield* requireThreadScope(callerScope, "t3_workflow_status");
    const found = yield* readRun(scope.environmentId, scope.thread.threadId, subagentId);
    if (found.legacy) return legacyResult(found.spec, found.workflowRun.status);
    return workflowResult(found.spec, yield* snapshotOf(found));
  });

  const start = () =>
    forkParked(
      Effect.gen(function* () {
        const environmentId = yield* (yield* ServerEnvironment.ServerEnvironment).getEnvironmentId;
        const open = yield* (yield* ProjectionStore.ProjectionStoreV2).getOpenWorkflowRuns;
        for (const { parentThreadId, taskId } of open) {
          yield* follow(environmentId, parentThreadId, taskId).pipe(Effect.forkIn(runners));
        }
      }).pipe(Effect.ignoreCause({ log: true })),
    );

  return WorkflowRuns.of({ run, status, start });
});

export const layer = Layer.effect(WorkflowRuns, make);
