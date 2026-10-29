import {
  CommandId,
  type EnvironmentId,
  NodeId,
  type OrchestrationV2SubagentWorkflow,
  type OrchestrationV2WorkflowRunState,
  OrchestratorMcpFailure,
  type OrchestratorMcpDelegateTaskResult,
  type ThreadId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";

import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
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
  resumeWorkflow,
  WorkflowAgentOutcome,
  type WorkflowPort,
  type WorkflowResult,
  workflowResult,
  type WorkflowSnapshot,
  WorkflowSpec,
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

const outcomeOf = (result: OrchestratorMcpDelegateTaskResult): WorkflowAgentOutcome => ({
  status: isOutcomeStatus(result.status) ? result.status : "failed",
  result: result.summary,
  ...(result.structuredResult === undefined ? {} : { structuredResult: result.structuredResult }),
  ...(result.invalidOutput === undefined ? {} : { invalidOutput: result.invalidOutput }),
});

/** The row keeps launches as `workflow.agents` (results stay on the child tasks) and the rest as `workflowRun`. */
function toRow(
  spec: WorkflowSpec,
  snapshot: WorkflowSnapshot,
  childThreads: ReadonlyMap<string, ThreadId>,
): {
  readonly workflow: OrchestrationV2SubagentWorkflow;
  readonly workflowRun: OrchestrationV2WorkflowRunState;
} {
  const phases = agentStages(spec).map((stage, index) => ({ index, title: stage.id }));
  const { launches, ...workflowRun } = snapshot;
  return {
    workflow: {
      name: spec.name,
      phases,
      agents: launches.map((launch, index) => {
        const childThreadId = launch.taskId === null ? undefined : childThreads.get(launch.taskId);
        return {
          index,
          label: launch.agent,
          ...(childThreadId === undefined ? {} : { childThreadId }),
          // A launch the run cancels as it ends stays live here; clients show the run's end for it.
          state:
            launch.outcome === null || launch.cancelRequested
              ? launch.taskId === null
                ? "queued"
                : "running"
              : launch.outcome.status === "completed"
                ? "completed"
                : "failed",
          phaseIndex: phases.find((phase) => phase.title === launch.stage)?.index ?? 0,
          attempt: launch.round,
        };
      }),
    },
    workflowRun,
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
    const childThreads = new Map(
      subagents.flatMap((child): Array<[string, ThreadId]> =>
        child.parentNodeId === row.id && child.childThreadId !== null
          ? [[child.id, child.childThreadId]]
          : [],
      ),
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
    return { row, spec, childThreads, scope };
  });

  /** Rebuilds the runner's snapshot; finished launches read their outcome from their child task. */
  const snapshotOf = Effect.fn("WorkflowRuns.snapshotOf")(function* (
    run: Effect.Success<ReturnType<typeof readRun>>,
  ) {
    const stages = agentStages(run.spec);
    const taskIds = new Map([...run.childThreads].map(([taskId, thread]) => [thread, taskId]));
    const launches = yield* Effect.forEach(run.row.workflow?.agents ?? [], (agent) =>
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
          stage: stages[agent.phaseIndex ?? 0]?.id ?? "",
          round: agent.attempt ?? 1,
          agent: agent.label,
          taskId,
          cancelRequested: false,
          outcome,
        };
      }),
    );
    return { ...run.row.workflowRun!, launches } satisfies WorkflowSnapshot;
  });

  const drive = Effect.fn("WorkflowRuns.drive")(function* (
    environmentId: EnvironmentId,
    threadId: ThreadId,
    taskId: NodeId,
  ) {
    const run = yield* readRun(environmentId, threadId, taskId);
    if (run.row.workflowRun?.status !== "running" || run.row.runId === null) return;
    const { row, spec, childThreads, scope } = run;
    const owner = { taskId: row.id, runId: row.runId! };
    const port: WorkflowPort = {
      start: ({ key, agent: { id: _id, task: _template, ...options }, task }) =>
        mcp
          .delegateTask(
            scope,
            { ...options, task, title: key, mode: "async", clientRequestId: key },
            owner,
          )
          .pipe(
            Effect.map((started) => {
              if (started.childThreadId !== null) {
                childThreads.set(started.taskId, started.childThreadId);
              }
              return started.taskId;
            }),
          ),
      await: (childTaskId) =>
        mcp.awaitTask(scope, NodeId.make(childTaskId)).pipe(Effect.map(outcomeOf)),
      cancel: (childTaskId) =>
        mcp.cancelTask(scope, { taskId: NodeId.make(childTaskId) }).pipe(Effect.asVoid),
      persist: (snapshot) =>
        commandId("update").pipe(
          Effect.flatMap((id) =>
            threads.dispatch({
              type: "workflow_run.update",
              commandId: id,
              parentThreadId: threadId,
              taskId: row.id,
              ...toRow(spec, snapshot, childThreads),
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
    const remote = agentStages(spec)
      .flatMap((stage) => stage.agents)
      .find(({ target }) => target?.environmentId !== undefined);
    if (remote !== undefined) {
      return yield* failure(
        "invalid_request",
        `Agent "${remote.id}": workflow agents run here. Omit target.environmentId.`,
      );
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
        ...toRow(spec, initialSnapshot(spec, yield* Clock.currentTimeMillis), new Map()),
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
    return workflowResult(found.spec, yield* snapshotOf(found));
  });

  return WorkflowRuns.of({ run, status });
});

export const layer = Layer.effect(WorkflowRuns, make);
