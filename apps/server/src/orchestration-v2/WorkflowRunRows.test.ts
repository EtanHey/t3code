import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  type OrchestrationV2WorkflowRunState,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";

import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as Orchestrator from "./Orchestrator.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ServerSettings from "../serverSettings.ts";

// A server-owned workflow run is an app-owned task on its parent. Its children
// start after the parent's turn has ended, and only the run's end wakes the parent.

const instanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const modelSelection = { instanceId, model: "gpt-5.4" };
const adapter = {
  instanceId,
  driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process needed for workflow run records"),
} as ProviderAdapter.ProviderAdapterV2["Service"];

class Offers extends Context.Service<
  Offers,
  Ref.Ref<ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>>
>()("t3/orchestration-v2/WorkflowRunRows.test/Offers") {}
const layerDatabase = SqlitePersistence.layerMemory;
const layerTest = ThreadManagementService.layer.pipe(
  Layer.provideMerge(
    Layer.mergeAll(
      layerDatabase,
      ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
      EventStore.layer.pipe(Layer.provide(layerDatabase)),
      EffectOutbox.layer.pipe(Layer.provide(layerDatabase)),
      ProviderReplayHarness.layerWithRegistry(
        { name: "workflow-run-rows" },
        ProviderAdapterRegistry.layerFromAdapters([adapter]),
        { databaseLayer: layerDatabase, runEffectWorker: false },
      ),
    ),
  ),
  Layer.provideMerge(ServerSettings.layerTest()),
  Layer.provideMerge(CommandReceiptStore.layer.pipe(Layer.provide(layerDatabase))),
  Layer.provide(
    Layer.effect(
      ProviderContinuationRequests.ProviderContinuationRequests,
      Offers.pipe(
        Effect.map((offers) => ({
          offer: (request: ProviderContinuationRequests.ProviderContinuationRequest) =>
            Ref.update(offers, (seen) => [...seen, request]),
          take: Effect.never,
        })),
      ),
    ),
  ),
  Layer.provideMerge(
    Layer.effect(
      Offers,
      Ref.make<ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>>([]),
    ),
  ),
);

const running: OrchestrationV2WorkflowRunState = {
  status: "running",
  cursor: 0,
  round: 1,
  phase: 0,
  deadline: 4_102_444_800_000,
  stop: false,
  exits: [],
};

/** A Codex parent whose run calls t3_workflow_run, then ends its turn. */
const seedParentWithRun = (name: string) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sink = yield* EventSink.EventSinkV2;
    const now = yield* DateTime.now;
    const threadId = ThreadId.make(`thread:${name}`);
    const runId = RunId.make(`run:${name}`);
    const rootNodeId = NodeId.make(`node:${name}:root`);
    const providerThreadId = ProviderThreadId.make(`provider-thread:${name}`);
    yield* orchestrator.dispatch({
      type: "thread.create",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make(`command:create:${name}`),
      threadId,
      projectId: ProjectId.make("project:workflow-run-rows"),
      title: "Parent",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
    });
    const run = (status: "running" | "completed") => ({
      id: EventId.make(`event:seed-run:${name}:${status}`),
      type: "run.updated" as const,
      threadId,
      runId,
      nodeId: rootNodeId,
      providerInstanceId: instanceId,
      occurredAt: now,
      payload: {
        id: runId,
        threadId,
        ordinal: 1,
        providerInstanceId: instanceId,
        modelSelection,
        providerThreadId,
        userMessageId: MessageId.make(`message:seed-user:${name}`),
        rootNodeId,
        activeAttemptId: null,
        status,
        requestedAt: now,
        startedAt: now,
        completedAt: status === "running" ? null : now,
        checkpointId: null,
        contextHandoffId: null,
      },
    });
    yield* sink.write({
      commandId: CommandId.make(`command:seed:${name}`),
      events: [
        {
          id: EventId.make(`event:seed-provider-thread:${name}`),
          type: "provider-thread.updated",
          threadId,
          driver,
          providerInstanceId: instanceId,
          occurredAt: now,
          payload: {
            id: providerThreadId,
            driver,
            providerInstanceId: instanceId,
            providerSessionId: null,
            appThreadId: threadId,
            ownerNodeId: rootNodeId,
            nativeThreadRef: { driver, nativeId: `native:${name}`, strength: "strong" },
            nativeConversationHeadRef: null,
            status: "active",
            firstRunOrdinal: 1,
            lastRunOrdinal: 1,
            handoffIds: [],
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
          },
        },
        {
          id: EventId.make(`event:seed-node:${name}`),
          type: "node.updated",
          threadId,
          runId,
          nodeId: rootNodeId,
          driver,
          providerInstanceId: instanceId,
          occurredAt: now,
          payload: {
            id: rootNodeId,
            threadId,
            runId,
            parentNodeId: null,
            rootNodeId,
            kind: "root_turn",
            status: "running",
            countsForRun: true,
            providerThreadId,
            providerTurnId: null,
            nativeItemRef: null,
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt: now,
            completedAt: null,
          },
        },
        run("running"),
      ],
    });
    const recorded = yield* orchestrator.dispatch({
      type: "workflow_run.record",
      commandId: CommandId.make(`command:workflow:${name}`),
      parentThreadId: threadId,
      parentRunId: runId,
      parentNodeId: rootNodeId,
      task: '{"name":"review","stages":[]}',
      title: "review",
      driver,
      modelSelection,
      workflow: { name: "review", phases: [], agents: [] },
      workflowRun: running,
    });
    const rowEvent = recorded.storedEvents.find((s) => s.event.type === "subagent.updated");
    const workflowTaskId =
      rowEvent?.event.type === "subagent.updated" ? rowEvent.event.payload.id : undefined;
    assert.isDefined(workflowTaskId);
    yield* sink.write({
      commandId: CommandId.make(`command:end-turn:${name}`),
      events: [run("completed")],
    });
    const delegate = (agent: string) =>
      orchestrator.dispatch({
        type: "workflow_run.delegate",
        createdBy: "agent",
        creationSource: "mcp",
        commandId: CommandId.make(`command:workflow:${name}:${agent}`),
        parentThreadId: threadId,
        parentRunId: runId,
        parentNodeId: workflowTaskId!,
        task: `Do ${agent}.`,
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
      });
    const end = (status: OrchestrationV2WorkflowRunState["status"], suffix = status) =>
      orchestrator.dispatch({
        type: "workflow_run.update",
        commandId: CommandId.make(`command:workflow:${name}:end:${suffix}`),
        parentThreadId: threadId,
        taskId: workflowTaskId!,
        workflowRun: { ...running, status },
        result: `{"status":"${status}"}`,
      });
    const read = Effect.gen(function* () {
      // Finalizes children whose runs ended, as the live run listener does.
      yield* orchestrator.recoverDelegatedTasks;
      const projection = yield* orchestrator.getThreadProjection(threadId);
      const row = projection.subagents.find((task) => task.id === workflowTaskId);
      const children = projection.subagents.filter((task) => task.parentNodeId === workflowTaskId);
      return { projection, row: row!, children };
    });
    // Recovery re-offers an open delivery; the continuation sends each message id once.
    const offered = Effect.map(Offers.pipe(Effect.flatMap(Ref.get)), (all) => [
      ...new Set(
        all
          .filter((offer) => offer.threadId === threadId)
          .map((offer) => offer.delegatedCompletion?.messageId),
      ),
    ]);
    return { threadId, workflowTaskId: workflowTaskId!, delegate, end, read, offered };
  });

it.layer(layerTest)("a server-owned workflow run", (it) => {
  it.effect("wakes a parent whose run ended exactly once when it ends, and never for a child", () =>
    Effect.gen(function* () {
      const threadManagement = yield* ThreadManagementService.ThreadManagementService;
      const run = yield* seedParentWithRun("wake");
      yield* run.delegate("a");
      yield* run.delegate("b");
      const started = yield* run.read;
      assert.equal(started.children.length, 2);
      assert.deepEqual(
        started.children.map((child) => child.completionDelivery?.state),
        ["disposed", "disposed"],
      );
      yield* threadManagement.stopDelegatedTasks({
        threadId: run.threadId,
        commandId: CommandId.make("command:stop-children:wake"),
      });
      const settled = yield* run.read;
      assert.isTrue(settled.children.every((child) => child.status !== "running"));
      assert.equal(settled.row.workflowRun?.stop, true);
      assert.lengthOf(yield* run.offered, 0);

      yield* run.end("completed");
      yield* run.end("failed");
      const ended = yield* run.read;
      assert.equal(ended.row.status, "completed");
      assert.equal(ended.row.result, '{"status":"completed"}');
      assert.equal(ended.row.completionDelivery?.state, "claimed");
      assert.lengthOf(yield* run.offered, 1);
    }),
  );

  it.effect("keeps its stop fence against a later runner write and refuses new children", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadManagement = yield* ThreadManagementService.ThreadManagementService;
      const run = yield* seedParentWithRun("stop");
      yield* run.delegate("a");
      yield* threadManagement.stopWorkflow({
        threadId: run.threadId,
        subagentId: run.workflowTaskId,
      });
      const stopped = yield* run.read;
      assert.equal(stopped.row.workflowRun?.stop, true);
      assert.equal(stopped.row.status, "running");
      assert.isTrue(stopped.children.every((child) => child.status !== "running"));

      // A runner that persisted before it saw the stop must not clear the fence.
      yield* orchestrator.dispatch({
        type: "workflow_run.update",
        commandId: CommandId.make("command:workflow:stop:late-persist"),
        parentThreadId: run.threadId,
        taskId: run.workflowTaskId,
        workflowRun: { ...running, cursor: 1 },
      });
      assert.equal((yield* run.read).row.workflowRun?.stop, true);
      const refused = yield* run.delegate("b").pipe(Effect.flip);
      assert.equal(refused._tag, "OrchestratorDispatchError");

      yield* run.end("cancelled");
      const ended = yield* run.read;
      assert.equal(ended.row.status, "cancelled");
      const notRunning = yield* threadManagement
        .stopWorkflow({ threadId: run.threadId, subagentId: run.workflowTaskId })
        .pipe(Effect.flip);
      assert.equal(notRunning.reason, "not-running");
    }),
  );

  it.effect("refuses a child at or after its persisted deadline", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const run = yield* seedParentWithRun("deadline");
      const now = DateTime.toEpochMillis(yield* DateTime.now);
      yield* orchestrator.dispatch({
        type: "workflow_run.update",
        commandId: CommandId.make("command:workflow:deadline:elapsed"),
        parentThreadId: run.threadId,
        taskId: run.workflowTaskId,
        workflowRun: { ...running, deadline: now },
      });
      const refused = yield* run.delegate("a").pipe(Effect.flip);
      assert.equal(refused._tag, "OrchestratorDispatchError");
      assert.lengthOf((yield* run.read).children, 0);
    }),
  );

  it.effect("ends without a wake or an error once its parent thread is archived", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const run = yield* seedParentWithRun("archived");
      yield* orchestrator.dispatch({
        type: "thread.archive",
        commandId: CommandId.make("command:archive:archived"),
        threadId: run.threadId,
      });
      yield* run.end("timed_out");
      const ended = yield* run.read;
      assert.equal(ended.row.status, "failed");
      assert.equal(ended.row.workflowRun?.status, "timed_out");
      assert.equal(ended.row.completionDelivery?.state, "disposed");
      assert.lengthOf(yield* run.offered, 0);
    }),
  );
});
