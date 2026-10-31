import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  EventId,
  isOrchestrationV2WorkActive,
  MessageId,
  NodeId,
  OrchestrationV2DomainEvent,
  type OrchestrationV2Subagent,
  type ModelSelection,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  type ServerProvider,
  ThreadId,
} from "@t3tools/contracts";
import { restoreSubagentBodies } from "@t3tools/shared/orchestrationV2DedupedBodies";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as LegacyV1ThreadImporter from "../orchestration-v2/legacy/LegacyV1ThreadImporter.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import { makeLiveBodyDeduper } from "../orchestration-v2/WireProjection.ts";
import * as ProviderReplayHarness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as RemoteDelegation from "../peer/RemoteDelegation.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProviderRegistryMock from "../provider/testUtils/providerRegistryMock.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import {
  deriveWorkflowGroups,
  projectedSubagentsToRuntime,
} from "../../../../packages/client-runtime/src/state/subagentRuntime.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import type { McpThreadInvocationScope } from "./McpInvocationContext.ts";
import * as OrchestratorMcpService from "./OrchestratorMcpService.ts";
import * as WorkflowRuns from "./WorkflowRuns.ts";

// t3_workflow_run end to end on the real orchestrator: children start under the
// workflow row after the parent's turn, and only the run's end wakes the parent.

const environmentId = EnvironmentId.make("environment:workflow-runs");
const codex = ProviderInstanceId.make("codex");
const claude = ProviderInstanceId.make("claudeAgent");
const codexDriver = ProviderDriverKind.make("codex");
const modelSelection = { instanceId: codex, model: "gpt-5.4" };
const adapter = (instanceId: ProviderInstanceId, driver: ProviderDriverKind) =>
  ({
    instanceId,
    driver,
    getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
    openSession: () => Effect.die("Children here are settled by the test, not a provider"),
  }) as ProviderAdapter.ProviderAdapterV2["Service"];
const provider = (instanceId: ProviderInstanceId, driver: string, model: string) =>
  ({
    instanceId,
    driver: ProviderDriverKind.make(driver),
    enabled: true,
    installed: true,
    version: "test",
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-10-29T00:00:00.000Z",
    models: [
      {
        slug: model,
        name: model,
        isCustom: false,
        capabilities: {
          optionDescriptors: [
            {
              id: "effort",
              label: "Effort",
              type: "select",
              options: [{ id: "low", label: "Low" }],
            },
          ],
        },
      },
    ],
    slashCommands: [],
    skills: [],
  }) satisfies ServerProvider;

class Offers extends Context.Service<
  Offers,
  SubscriptionRef.SubscriptionRef<
    ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
  >
>()("t3/mcp/WorkflowRuns.test/Offers") {}

const layerDatabase = SqlitePersistence.layerMemory;
const layerRegistry = ProviderAdapterRegistry.layerFromAdapters([
  adapter(codex, codexDriver),
  adapter(claude, ProviderDriverKind.make("claudeAgent")),
]);
const layerSettings = ServerSettings.layerTest({
  providerInstances: {
    [codex]: { driver: "codex", enabled: true },
    [claude]: { driver: "claudeAgent", enabled: true },
  },
});
const layerOrchestrator = ProviderReplayHarness.layerWithRegistry(
  { name: "workflow-runs" },
  layerRegistry,
  { databaseLayer: layerDatabase, runEffectWorker: false },
);
const layerThreads = ThreadManagementService.layer.pipe(
  Layer.provideMerge(layerOrchestrator),
  Layer.provide(
    LegacyV1ThreadImporter.layer.pipe(
      Layer.provide(layerOrchestrator),
      Layer.provide(layerDatabase),
    ),
  ),
  Layer.provide(CommandReceiptStore.layer.pipe(Layer.provide(layerDatabase))),
  Layer.provideMerge(layerSettings),
);
const layerTest = OrchestratorMcpService.layer.pipe(
  Layer.provide(Layer.mock(ThreadLaunchService.ThreadLaunchService)({})),
  Layer.provide(Layer.mock(GitVcsDriver.GitVcsDriver)({})),
  Layer.provideMerge(layerThreads),
  Layer.provideMerge(ProjectionStore.layer.pipe(Layer.provide(layerDatabase))),
  Layer.provide(
    Layer.mergeAll(
      layerRegistry,
      ProviderRegistryMock.layer([
        provider(codex, "codex", "gpt-5.4"),
        provider(claude, "claudeAgent", "claude-sonnet-5-5"),
      ]),
      Layer.mock(ProjectService.ProjectService)({}),
      Layer.mock(SecretRequests.SecretRequests)({}),
      Layer.mock(RemoteDelegation.RemoteDelegation)({}),
      Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
    ),
  ),
  Layer.provide(
    Layer.effect(
      ProviderContinuationRequests.ProviderContinuationRequests,
      Offers.pipe(
        Effect.map((offers) => ({
          offer: (request: ProviderContinuationRequests.ProviderContinuationRequest) =>
            SubscriptionRef.update(offers, (seen) => [...seen, request]),
          take: Effect.never,
        })),
      ),
    ),
  ),
  Layer.provideMerge(
    Layer.effect(
      Offers,
      SubscriptionRef.make<ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>>(
        [],
      ),
    ),
  ),
  Layer.provideMerge(
    Layer.mock(ServerEnvironment.ServerEnvironment)({
      getEnvironmentId: Effect.succeed(environmentId),
    }),
  ),
  Layer.provideMerge(NodeServices.layer),
);

const spec = {
  name: "review",
  stages: [
    {
      id: "draft",
      agents: [
        { id: "a", task: "Draft A.", target: { providerInstanceId: codex } },
        { id: "b", task: "Draft B.", target: { providerInstanceId: claude } },
      ],
    },
    {
      id: "judge",
      agents: [
        {
          id: "j",
          task: "Judge {{draft}}.",
          resultSchema: { type: "object", required: ["ok"] },
        },
      ],
    },
  ],
};

/** A repeat between two stages; agent `a` names its model and effort, the rest inherit the parent's. */
const planned = {
  name: "plan",
  stages: [
    {
      id: "draft",
      agents: [
        {
          id: "a",
          task: "Draft A.",
          target: {
            providerInstanceId: claude,
            model: "claude-sonnet-5-5",
            options: [{ id: "effort", value: "low" }],
          },
        },
        { id: "b", task: "Draft B." },
      ],
    },
    {
      id: "loop",
      repeat: {
        stages: [
          { id: "revise", agents: [{ id: "writer", task: "Revise {{draft}} {{feedback}}." }] },
          {
            id: "review",
            agents: [
              {
                id: "judge",
                task: "Judge {{revise}}.",
                resultSchema: { type: "object", required: ["ok"] },
              },
            ],
          },
        ],
        judge: "judge",
        until: { path: "ok", equals: true },
        max: 2,
      },
    },
    { id: "ship", agents: [{ id: "p", task: "Ship {{revise}}." }] },
  ],
};

/** A Codex parent whose turn is live when it calls t3_workflow_run. */
const seedParent = (name: string, selection: ModelSelection = modelSelection) =>
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
      projectId: ProjectId.make("project:workflow-runs"),
      title: "Parent",
      modelSelection: selection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
    });
    const run = (status: "running" | "completed") => ({
      id: EventId.make(`event:run:${name}:${status}`),
      type: "run.updated" as const,
      threadId,
      runId,
      nodeId: rootNodeId,
      providerInstanceId: codex,
      occurredAt: now,
      payload: {
        id: runId,
        threadId,
        ordinal: 1,
        providerInstanceId: codex,
        modelSelection: selection,
        providerThreadId,
        userMessageId: MessageId.make(`message:${name}`),
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
          id: EventId.make(`event:provider-thread:${name}`),
          type: "provider-thread.updated",
          threadId,
          driver: codexDriver,
          providerInstanceId: codex,
          occurredAt: now,
          payload: {
            id: providerThreadId,
            driver: codexDriver,
            providerInstanceId: codex,
            providerSessionId: null,
            appThreadId: threadId,
            ownerNodeId: rootNodeId,
            nativeThreadRef: { driver: codexDriver, nativeId: name, strength: "strong" },
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
          id: EventId.make(`event:node:${name}`),
          type: "node.updated",
          threadId,
          runId,
          nodeId: rootNodeId,
          driver: codexDriver,
          providerInstanceId: codex,
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
    const scope: McpThreadInvocationScope = {
      environmentId,
      requestNamespace: `provider-session:${name}`,
      thread: {
        threadId,
        providerSessionId: `provider-session:${name}`,
        providerInstanceId: codex,
      },
      client: undefined,
      capabilities: new Set(["orchestration"]),
      issuedAt: 1,
    };
    const endTurn = sink.write({
      commandId: CommandId.make(`command:end-turn:${name}`),
      events: [run("completed")],
    });
    return { threadId, scope, endTurn };
  });

/** Waits on persisted parent events, never on time. */
const parentEvents = (threadId: ThreadId) =>
  Effect.map(Orchestrator.OrchestratorV2, (orchestrator) =>
    orchestrator
      .streamStoredEventsFrom({ threadId, afterSequence: 0 })
      .pipe(
        Stream.flatMap(({ event }) =>
          event.type === "subagent.updated" ? Stream.make(event) : Stream.empty,
        ),
      ),
  );

const childrenOf = (threadId: ThreadId, workflowTaskId: NodeId, count: number) =>
  Effect.gen(function* () {
    const events = yield* parentEvents(threadId);
    const seen = new Map<NodeId, OrchestrationV2Subagent>();
    yield* events.pipe(
      Stream.takeUntil((event) => {
        if (event.payload.parentNodeId === workflowTaskId)
          seen.set(event.payload.id, event.payload);
        return seen.size >= count;
      }),
      Stream.runDrain,
    );
    return [...seen.values()];
  });

const ended = (threadId: ThreadId, workflowTaskId: NodeId) =>
  Effect.gen(function* () {
    const events = yield* parentEvents(threadId);
    return yield* events.pipe(
      Stream.filter(
        (event) => event.payload.id === workflowTaskId && event.payload.status !== "running",
      ),
      Stream.runHead,
      Effect.map((event) => Option.getOrNull(event)?.payload),
    );
  });

/** The first workflow row update that matches, in order. */
const rowWhere = (
  threadId: ThreadId,
  workflowTaskId: NodeId,
  matches: (row: OrchestrationV2Subagent) => boolean,
) =>
  Effect.gen(function* () {
    const events = yield* parentEvents(threadId);
    return yield* events.pipe(
      Stream.filter((event) => event.payload.id === workflowTaskId && matches(event.payload)),
      Stream.runHead,
      Effect.map((event) => Option.getOrThrow(event).payload),
    );
  });

const agentsRunning = (row: OrchestrationV2Subagent) =>
  row.workflow?.agents.filter((agent) => agent.state === "running").length ?? 0;

/** Settles a child as its provider would: a final reply, then the run ends. */
const settleChild = (child: OrchestrationV2Subagent, text: string) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sink = yield* EventSink.EventSinkV2;
    const now = yield* DateTime.now;
    const childThreadId = child.childThreadId!;
    const childRun = (yield* orchestrator.getThreadProjection(childThreadId)).runs[0]!;
    const messageId = MessageId.make(`message:reply:${child.id}`);
    yield* sink.write({
      commandId: CommandId.make(`command:settle:${child.id}`),
      events: [
        {
          id: EventId.make(`event:reply:${child.id}`),
          type: "message.updated",
          threadId: childThreadId,
          runId: childRun.id,
          providerInstanceId: childRun.providerInstanceId,
          occurredAt: now,
          payload: {
            createdBy: "agent",
            creationSource: "provider",
            id: messageId,
            threadId: childThreadId,
            runId: childRun.id,
            nodeId: null,
            role: "assistant",
            text,
            attachments: [],
            streaming: false,
            createdAt: now,
            updatedAt: now,
          },
        },
        {
          id: EventId.make(`event:child-run:${child.id}`),
          type: "run.updated",
          threadId: childThreadId,
          runId: childRun.id,
          providerInstanceId: childRun.providerInstanceId,
          occurredAt: now,
          payload: {
            ...childRun,
            status: "completed",
            startedAt: childRun.startedAt ?? now,
            completedAt: now,
          },
        },
      ],
    });
  });

/** Starts a child's run as its provider would. */
const startChild = (child: OrchestrationV2Subagent) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sink = yield* EventSink.EventSinkV2;
    const now = yield* DateTime.now;
    const childThreadId = child.childThreadId!;
    const childRun = (yield* orchestrator.getThreadProjection(childThreadId)).runs[0]!;
    yield* sink.write({
      commandId: CommandId.make(`command:start:${child.id}`),
      events: [
        {
          id: EventId.make(`event:child-run-start:${child.id}`),
          type: "run.updated",
          threadId: childThreadId,
          runId: childRun.id,
          providerInstanceId: childRun.providerInstanceId,
          occurredAt: now,
          payload: { ...childRun, status: "running", startedAt: now },
        },
      ],
    });
  });

/** The distinct wakes offered to a parent, once it has had one; the offer follows the row's commit. */
const wakes = (threadId: ThreadId) =>
  Offers.pipe(
    Effect.flatMap((offers) =>
      SubscriptionRef.changes(offers).pipe(
        Stream.map((all) => all.filter((offer) => offer.threadId === threadId)),
        Stream.filter((offered) => offered.length > 0),
        Stream.runHead,
      ),
    ),
    Effect.map((offered) => [
      ...new Set(Option.getOrThrow(offered).map((offer) => offer.delegatedCompletion?.messageId)),
    ]),
  );

/** A WorkflowRuns instance whose runners die with `scope`, as in a server restart. */
const workflowRunsIn = (scope: Scope.Scope) =>
  Layer.buildWithScope(WorkflowRuns.layer, scope).pipe(
    Effect.map((context) => Context.get(context, WorkflowRuns.WorkflowRuns)),
  );

const sizeOf = Schema.encodeSync(Schema.fromJsonString(OrchestrationV2DomainEvent));

/** The largest row update a client accepting deduped bodies receives, and its restored agents. */
const largestUpdate = (threadId: ThreadId, subagentId: NodeId) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const stored = yield* orchestrator.streamStoredEventsFrom({ threadId, afterSequence: 0 }).pipe(
      Stream.takeUntil(
        ({ event }) =>
          event.type === "subagent.updated" &&
          event.payload.id === subagentId &&
          event.payload.status !== "running",
      ),
      Stream.runCollect,
    );
    const toWire = makeLiveBodyDeduper({ workflowAgentDeltas: true });
    let restored: OrchestrationV2Subagent | undefined;
    let largest = 0;
    for (const { sequence, event } of stored) {
      const item = toWire({ kind: "event", sequence, event });
      if (item.kind !== "event" || item.event.type !== "subagent.updated") continue;
      if (item.event.payload.id !== subagentId || event.type !== "subagent.updated") continue;
      // The record carries the spec once; updates are budgeted, without agent results.
      if (restored !== undefined) {
        const payload = { ...item.event.payload, result: null };
        largest = Math.max(largest, Buffer.byteLength(sizeOf({ ...item.event, payload })));
      }
      restored = restoreSubagentBodies(item.event.payload, restored);
      assert.deepEqual(restored.workflow, event.payload.workflow);
    }
    return { largest, agents: restored?.workflow?.agents ?? [] };
  });

it.layer(layerTest)("t3_workflow_run", (it) => {
  it.effect("runs mixed-provider stages after its parent's turn ends and wakes it once", () =>
    Effect.gen(function* () {
      const runs = yield* workflowRunsIn(yield* Effect.scope);
      const parent = yield* seedParent("full");
      const { subagentId } = yield* runs.run(parent.scope, spec);
      yield* parent.endTurn;

      const drafts = yield* childrenOf(parent.threadId, subagentId, 2);
      assert.deepEqual(drafts.map((child) => child.providerInstanceId).toSorted(), [claude, codex]);
      assert.isTrue(drafts.every((child) => child.completionDelivery?.state === "disposed"));
      for (const child of drafts) yield* settleChild(child, `Draft by ${child.driver}.`);
      const [judge] = (yield* childrenOf(parent.threadId, subagentId, 3)).filter(
        (child) => !drafts.some((draft) => draft.id === child.id),
      );
      assert.include(judge!.prompt, "Draft by codex.");
      yield* settleChild(judge!, 'Fine.\n```json\n{"ok":true}\n```');

      const row = yield* ended(parent.threadId, subagentId);
      assert.equal(row?.status, "completed");
      assert.equal(row?.workflowRun?.status, "completed");
      assert.include(row?.result ?? "", '"structuredResult":{"ok":true}');
      assert.lengthOf(yield* wakes(parent.threadId), 1);
      const status = yield* runs.status(parent.scope, subagentId);
      assert.equal(status.status, "completed");
    }),
  );

  it.effect(
    "lists every phase and agent, with its model and effort, in the run's first update",
    () =>
      Effect.gen(function* () {
        const runs = yield* workflowRunsIn(yield* Effect.scope);
        const parent = yield* seedParent("plan");
        const { subagentId } = yield* runs.run(parent.scope, planned);

        const first = yield* rowWhere(parent.threadId, subagentId, () => true);
        assert.deepEqual(
          first.workflow?.phases.map((phase) => phase.title),
          ["draft", "revise · round 1", "review · round 1", "ship"],
        );
        assert.deepEqual(
          first.workflow?.agents.map(({ index, label, phaseIndex, state, ...target }) => ({
            index,
            label,
            phaseIndex,
            state,
            target,
          })),
          [
            {
              index: 0,
              label: "a",
              phaseIndex: 0,
              state: "queued",
              target: { providerInstanceId: claude, model: "claude-sonnet-5-5", effort: "low" },
            },
            {
              index: 1,
              label: "b",
              phaseIndex: 0,
              state: "queued",
              target: { providerInstanceId: codex, model: "gpt-5.4" },
            },
            {
              index: 2,
              label: "writer",
              phaseIndex: 1,
              state: "queued",
              target: { providerInstanceId: codex, model: "gpt-5.4" },
            },
            {
              index: 3,
              label: "judge",
              phaseIndex: 2,
              state: "queued",
              target: { providerInstanceId: codex, model: "gpt-5.4" },
            },
            {
              index: 4,
              label: "p",
              phaseIndex: 3,
              state: "queued",
              target: { providerInstanceId: codex, model: "gpt-5.4" },
            },
          ],
        );
      }),
  );

  it.effect("times each agent and adds a later round before the stages after it", () =>
    Effect.gen(function* () {
      const runs = yield* workflowRunsIn(yield* Effect.scope);
      const parent = yield* seedParent("timed");
      const { subagentId } = yield* runs.run(parent.scope, planned);
      yield* parent.endTurn;
      const settled = new Set<NodeId>();
      /** Once the runner delegates the next agents, their runs start 500 ms later and take `ms`. */
      const settleNext = (count: number, ms: number, text: string) =>
        Effect.gen(function* () {
          const children = (yield* childrenOf(
            parent.threadId,
            subagentId,
            settled.size + count,
          )).filter((child) => !settled.has(child.id));
          yield* rowWhere(
            parent.threadId,
            subagentId,
            (row) =>
              row.workflow?.agents.filter(
                (agent) =>
                  agent.state === "running" &&
                  children.some((child) => child.childThreadId === agent.childThreadId),
              ).length === children.length,
          );
          yield* TestClock.adjust(500);
          for (const child of children) yield* startChild(child);
          yield* TestClock.adjust(ms);
          for (const child of children) {
            settled.add(child.id);
            yield* settleChild(child, text);
          }
        });
      yield* settleNext(2, 3_000, "Draft.");
      yield* settleNext(1, 2_000, "Revised.");
      yield* settleNext(1, 1_000, '```json\n{"ok":false}\n```');
      const second = yield* rowWhere(parent.threadId, subagentId, (row) =>
        (row.workflow?.phases ?? []).some((phase) => phase.title === "revise · round 2"),
      );
      assert.deepEqual(
        second.workflow?.agents.map((agent) => [agent.label, agent.phaseIndex, agent.state]),
        [
          ["a", 0, "completed"],
          ["b", 0, "completed"],
          ["writer", 1, "completed"],
          ["judge", 2, "completed"],
          ["writer", 3, "queued"],
          ["judge", 4, "queued"],
          ["p", 5, "queued"],
        ],
      );
      yield* settleNext(1, 1_000, "Revised again.");
      yield* settleNext(1, 1_000, '```json\n{"ok":true}\n```');
      yield* settleNext(1, 4_000, "Shipped.");

      const row = yield* ended(parent.threadId, subagentId);
      assert.equal(row?.status, "completed");
      assert.deepEqual(
        row?.workflow?.phases.map((phase) => phase.title),
        [
          "draft",
          "revise · round 1",
          "review · round 1",
          "revise · round 2",
          "review · round 2",
          "ship",
        ],
      );
      assert.deepEqual(
        row?.workflow?.agents.map((agent) => [
          agent.label,
          agent.state,
          agent.queuedAt,
          agent.startedAt,
          agent.completedAt,
          agent.durationMs,
        ]),
        [
          ["a", "completed", 0, 500, 3_500, 3_000],
          ["b", "completed", 0, 500, 3_500, 3_000],
          ["writer", "completed", 3_500, 4_000, 6_000, 2_000],
          ["judge", "completed", 6_000, 6_500, 7_500, 1_000],
          ["writer", "completed", 7_500, 8_000, 9_000, 1_000],
          ["judge", "completed", 9_000, 9_500, 10_500, 1_000],
          ["p", "completed", 10_500, 11_000, 15_000, 4_000],
        ],
      );
    }),
  );

  it.effect(
    "times an agent from its child's run, even one that ended while the server was down",
    () =>
      Effect.gen(function* () {
        const parent = yield* seedParent("downtime");
        const before = yield* Scope.make();
        const { subagentId } = yield* (yield* workflowRunsIn(before)).run(parent.scope, {
          name: "one",
          stages: [{ id: "one", agents: [{ id: "a", task: "A." }] }],
        });
        yield* parent.endTurn;
        const [child] = yield* childrenOf(parent.threadId, subagentId, 1);
        yield* rowWhere(parent.threadId, subagentId, (row) => agentsRunning(row) === 1);
        const startedAt = yield* Clock.currentTimeMillis;
        yield* startChild(child!);
        yield* TestClock.adjust(1_000);
        yield* Scope.close(before, Exit.void);
        yield* settleChild(child!, "Done.");
        yield* TestClock.adjust(9_000);

        yield* (yield* workflowRunsIn(yield* Effect.scope)).start();
        const [agent] = (yield* ended(parent.threadId, subagentId))?.workflow?.agents ?? [];
        assert.deepEqual(
          [agent?.state, agent?.startedAt, agent?.completedAt, agent?.durationMs],
          ["completed", startedAt, startedAt + 1_000, 1_000],
        );
      }),
  );

  it.effect("shows the model a child actually runs once it starts, not the planned one", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const runs = yield* workflowRunsIn(yield* Effect.scope);
      const parent = yield* seedParent("actual-model");
      const { subagentId } = yield* runs.run(parent.scope, {
        name: "two",
        stages: [
          { id: "one", agents: [{ id: "a", task: "A." }] },
          { id: "two", agents: [{ id: "b", task: "B." }] },
        ],
      });
      yield* parent.endTurn;
      const [first] = yield* childrenOf(parent.threadId, subagentId, 1);
      yield* orchestrator.dispatch({
        type: "thread.model-selection.set",
        commandId: CommandId.make("command:actual-model:switch"),
        threadId: parent.threadId,
        modelSelection: {
          instanceId: claude,
          model: "claude-sonnet-5-5",
          options: [{ id: "effort", value: "low" }],
        },
      });
      yield* settleChild(first!, "Done.");
      const row = yield* rowWhere(
        parent.threadId,
        subagentId,
        (row) => row.workflow?.agents[1]?.state === "running",
      );
      const { providerInstanceId, model, effort } = row.workflow!.agents[1]!;
      assert.deepEqual(
        { providerInstanceId, model, effort },
        { providerInstanceId: claude, model: "claude-sonnet-5-5", effort: "low" },
      );
    }),
  );

  it.effect("drops a planned effort once the child runs a model without one", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const runs = yield* workflowRunsIn(yield* Effect.scope);
      const parent = yield* seedParent("dropped-effort", {
        ...modelSelection,
        options: [{ id: "effort", value: "low" }],
      });
      const { subagentId } = yield* runs.run(parent.scope, {
        name: "two",
        stages: [
          { id: "one", agents: [{ id: "a", task: "A." }] },
          { id: "two", agents: [{ id: "b", task: "B." }] },
        ],
      });
      const planned = yield* rowWhere(parent.threadId, subagentId, () => true);
      assert.equal(planned.workflow?.agents[1]?.effort, "low");
      yield* parent.endTurn;
      const [first] = yield* childrenOf(parent.threadId, subagentId, 1);
      yield* orchestrator.dispatch({
        type: "thread.model-selection.set",
        commandId: CommandId.make("command:dropped-effort:switch"),
        threadId: parent.threadId,
        modelSelection: { instanceId: claude, model: "claude-sonnet-5-5" },
      });
      yield* settleChild(first!, "Done.");
      const row = yield* rowWhere(
        parent.threadId,
        subagentId,
        (row) => row.workflow?.agents[1]?.state === "running",
      );
      const { providerInstanceId, model, effort } = row.workflow!.agents[1]!;
      assert.deepEqual(
        { providerInstanceId, model, effort },
        { providerInstanceId: claude, model: "claude-sonnet-5-5", effort: undefined },
      );
    }),
  );

  it.effect("keeps a child's real start when the run times out after it started", () =>
    Effect.gen(function* () {
      const runs = yield* workflowRunsIn(yield* Effect.scope);
      const parent = yield* seedParent("timeout-start");
      const { subagentId } = yield* runs.run(parent.scope, {
        name: "slow",
        timeoutMs: 5_000,
        stages: [{ id: "one", agents: [{ id: "a", task: "A." }] }],
      });
      yield* parent.endTurn;
      const [child] = yield* childrenOf(parent.threadId, subagentId, 1);
      yield* rowWhere(parent.threadId, subagentId, (row) => agentsRunning(row) === 1);
      const requestedAt = yield* Clock.currentTimeMillis;
      yield* TestClock.adjust(500);
      yield* startChild(child!);
      yield* TestClock.adjust(5_000);

      const row = yield* ended(parent.threadId, subagentId);
      assert.equal(row?.workflowRun?.status, "timed_out");
      assert.equal(row?.workflow?.agents[0]?.startedAt, requestedAt + 500);
    }),
  );

  it.effect("fails a running row an older build recorded instead of resuming it", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const parent = yield* seedParent("legacy");
      const before = yield* Scope.make();
      const { subagentId } = yield* (yield* workflowRunsIn(before)).run(parent.scope, spec);
      yield* parent.endTurn;
      const drafts = yield* childrenOf(parent.threadId, subagentId, 2);
      yield* Scope.close(before, Exit.void);
      const row = (yield* orchestrator.getThreadProjection(parent.threadId)).subagents.find(
        (task) => task.id === subagentId,
      )!;
      const { version: _version, ...legacy } = row.workflowRun!;
      const oldLayout = {
        name: "review",
        phases: [
          { index: 0, title: "draft" },
          { index: 1, title: "judge" },
        ],
        agents: drafts.map((draft, index) => ({
          index,
          label: draft.title!.split(".")[1]!.split("#")[0]!,
          childThreadId: draft.childThreadId!,
          state: "running" as const,
          phaseIndex: 0,
          attempt: 1,
        })),
      };
      yield* orchestrator.dispatch({
        type: "workflow_run.update",
        commandId: CommandId.make("command:legacy:old-layout"),
        parentThreadId: parent.threadId,
        taskId: subagentId,
        workflow: oldLayout,
        workflowRun: legacy,
      });

      yield* (yield* workflowRunsIn(yield* Effect.scope)).start();
      const failed = yield* ended(parent.threadId, subagentId);
      assert.equal(failed?.status, "failed");
      assert.deepEqual(failed?.workflow, oldLayout);
      assert.include(JSON.parse(failed?.result ?? "{}").error, "older T3 build");
      const children = (yield* orchestrator.getThreadProjection(parent.threadId)).subagents.filter(
        (task) => task.parentNodeId === subagentId,
      );
      assert.lengthOf(children, 2);
      assert.isFalse(children.some((task) => isOrchestrationV2WorkActive(task.status)));
      assert.lengthOf(yield* wakes(parent.threadId), 1);
    }),
  );

  it.effect(
    "keeps every workflow row update within 2 KB on the wire across a 20-agent stage and 3 rounds",
    () =>
      Effect.gen(function* () {
        // Real epoch times, so each timestamp costs its full width on the wire.
        yield* TestClock.setTime(1_793_404_800_000);
        const runs = yield* workflowRunsIn(yield* Effect.scope);
        const parent = yield* seedParent("bytes");
        const wide = {
          name: "wide",
          stages: [
            {
              id: "fan",
              agents: Array.from({ length: 20 }, (_, index) => ({
                id: `agent-${index + 1}`,
                task: `Part ${index + 1}.`,
              })),
            },
            {
              id: "loop",
              repeat: {
                stages: [
                  { id: "revise", agents: [{ id: "writer", task: "Revise {{feedback}}." }] },
                  {
                    id: "review",
                    agents: [
                      {
                        id: "judge",
                        task: "Judge {{revise}}.",
                        resultSchema: { type: "object", required: ["ok"] },
                      },
                    ],
                  },
                ],
                judge: "judge",
                until: { path: "ok", equals: true },
                max: 3,
              },
            },
          ],
        };
        const { subagentId } = yield* runs.run(parent.scope, wide);
        yield* parent.endTurn;
        const settled = new Set<NodeId>();
        const settleNext = (count: number, text: string) =>
          Effect.gen(function* () {
            const children = yield* childrenOf(parent.threadId, subagentId, settled.size + count);
            for (const child of children.filter((c) => !settled.has(c.id))) {
              settled.add(child.id);
              yield* settleChild(child, text);
            }
          });
        yield* settleNext(20, "Part done.");
        for (const ok of [false, false, true]) {
          yield* settleNext(1, "Revised.");
          yield* settleNext(1, `\`\`\`json\n{"ok":${ok}}\n\`\`\``);
        }
        assert.equal((yield* ended(parent.threadId, subagentId))?.status, "completed");

        const accepted = yield* largestUpdate(parent.threadId, subagentId);
        assert.lengthOf(accepted.agents, 26);
        assert.isAtMost(accepted.largest, 2048);

        // A stop mid-fanout ends all 20 at once.
        const threadManagement = yield* ThreadManagementService.ThreadManagementService;
        const other = yield* seedParent("bytes-stop");
        const stopped = yield* runs.run(other.scope, { name: "wide", stages: [wide.stages[0]] });
        yield* childrenOf(other.threadId, stopped.subagentId, 20);
        yield* threadManagement.stopWorkflow({
          threadId: other.threadId,
          subagentId: stopped.subagentId,
        });
        assert.equal((yield* ended(other.threadId, stopped.subagentId))?.status, "cancelled");
        const cancelled = yield* largestUpdate(other.threadId, stopped.subagentId);
        assert.isAtMost(cancelled.largest, 2048);

        // A judge naming a huge template fails the run with the longest error, in each encoding.
        for (const char of ["X", '"', "界"]) {
          const failing = yield* seedParent(`bytes-error-${char.codePointAt(0)}`);
          const failed = yield* runs.run(failing.scope, wide);
          yield* failing.endTurn;
          settled.clear();
          const settleFailing = (count: number, text: string) =>
            Effect.gen(function* () {
              const children = yield* childrenOf(
                failing.threadId,
                failed.subagentId,
                settled.size + count,
              );
              for (const child of children.filter((c) => !settled.has(c.id))) {
                settled.add(child.id);
                yield* settleChild(child, text);
              }
            });
          yield* settleFailing(20, "Part done.");
          yield* settleFailing(1, "Revised.");
          const judged = { ok: false, addAgents: [{ template: char.repeat(1_000) }] };
          yield* settleFailing(1, `\`\`\`json\n${JSON.stringify(judged)}\n\`\`\``);
          assert.equal((yield* ended(failing.threadId, failed.subagentId))?.status, "failed");
          const errored = yield* largestUpdate(failing.threadId, failed.subagentId);
          assert.isAtMost(errored.largest, 2048, `template of ${char}`);
        }
      }),
  );

  it.effect(
    "puts a judge's grown agents in their own round's phase, which reads as not started until they launch",
    () =>
      Effect.gen(function* () {
        const runs = yield* workflowRunsIn(yield* Effect.scope);
        const parent = yield* seedParent("grow-phase");
        const { subagentId } = yield* runs.run(parent.scope, {
          name: "grow-phase",
          stages: [
            {
              id: "loop",
              repeat: {
                stages: [
                  { id: "draft", agents: [{ id: "poet", task: "Write." }] },
                  {
                    id: "critique",
                    agents: [
                      {
                        id: "critic",
                        task: "Judge {{draft}}.",
                        resultSchema: { type: "object", required: ["ok"] },
                      },
                    ],
                  },
                ],
                judge: "critic",
                until: { path: "ok", equals: true },
                max: 2,
                templates: { probe: { task: "Probe {{input.angle}}." } },
              },
            },
          ],
        });
        yield* parent.endTurn;
        const [poet] = yield* childrenOf(parent.threadId, subagentId, 1);
        yield* settleChild(poet!, "Draft.");
        const critic = (yield* childrenOf(parent.threadId, subagentId, 2)).find(
          (child) => child.id !== poet!.id,
        )!;
        const addAgents = ["probe-1", "probe-2"].map((key) => ({
          key,
          template: "probe",
          input: { angle: key },
        }));
        yield* settleChild(
          critic,
          `\`\`\`json\n${JSON.stringify({ ok: false, addAgents, rerun: ["critique"] })}\n\`\`\``,
        );

        // The row as it stands once both grown agents are recorded, before either launches.
        const row = yield* (yield* parentEvents(parent.threadId)).pipe(
          Stream.filter(({ payload }) =>
            (payload.workflow?.agents ?? []).some((agent) => agent.label === "probe-2"),
          ),
          Stream.runHead,
          Effect.map((event) => Option.getOrThrow(event).payload),
        );
        const titles = row.workflow!.phases.map((phase) => phase.title);
        assert.deepEqual(titles, [
          "draft · round 1",
          "critique · round 1",
          "draft · round 2",
          "critique · round 2",
        ]);
        const grown = row.workflow!.agents.filter((agent) => agent.label.startsWith("probe-"));
        assert.deepEqual(
          grown.map((agent) => [agent.label, titles[agent.phaseIndex!], agent.state]),
          [
            ["probe-1", "draft · round 2", "queued"],
            ["probe-2", "draft · round 2", "queued"],
          ],
        );
        const phaseStates = (subagent: OrchestrationV2Subagent) =>
          deriveWorkflowGroups(projectedSubagentsToRuntime([subagent]))[0]!.phases.map((phase) => [
            phase.title,
            phase.state,
          ]);
        assert.deepEqual(phaseStates(row), [
          ["draft · round 1", "done"],
          ["critique · round 1", "done"],
          ["draft · round 2", "pending"],
          ["critique · round 2", "pending"],
        ]);

        const probes = (yield* childrenOf(parent.threadId, subagentId, 4)).filter((child) =>
          child.title?.startsWith("draft.probe-"),
        );
        assert.deepEqual(probes.map((child) => child.title).toSorted(), [
          "draft.probe-1#2",
          "draft.probe-2#2",
        ]);
        for (const child of probes) yield* settleChild(child, "Probed.");
        const last = (yield* childrenOf(parent.threadId, subagentId, 5)).find(
          (child) => child.title === "critique.critic#2",
        )!;
        yield* settleChild(last, '```json\n{"ok":true}\n```');
        assert.equal((yield* ended(parent.threadId, subagentId))?.status, "completed");
      }),
  );

  it.effect(
    "shows a judge's grown agents queued under their phase and round, within 2 KB per update",
    () =>
      Effect.gen(function* () {
        const runs = yield* workflowRunsIn(yield* Effect.scope);
        const parent = yield* seedParent("grow");
        const growing = {
          name: "grow",
          stages: [
            {
              id: "loop",
              repeat: {
                stages: [
                  { id: "draft", agents: [{ id: "poet", task: "Write." }] },
                  {
                    id: "critique",
                    agents: [
                      {
                        id: "critic",
                        task: "Judge {{draft}}.",
                        resultSchema: { type: "object", required: ["ok"] },
                      },
                    ],
                  },
                ],
                judge: "critic",
                until: { path: "ok", equals: true },
                max: 2,
                templates: { probe: { task: "Probe {{input.angle}}." } },
              },
            },
          ],
        };
        const { subagentId } = yield* runs.run(parent.scope, growing);
        yield* parent.endTurn;
        const settled = new Set<NodeId>();
        const settleNext = (count: number, text: string) =>
          Effect.gen(function* () {
            const children = yield* childrenOf(parent.threadId, subagentId, settled.size + count);
            for (const child of children.filter((c) => !settled.has(c.id))) {
              settled.add(child.id);
              yield* settleChild(child, text);
            }
            return children;
          });
        const addAgents = Array.from({ length: 17 }, (_, index) => ({
          key: `probe-${index + 1}`,
          template: "probe",
          input: { angle: `angle ${index + 1}` },
        }));
        yield* settleNext(1, "Draft.");
        yield* settleNext(
          1,
          `\`\`\`json\n${JSON.stringify({ ok: false, addAgents, rerun: ["critique"] })}\n\`\`\``,
        );

        const grown = yield* (yield* parentEvents(parent.threadId)).pipe(
          Stream.filter(({ payload }) =>
            (payload.workflow?.agents ?? []).some((agent) => agent.label === "probe-17"),
          ),
          Stream.runHead,
          Effect.map((event) => Option.getOrThrow(event).payload.workflow!),
        );
        assert.deepEqual(
          grown.phases.map((phase) => phase.title),
          ["draft · round 1", "critique · round 1", "draft · round 2", "critique · round 2"],
        );
        // Every grown row is there before round 2 starts anything; rerun left the poet out.
        const round2 = grown.agents.filter((agent) => (agent.phaseIndex ?? 0) >= 2);
        assert.deepEqual(
          round2.map((row) => row.label),
          [...addAgents.map((add) => add.key), "critic"],
        );
        for (const row of round2) {
          assert.deepEqual([row.state, row.childThreadId], ["queued", undefined]);
        }

        const probes = yield* settleNext(17, "Probed.");
        assert.include(
          probes.find((child) => child.title === "draft.probe-3#2")?.prompt ?? "",
          "Probe angle 3.",
        );
        yield* settleNext(1, '```json\n{"ok":true}\n```');
        assert.equal((yield* ended(parent.threadId, subagentId))?.status, "completed");
        const status = yield* runs.status(parent.scope, subagentId);
        assert.deepInclude(status.stages[0], { exit: "accepted" });

        const sized = yield* largestUpdate(parent.threadId, subagentId);
        assert.lengthOf(sized.agents, 20);
        assert.isAtMost(sized.largest, 2048);
      }),
  );

  it.effect("says why a judge's invalid growth failed the run: result, status, wake and row", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const runs = yield* workflowRunsIn(yield* Effect.scope);
      const parent = yield* seedParent("growth-error");
      const { subagentId } = yield* runs.run(parent.scope, {
        name: "grow",
        stages: [
          {
            id: "loop",
            repeat: {
              stages: [
                { id: "draft", agents: [{ id: "poet", task: "Write." }] },
                {
                  id: "critique",
                  agents: [
                    {
                      id: "critic",
                      task: "Judge {{draft}}.",
                      resultSchema: { type: "object", required: ["ok"] },
                    },
                  ],
                },
              ],
              judge: "critic",
              until: { path: "ok", equals: true },
              max: 2,
              templates: { rewriter: { task: "Rewrite {{draft}}." } },
            },
          },
        ],
      });
      yield* parent.endTurn;
      const [poet] = yield* childrenOf(parent.threadId, subagentId, 1);
      yield* settleChild(poet!, "Draft. ".repeat(3_000));
      const critic = (yield* childrenOf(parent.threadId, subagentId, 2)).find(
        (child) => child.id !== poet!.id,
      )!;
      const judged = { ok: false, addAgents: [{ template: "rewiter", task: "Fix line 1." }] };
      yield* settleChild(critic, `\`\`\`json\n${JSON.stringify(judged)}\n\`\`\``);

      const error = 'round 1 critic: addAgents[0]: unknown template "rewiter"';
      const row = yield* ended(parent.threadId, subagentId);
      assert.equal(row?.status, "failed");
      assert.equal(JSON.parse(row?.result ?? "{}").error, error);
      assert.equal((yield* runs.status(parent.scope, subagentId)).error, error);
      assert.deepInclude(
        row?.workflow?.agents.find((agent) => agent.label === "critic"),
        { state: "failed", result: error },
      );
      // Delivered as the continuation service would; the long draft keeps the result out of it.
      yield* wakes(parent.threadId);
      const offered = (yield* SubscriptionRef.get(yield* Offers)).find(
        (offer) => offer.threadId === parent.threadId,
      )!.delegatedCompletion!;
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("command:growth-error:wake"),
        threadId: parent.threadId,
        messageId: offered.messageId,
        text: "Delegated task completed.",
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "agent",
        creationSource: "server",
        delegatedCompletion: {
          parentRunId: offered.parentRunId,
          generation: offered.generation,
          taskIds: [subagentId],
        },
      });
      const wake = (yield* orchestrator.getThreadProjection(parent.threadId)).messages.find(
        (message) => message.id === offered.messageId,
      );
      assert.notInclude(wake?.text ?? "", row?.result ?? "");
      assert.include(wake?.text ?? "", error);
    }),
  );

  it.effect("resumes after a restart without relaunching a child and still wakes once", () =>
    Effect.gen(function* () {
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const parent = yield* seedParent("restart");
      const before = yield* Scope.make();
      const { subagentId } = yield* (yield* workflowRunsIn(before)).run(parent.scope, spec);
      const drafts = yield* childrenOf(parent.threadId, subagentId, 2);
      yield* Scope.close(before, Exit.void);
      // Both children finish while the server is down.
      for (const child of drafts) yield* settleChild(child, `Draft by ${child.driver}.`);
      assert.deepEqual(
        (yield* projections.getOpenWorkflowRuns).filter(
          ({ parentThreadId }) => parentThreadId === parent.threadId,
        ),
        [{ parentThreadId: parent.threadId, taskId: subagentId }],
      );

      yield* (yield* workflowRunsIn(yield* Effect.scope)).start();
      const children = yield* childrenOf(parent.threadId, subagentId, 3);
      assert.lengthOf(children, 3);
      const judge = children.find((child) => !drafts.some((draft) => draft.id === child.id))!;
      yield* settleChild(judge, '```json\n{"ok":true}\n```');
      const row = yield* ended(parent.threadId, subagentId);
      assert.equal(row?.status, "completed");
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const all = (yield* orchestrator.getThreadProjection(parent.threadId)).subagents;
      assert.lengthOf(
        all.filter((task) => task.parentNodeId === subagentId),
        3,
      );
      assert.lengthOf(yield* wakes(parent.threadId), 1);
    }),
  );

  it.effect("status, wake and resume agree that a schema-failed judge failed the run", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const runs = yield* workflowRunsIn(yield* Effect.scope);
      const live = yield* seedParent("invalid-judge");
      const { subagentId } = yield* runs.run(live.scope, spec);
      yield* live.endTurn;
      const drafts = yield* childrenOf(live.threadId, subagentId, 2);
      for (const child of drafts) yield* settleChild(child, "Draft.");
      const judge = (yield* childrenOf(live.threadId, subagentId, 3)).find(
        (child) => !drafts.some((draft) => draft.id === child.id),
      )!;
      yield* settleChild(judge, '```json\n{"nope":true}\n```');
      const row = yield* ended(live.threadId, subagentId);
      assert.equal(row?.status, "failed");
      const woken = JSON.parse(row?.result ?? "{}");
      assert.equal(woken.stages[1].agents[0].status, "invalid_output");
      assert.match(woken.error, /^round 1 j: invalid_output: ./);
      assert.deepInclude(
        row?.workflow?.agents.find((agent) => agent.label === "j"),
        { state: "failed", result: woken.error },
      );
      assert.deepEqual(yield* runs.status(live.scope, subagentId), woken);

      // The runner records the judge's outcome, and the server stops before it ends the run.
      const parent = yield* seedParent("invalid-judge-restart");
      const before = yield* Scope.make();
      const resumed = yield* (yield* workflowRunsIn(before)).run(parent.scope, spec);
      yield* parent.endTurn;
      const first = yield* childrenOf(parent.threadId, resumed.subagentId, 2);
      for (const child of first) yield* settleChild(child, "Draft.");
      const late = (yield* childrenOf(parent.threadId, resumed.subagentId, 3)).find(
        (child) => !first.some((draft) => draft.id === child.id),
      )!;
      yield* Scope.close(before, Exit.void);
      yield* settleChild(late, '```json\n{"nope":true}\n```');
      const open = (yield* orchestrator.getThreadProjection(parent.threadId)).subagents.find(
        (task) => task.id === resumed.subagentId,
      )!;
      yield* orchestrator.dispatch({
        type: "workflow_run.update",
        commandId: CommandId.make("command:invalid-judge-restart:judged"),
        parentThreadId: parent.threadId,
        taskId: resumed.subagentId,
        workflow: {
          ...open.workflow!,
          agents: open.workflow!.agents.map((agent) =>
            agent.label === "j"
              ? { ...agent, childThreadId: late.childThreadId!, state: "failed" as const }
              : agent,
          ),
        },
      });
      assert.lengthOf(
        (yield* projections.getOpenWorkflowRuns).filter(
          ({ taskId }) => taskId === resumed.subagentId,
        ),
        1,
      );

      yield* (yield* workflowRunsIn(yield* Effect.scope)).start();
      const after = yield* ended(parent.threadId, resumed.subagentId);
      assert.equal(after?.status, "failed");
      assert.equal(JSON.parse(after?.result ?? "{}").stages[1].agents[0].status, "invalid_output");
    }),
  );

  it.effect("cancels the live children of a run that ended just before a restart", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const parent = yield* seedParent("ended-restart");
      const before = yield* Scope.make();
      const { subagentId } = yield* (yield* workflowRunsIn(before)).run(parent.scope, spec);
      yield* parent.endTurn;
      const drafts = yield* childrenOf(parent.threadId, subagentId, 2);
      yield* Scope.close(before, Exit.void);
      // The runner's terminal write lands; the server stops before it cancels the children.
      const result = '{"name":"review","status":"failed","stages":[]}';
      const row = (yield* orchestrator.getThreadProjection(parent.threadId)).subagents.find(
        (task) => task.id === subagentId,
      )!;
      yield* orchestrator.dispatch({
        type: "workflow_run.update",
        commandId: CommandId.make("command:ended-restart:terminal"),
        parentThreadId: parent.threadId,
        taskId: subagentId,
        workflowRun: { ...row.workflowRun!, status: "failed" },
        result,
      });
      assert.lengthOf(yield* wakes(parent.threadId), 1);

      yield* (yield* workflowRunsIn(yield* Effect.scope)).start();
      const stopped = new Set<NodeId>();
      yield* (yield* parentEvents(parent.threadId)).pipe(
        Stream.takeUntil(({ payload }) => {
          if (drafts.some((draft) => draft.id === payload.id) && payload.status !== "running") {
            stopped.add(payload.id);
          }
          return stopped.size === drafts.length;
        }),
        Stream.runDrain,
      );
      const ended = (yield* orchestrator.getThreadProjection(parent.threadId)).subagents.find(
        (task) => task.id === subagentId,
      );
      assert.equal(ended?.status, "failed");
      assert.equal(ended?.result, result);
      assert.lengthOf(yield* wakes(parent.threadId), 1);
    }),
  );

  it.effect("stops mid-run: running children stop, no later stage starts, it ends cancelled", () =>
    Effect.gen(function* () {
      const threadManagement = yield* ThreadManagementService.ThreadManagementService;
      const runs = yield* workflowRunsIn(yield* Effect.scope);
      const parent = yield* seedParent("stop");
      const { subagentId } = yield* runs.run(parent.scope, spec);
      yield* parent.endTurn;
      yield* childrenOf(parent.threadId, subagentId, 2);
      yield* threadManagement.stopWorkflow({ threadId: parent.threadId, subagentId });

      const row = yield* ended(parent.threadId, subagentId);
      assert.equal(row?.status, "cancelled");
      assert.equal(row?.workflowRun?.status, "cancelled");
      assert.equal(row?.workflowRun?.stop, true);
      assert.equal(JSON.parse(row?.result ?? "{}").status, "cancelled");
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const children = (yield* orchestrator.getThreadProjection(parent.threadId)).subagents.filter(
        (task) => task.parentNodeId === subagentId,
      );
      assert.lengthOf(children, 2);
      assert.lengthOf(yield* wakes(parent.threadId), 1);
    }),
  );

  it.effect("refuses an invalid spec before anything starts", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const runs = yield* workflowRunsIn(yield* Effect.scope);
      const parent = yield* seedParent("invalid");
      const error = yield* runs
        .run(parent.scope, {
          name: "x",
          stages: [{ id: "s", agents: [{ id: "a", task: "{{s}}" }] }],
        })
        .pipe(Effect.flip);
      assert.equal(error.code, "invalid_request");
      assert.lengthOf((yield* orchestrator.getThreadProjection(parent.threadId)).subagents, 0);
    }),
  );

  it.effect("refuses an option a later agent's model does not declare before anything starts", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const runs = yield* workflowRunsIn(yield* Effect.scope);
      const parent = yield* seedParent("bad-option");
      const target = {
        providerInstanceId: claude,
        model: "claude-sonnet-5-5",
        options: [{ id: "fastMode", value: true }],
      };
      const error = yield* runs
        .run(parent.scope, {
          ...spec,
          stages: [spec.stages[0], { id: "late", agents: [{ id: "x", task: "Go.", target }] }],
        })
        .pipe(Effect.flip);
      assert.equal(error.code, "invalid_request");
      assert.equal(
        error.message,
        'Agent "x": Model claude-sonnet-5-5 on provider claudeAgent rejected options: Unknown option fastMode; supported options: effort.',
      );
      assert.lengthOf((yield* orchestrator.getThreadProjection(parent.threadId)).subagents, 0);
    }),
  );
});
