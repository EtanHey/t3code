import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  EventId,
  MessageId,
  NodeId,
  OrchestrationV2DomainEvent,
  type OrchestrationV2Subagent,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  type ServerProvider,
  ThreadId,
} from "@t3tools/contracts";
import { restoreSubagentBodies } from "@t3tools/shared/orchestrationV2DedupedBodies";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

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
  Ref.Ref<ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>>
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

/** A Codex parent whose turn is live when it calls t3_workflow_run. */
const seedParent = (name: string) =>
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
      modelSelection,
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
        modelSelection,
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
          payload: { ...childRun, status: "completed", startedAt: now, completedAt: now },
        },
      ],
    });
  });

const wakes = (threadId: ThreadId) =>
  Effect.map(Offers.pipe(Effect.flatMap(Ref.get)), (all) => [
    ...new Set(
      all
        .filter((offer) => offer.threadId === threadId)
        .map((offer) => offer.delegatedCompletion?.messageId),
    ),
  ]);

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
    "keeps every workflow row update within 2 KB on the wire across a 20-agent stage and 3 rounds",
    () =>
      Effect.gen(function* () {
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
