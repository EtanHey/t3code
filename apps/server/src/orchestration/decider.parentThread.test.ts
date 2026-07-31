import {
  CommandId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationCommand,
  type OrchestrationReadModel,
  type OrchestrationThread,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { decideOrchestrationCommand } from "./decider.ts";

const NOW = "2026-01-01T00:00:00.000Z";
const PROJECT_ID = ProjectId.make("project-a");
const OTHER_PROJECT_ID = ProjectId.make("project-b");
const PARENT_THREAD_ID = ThreadId.make("thread-parent");
const CHILD_THREAD_ID = ThreadId.make("thread-child");

function makeThread(input: {
  readonly id: ThreadId;
  readonly projectId?: ProjectId;
  readonly parentThreadId?: ThreadId | null;
  readonly deletedAt?: string | null;
}): OrchestrationThread {
  return {
    id: input.id,
    projectId: input.projectId ?? PROJECT_ID,
    parentThreadId: input.parentThreadId ?? null,
    title: "Existing thread",
    modelSelection: {
      instanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5-codex",
    },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    latestTurn: null,
    createdAt: NOW,
    updatedAt: NOW,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    deletedAt: input.deletedAt ?? null,
    messages: [],
    proposedPlans: [],
    activities: [],
    checkpoints: [],
    session: null,
  };
}

function makeReadModel(threads: ReadonlyArray<OrchestrationThread>): OrchestrationReadModel {
  return {
    snapshotSequence: 2,
    projects: [
      {
        id: PROJECT_ID,
        title: "Project A",
        workspaceRoot: "/tmp/project-a",
        defaultModelSelection: null,
        scripts: [],
        createdAt: NOW,
        updatedAt: NOW,
        deletedAt: null,
      },
      {
        id: OTHER_PROJECT_ID,
        title: "Project B",
        workspaceRoot: "/tmp/project-b",
        defaultModelSelection: null,
        scripts: [],
        createdAt: NOW,
        updatedAt: NOW,
        deletedAt: null,
      },
    ],
    threads,
    updatedAt: NOW,
  };
}

function makeCreateCommand(
  parentThreadId: ThreadId | null,
): Extract<OrchestrationCommand, { type: "thread.create" }> {
  return {
    type: "thread.create",
    commandId: CommandId.make("cmd-thread-create"),
    threadId: CHILD_THREAD_ID,
    projectId: PROJECT_ID,
    parentThreadId,
    title: "New thread",
    modelSelection: {
      instanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5-codex",
    },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdAt: NOW,
  };
}

it.layer(NodeServices.layer)("parent thread decider", (it) => {
  it.effect("emits the exact nullable parentThreadId for standalone and worker threads", () =>
    Effect.gen(function* () {
      const parent = makeThread({ id: PARENT_THREAD_ID });
      const readModel = makeReadModel([parent]);

      for (const parentThreadId of [null, PARENT_THREAD_ID] as const) {
        const result = yield* decideOrchestrationCommand({
          command: makeCreateCommand(parentThreadId),
          readModel,
        });
        const event = Array.isArray(result) ? result[0] : result;

        expect(event.type).toBe("thread.created");
        if (event.type === "thread.created") {
          expect(event.payload.parentThreadId).toBe(parentThreadId);
        }
      }
    }),
  );

  it.effect("rejects a missing parent thread", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        decideOrchestrationCommand({
          command: makeCreateCommand(PARENT_THREAD_ID),
          readModel: makeReadModel([]),
        }),
      );

      expect(error.message).toContain("does not exist");
    }),
  );

  it.effect("rejects a deleted parent thread", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        decideOrchestrationCommand({
          command: makeCreateCommand(PARENT_THREAD_ID),
          readModel: makeReadModel([makeThread({ id: PARENT_THREAD_ID, deletedAt: NOW })]),
        }),
      );

      expect(error.message).toContain("deleted");
    }),
  );

  it.effect("rejects a parent thread from another project", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        decideOrchestrationCommand({
          command: makeCreateCommand(PARENT_THREAD_ID),
          readModel: makeReadModel([
            makeThread({ id: PARENT_THREAD_ID, projectId: OTHER_PROJECT_ID }),
          ]),
        }),
      );

      expect(error.message).toContain("project");
    }),
  );

  it.effect("rejects a thread as its own parent", () =>
    Effect.gen(function* () {
      const command = {
        ...makeCreateCommand(CHILD_THREAD_ID),
        parentThreadId: CHILD_THREAD_ID,
      };
      const error = yield* Effect.flip(
        decideOrchestrationCommand({
          command,
          readModel: makeReadModel([]),
        }),
      );

      expect(error.message).toContain("own parent");
    }),
  );

  it.effect("deleting a parent does not cascade to its child", () =>
    Effect.gen(function* () {
      const parent = makeThread({ id: PARENT_THREAD_ID });
      const child = makeThread({
        id: CHILD_THREAD_ID,
        parentThreadId: PARENT_THREAD_ID,
      });
      const result = yield* decideOrchestrationCommand({
        command: {
          type: "thread.delete",
          commandId: CommandId.make("cmd-thread-delete-parent"),
          threadId: PARENT_THREAD_ID,
        },
        readModel: makeReadModel([parent, child]),
      });
      const events = Array.isArray(result) ? result : [result];

      expect(events).toHaveLength(1);
      expect(events[0]?.type).toBe("thread.deleted");
      expect(events[0]?.aggregateId).toBe(PARENT_THREAD_ID);
    }),
  );
});
