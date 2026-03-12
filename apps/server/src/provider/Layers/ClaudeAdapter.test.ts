import assert from "node:assert/strict";
import { EventEmitter } from "node:events";

import {
  ApprovalRequestId,
  EventId,
  type ProviderApprovalDecision,
  type ProviderRuntimeEvent,
  type ProviderSession,
  type ProviderTurnStartResult,
  type ProviderUserInputAnswers,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { afterAll, it, vi } from "@effect/vitest";

import { Effect, Fiber, Layer, Option, Stream } from "effect";

import type {
  ClaudeAdapterManager,
  ClaudeAdapterManagerEvents,
  ClaudeAdapterSendTurnInput,
  ClaudeAdapterStartSessionInput,
} from "./ClaudeAdapter.ts";
import { makeClaudeAdapterLive } from "./ClaudeAdapter.ts";
import { ClaudeAdapter } from "../Services/ClaudeAdapter.ts";
import { ProviderSessionDirectory } from "../Services/ProviderSessionDirectory.ts";
import { ServerConfig } from "../../config.ts";

const asThreadId = (value: string): ThreadId => ThreadId.makeUnsafe(value);
const asTurnId = (value: string): TurnId => TurnId.makeUnsafe(value);
const asEventId = (value: string): EventId => EventId.makeUnsafe(value);

class FakeClaudeManager
  extends EventEmitter<ClaudeAdapterManagerEvents>
  implements ClaudeAdapterManager
{
  public startSessionImpl = vi.fn(
    async (input: ClaudeAdapterStartSessionInput): Promise<ProviderSession> => {
      const now = new Date().toISOString();
      return {
        provider: "claude",
        status: "ready",
        runtimeMode: input.runtimeMode,
        threadId: input.threadId,
        cwd: input.cwd,
        createdAt: now,
        updatedAt: now,
      };
    },
  );

  public sendTurnImpl = vi.fn(
    async (_input: ClaudeAdapterSendTurnInput): Promise<ProviderTurnStartResult> => ({
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-1"),
    }),
  );

  public interruptTurnImpl = vi.fn(
    async (_threadId: ThreadId, _turnId?: TurnId): Promise<void> => undefined,
  );

  public readThreadImpl = vi.fn(async (_threadId: ThreadId) => ({
    threadId: asThreadId("thread-1"),
    turns: [],
  }));

  public rollbackThreadImpl = vi.fn(async (_threadId: ThreadId, _numTurns: number) => ({
    threadId: asThreadId("thread-1"),
    turns: [],
  }));

  public respondToRequestImpl = vi.fn(
    async (
      _threadId: ThreadId,
      _requestId: ApprovalRequestId,
      _decision: ProviderApprovalDecision,
    ): Promise<void> => undefined,
  );

  public respondToUserInputImpl = vi.fn(
    async (
      _threadId: ThreadId,
      _requestId: ApprovalRequestId,
      _answers: ProviderUserInputAnswers,
    ): Promise<void> => undefined,
  );

  public stopAllImpl = vi.fn(() => undefined);

  startSession(input: ClaudeAdapterStartSessionInput): Promise<ProviderSession> {
    return this.startSessionImpl(input);
  }

  sendTurn(input: ClaudeAdapterSendTurnInput): Promise<ProviderTurnStartResult> {
    return this.sendTurnImpl(input);
  }

  interruptTurn(threadId: ThreadId, turnId?: TurnId): Promise<void> {
    return this.interruptTurnImpl(threadId, turnId);
  }

  readThread(threadId: ThreadId) {
    return this.readThreadImpl(threadId);
  }

  rollbackThread(threadId: ThreadId, numTurns: number) {
    return this.rollbackThreadImpl(threadId, numTurns);
  }

  respondToRequest(
    threadId: ThreadId,
    requestId: ApprovalRequestId,
    decision: ProviderApprovalDecision,
  ): Promise<void> {
    return this.respondToRequestImpl(threadId, requestId, decision);
  }

  respondToUserInput(
    threadId: ThreadId,
    requestId: ApprovalRequestId,
    answers: ProviderUserInputAnswers,
  ): Promise<void> {
    return this.respondToUserInputImpl(threadId, requestId, answers);
  }

  stopSession(_threadId: ThreadId): void {}

  listSessions(): ProviderSession[] {
    return [];
  }

  hasSession(_threadId: ThreadId): boolean {
    return false;
  }

  stopAll(): void {
    this.stopAllImpl();
  }
}

const providerSessionDirectoryTestLayer = Layer.succeed(ProviderSessionDirectory, {
  upsert: () => Effect.void,
  getProvider: () =>
    Effect.die(new Error("ProviderSessionDirectory.getProvider is not used in test")),
  getBinding: () => Effect.succeed(Option.none()),
  remove: () => Effect.void,
  listThreadIds: () => Effect.succeed([]),
});

const validationManager = new FakeClaudeManager();
const validationLayer = it.layer(
  makeClaudeAdapterLive({ manager: validationManager }).pipe(
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
    Layer.provideMerge(providerSessionDirectoryTestLayer),
    Layer.provideMerge(NodeServices.layer),
  ),
);

validationLayer("ClaudeAdapterLive validation", (it) => {
  it.effect("rejects a mismatched provider during session start", () =>
    Effect.gen(function* () {
      const adapter = yield* ClaudeAdapter;
      const result = yield* adapter
        .startSession({
          provider: "codex",
          threadId: asThreadId("thread-1"),
          runtimeMode: "full-access",
        })
        .pipe(Effect.result);

      assert.equal(result._tag, "Failure");
      if (result._tag !== "Failure") {
        return;
      }

      assert.equal(result.failure._tag, "ProviderAdapterValidationError");
      if (result.failure._tag !== "ProviderAdapterValidationError") {
        return;
      }
      assert.equal(result.failure.provider, "claude");
    }),
  );

  it.effect("maps sendTurn thread ids back to the requested claude thread id", () =>
    Effect.gen(function* () {
      validationManager.sendTurnImpl.mockClear();
      const adapter = yield* ClaudeAdapter;

      const result = yield* adapter.sendTurn({
        threadId: asThreadId("thread-1"),
        input: "hello",
        attachments: [],
      });

      assert.equal(result.threadId, "thread-1");
      assert.deepStrictEqual(validationManager.sendTurnImpl.mock.calls[0]?.[0], {
        threadId: asThreadId("thread-1"),
        input: "hello",
      });
    }),
  );
});

const sessionErrorManager = new FakeClaudeManager();
sessionErrorManager.sendTurnImpl.mockImplementation(async () => {
  throw new Error("Unknown session: claude-thread-missing");
});
const sessionErrorLayer = it.layer(
  makeClaudeAdapterLive({ manager: sessionErrorManager }).pipe(
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
    Layer.provideMerge(providerSessionDirectoryTestLayer),
    Layer.provideMerge(NodeServices.layer),
  ),
);

sessionErrorLayer("ClaudeAdapterLive session errors", (it) => {
  it.effect("maps unknown-session sendTurn errors to ProviderAdapterSessionNotFoundError", () =>
    Effect.gen(function* () {
      const adapter = yield* ClaudeAdapter;
      const result = yield* adapter
        .sendTurn({
          threadId: asThreadId("claude-thread-missing"),
          input: "hello",
          attachments: [],
        })
        .pipe(Effect.result);

      assert.equal(result._tag, "Failure");
      if (result._tag !== "Failure") {
        return;
      }

      assert.equal(result.failure._tag, "ProviderAdapterSessionNotFoundError");
      if (result.failure._tag !== "ProviderAdapterSessionNotFoundError") {
        return;
      }
      assert.equal(result.failure.provider, "claude");
      assert.equal(result.failure.threadId, "claude-thread-missing");
    }),
  );

  it.effect("rejects rollbackThread requests below 1 turn", () =>
    Effect.gen(function* () {
      const adapter = yield* ClaudeAdapter;
      const result = yield* adapter.rollbackThread(asThreadId("thread-1"), 0).pipe(Effect.result);

      assert.equal(result._tag, "Failure");
      if (result._tag !== "Failure") {
        return;
      }

      assert.equal(result.failure._tag, "ProviderAdapterValidationError");
      if (result.failure._tag !== "ProviderAdapterValidationError") {
        return;
      }
      assert.equal(result.failure.operation, "rollbackThread");
    }),
  );
});

const lifecycleManager = new FakeClaudeManager();
const lifecycleLayer = it.layer(
  makeClaudeAdapterLive({ manager: lifecycleManager }).pipe(
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
    Layer.provideMerge(providerSessionDirectoryTestLayer),
    Layer.provideMerge(NodeServices.layer),
  ),
);

lifecycleLayer("ClaudeAdapterLive lifecycle", (it) => {
  it.effect("forwards canonical claude runtime events through the adapter stream", () =>
    Effect.gen(function* () {
      const adapter = yield* ClaudeAdapter;
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      lifecycleManager.emit("event", {
        eventId: asEventId("evt-claude-session-ready"),
        provider: "claude",
        threadId: asThreadId("thread-1"),
        createdAt: new Date().toISOString(),
        type: "session.state.changed",
        payload: {
          state: "ready",
        },
      } satisfies ProviderRuntimeEvent);

      const firstEvent = yield* Fiber.join(firstEventFiber);

      assert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }

      assert.equal(firstEvent.value.provider, "claude");
      assert.equal(firstEvent.value.type, "session.state.changed");
      if (firstEvent.value.type !== "session.state.changed") {
        return;
      }
      assert.equal(firstEvent.value.payload.state, "ready");
    }),
  );
});

afterAll(() => {
  validationManager.removeAllListeners();
  sessionErrorManager.removeAllListeners();
  lifecycleManager.removeAllListeners();
});
