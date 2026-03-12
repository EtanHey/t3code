import { EventEmitter } from "node:events";

import type {
  ApprovalRequestId,
  ProviderApprovalDecision,
  ProviderInteractionMode,
  ProviderRuntimeEvent,
  ProviderSession,
  ProviderSessionStartInput,
  ProviderTurnStartResult,
  ProviderUserInputAnswers,
  RuntimeMode,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { Effect, FileSystem, Layer, Queue, ServiceMap, Stream } from "effect";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import { ProviderAdapterRequestError, ProviderAdapterValidationError } from "../Errors.ts";
import { ClaudeAdapter, type ClaudeAdapterShape } from "../Services/ClaudeAdapter.ts";
import {
  toProviderAdapterMessage,
  toProviderAdapterProcessError,
  toProviderAdapterRequestError,
} from "./ProviderAdapterErrorUtils.ts";

const PROVIDER = "claude" as const;

export interface ClaudeAdapterAttachment {
  readonly type: "image";
  readonly url: string;
  readonly mimeType: string;
  readonly name: string;
}

export interface ClaudeAdapterStartSessionInput {
  readonly threadId: ThreadId;
  readonly provider?: "claude";
  readonly cwd?: string;
  readonly model?: string;
  readonly resumeCursor?: unknown;
  readonly providerOptions?: ProviderSessionStartInput["providerOptions"];
  readonly runtimeMode: RuntimeMode;
}

export interface ClaudeAdapterSendTurnInput {
  readonly threadId: ThreadId;
  readonly input?: string;
  readonly attachments?: ReadonlyArray<ClaudeAdapterAttachment>;
  readonly model?: string;
  readonly interactionMode?: ProviderInteractionMode;
}

export interface ClaudeThreadTurnSnapshot {
  readonly id: TurnId;
  readonly items: ReadonlyArray<unknown>;
}

export interface ClaudeThreadSnapshot {
  readonly threadId: ThreadId;
  readonly turns: ReadonlyArray<ClaudeThreadTurnSnapshot>;
}

export interface ClaudeAdapterManagerEvents {
  event: [event: ProviderRuntimeEvent];
}

export interface ClaudeAdapterManager {
  readonly startSession: (input: ClaudeAdapterStartSessionInput) => Promise<ProviderSession>;
  readonly sendTurn: (input: ClaudeAdapterSendTurnInput) => Promise<ProviderTurnStartResult>;
  readonly interruptTurn: (threadId: ThreadId, turnId?: TurnId) => Promise<void>;
  readonly readThread: (threadId: ThreadId) => Promise<ClaudeThreadSnapshot>;
  readonly rollbackThread: (threadId: ThreadId, numTurns: number) => Promise<ClaudeThreadSnapshot>;
  readonly respondToRequest: (
    threadId: ThreadId,
    requestId: ApprovalRequestId,
    decision: ProviderApprovalDecision,
  ) => Promise<void>;
  readonly respondToUserInput: (
    threadId: ThreadId,
    requestId: ApprovalRequestId,
    answers: ProviderUserInputAnswers,
  ) => Promise<void>;
  readonly stopSession: (threadId: ThreadId) => void;
  readonly listSessions: () => ReadonlyArray<ProviderSession>;
  readonly hasSession: (threadId: ThreadId) => boolean;
  readonly stopAll: () => void;
  readonly on: (
    eventName: "event",
    listener: (event: ProviderRuntimeEvent) => void,
  ) => ClaudeAdapterManager;
  readonly off: (
    eventName: "event",
    listener: (event: ProviderRuntimeEvent) => void,
  ) => ClaudeAdapterManager;
}

class UnsupportedClaudeAdapterManager
  extends EventEmitter<ClaudeAdapterManagerEvents>
  implements ClaudeAdapterManager
{
  private unsupported(): Error {
    return new Error("Claude adapter manager is not configured.");
  }

  startSession(_input: ClaudeAdapterStartSessionInput): Promise<ProviderSession> {
    return Promise.reject(this.unsupported());
  }

  sendTurn(_input: ClaudeAdapterSendTurnInput): Promise<ProviderTurnStartResult> {
    return Promise.reject(this.unsupported());
  }

  interruptTurn(_threadId: ThreadId, _turnId?: TurnId): Promise<void> {
    return Promise.reject(this.unsupported());
  }

  readThread(_threadId: ThreadId): Promise<ClaudeThreadSnapshot> {
    return Promise.reject(this.unsupported());
  }

  rollbackThread(_threadId: ThreadId, _numTurns: number): Promise<ClaudeThreadSnapshot> {
    return Promise.reject(this.unsupported());
  }

  respondToRequest(
    _threadId: ThreadId,
    _requestId: ApprovalRequestId,
    _decision: ProviderApprovalDecision,
  ): Promise<void> {
    return Promise.reject(this.unsupported());
  }

  respondToUserInput(
    _threadId: ThreadId,
    _requestId: ApprovalRequestId,
    _answers: ProviderUserInputAnswers,
  ): Promise<void> {
    return Promise.reject(this.unsupported());
  }

  stopSession(_threadId: ThreadId): void {}

  listSessions(): ReadonlyArray<ProviderSession> {
    return [];
  }

  hasSession(_threadId: ThreadId): boolean {
    return false;
  }

  stopAll(): void {}
}

export interface ClaudeAdapterLiveOptions {
  readonly manager?: ClaudeAdapterManager;
  readonly makeManager?: (services?: ServiceMap.ServiceMap<never>) => ClaudeAdapterManager;
}

const makeClaudeAdapter = (options?: ClaudeAdapterLiveOptions) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const serverConfig = yield* Effect.service(ServerConfig);

    const manager = yield* Effect.acquireRelease(
      Effect.gen(function* () {
        if (options?.manager) {
          return options.manager;
        }

        const services = yield* Effect.services<never>();
        return options?.makeManager?.(services) ?? new UnsupportedClaudeAdapterManager();
      }),
      (manager) =>
        Effect.sync(() => {
          try {
            manager.stopAll();
          } catch {
            // Finalizers should not block layer shutdown.
          }
        }),
    );

    const startSession: ClaudeAdapterShape["startSession"] = (input) => {
      if (input.provider !== undefined && input.provider !== PROVIDER) {
        return Effect.fail(
          new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
          }),
        );
      }

      const managerInput: ClaudeAdapterStartSessionInput = {
        threadId: input.threadId,
        provider: PROVIDER,
        ...(input.cwd !== undefined ? { cwd: input.cwd } : {}),
        ...(input.model !== undefined ? { model: input.model } : {}),
        ...(input.resumeCursor !== undefined ? { resumeCursor: input.resumeCursor } : {}),
        ...(input.providerOptions !== undefined ? { providerOptions: input.providerOptions } : {}),
        runtimeMode: input.runtimeMode,
      };

      return Effect.tryPromise({
        try: () => manager.startSession(managerInput),
        catch: (cause) =>
          toProviderAdapterProcessError({
            provider: PROVIDER,
            threadId: input.threadId,
            fallback: "Failed to start Claude adapter session.",
            cause,
          }),
      });
    };

    const sendTurn: ClaudeAdapterShape["sendTurn"] = (input) =>
      Effect.gen(function* () {
        const attachments = yield* Effect.forEach(
          input.attachments ?? [],
          (attachment) =>
            Effect.gen(function* () {
              const attachmentPath = resolveAttachmentPath({
                stateDir: serverConfig.stateDir,
                attachment,
              });

              if (!attachmentPath) {
                return yield* toProviderAdapterRequestError({
                  provider: PROVIDER,
                  threadId: input.threadId,
                  method: "turn/start",
                  cause: new Error(`Invalid attachment id '${attachment.id}'.`),
                });
              }

              const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapterRequestError({
                      provider: PROVIDER,
                      method: "turn/start",
                      detail: toProviderAdapterMessage(cause, "Failed to read attachment file."),
                      cause,
                    }),
                ),
              );

              return {
                type: "image" as const,
                url: `data:${attachment.mimeType};base64,${Buffer.from(bytes).toString("base64")}`,
                mimeType: attachment.mimeType,
                name: attachment.name,
              };
            }),
          { concurrency: 1 },
        );

        return yield* Effect.tryPromise({
          try: () =>
            manager.sendTurn({
              threadId: input.threadId,
              ...(input.input !== undefined ? { input: input.input } : {}),
              ...(input.model !== undefined ? { model: input.model } : {}),
              ...(input.interactionMode !== undefined
                ? { interactionMode: input.interactionMode }
                : {}),
              ...(attachments.length > 0 ? { attachments } : {}),
            }),
          catch: (cause) =>
            toProviderAdapterRequestError({
              provider: PROVIDER,
              threadId: input.threadId,
              method: "turn/start",
              cause,
            }),
        }).pipe(
          Effect.map((result) => ({
            ...result,
            threadId: input.threadId,
          })),
        );
      });

    const interruptTurn: ClaudeAdapterShape["interruptTurn"] = (threadId, turnId) =>
      Effect.tryPromise({
        try: () => manager.interruptTurn(threadId, turnId),
        catch: (cause) =>
          toProviderAdapterRequestError({
            provider: PROVIDER,
            threadId,
            method: "turn/interrupt",
            cause,
          }),
      });

    const readThread: ClaudeAdapterShape["readThread"] = (threadId) =>
      Effect.tryPromise({
        try: () => manager.readThread(threadId),
        catch: (cause) =>
          toProviderAdapterRequestError({
            provider: PROVIDER,
            threadId,
            method: "thread/read",
            cause,
          }),
      }).pipe(
        Effect.map((snapshot) => ({
          threadId,
          turns: snapshot.turns,
        })),
      );

    const rollbackThread: ClaudeAdapterShape["rollbackThread"] = (threadId, numTurns) => {
      if (!Number.isInteger(numTurns) || numTurns < 1) {
        return Effect.fail(
          new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "rollbackThread",
            issue: "numTurns must be an integer >= 1.",
          }),
        );
      }

      return Effect.tryPromise({
        try: () => manager.rollbackThread(threadId, numTurns),
        catch: (cause) =>
          toProviderAdapterRequestError({
            provider: PROVIDER,
            threadId,
            method: "thread/rollback",
            cause,
          }),
      }).pipe(
        Effect.map((snapshot) => ({
          threadId,
          turns: snapshot.turns,
        })),
      );
    };

    const respondToRequest: ClaudeAdapterShape["respondToRequest"] = (
      threadId,
      requestId,
      decision,
    ) =>
      Effect.tryPromise({
        try: () => manager.respondToRequest(threadId, requestId, decision),
        catch: (cause) =>
          toProviderAdapterRequestError({
            provider: PROVIDER,
            threadId,
            method: "request/respond",
            cause,
          }),
      });

    const respondToUserInput: ClaudeAdapterShape["respondToUserInput"] = (
      threadId,
      requestId,
      answers,
    ) =>
      Effect.tryPromise({
        try: () => manager.respondToUserInput(threadId, requestId, answers),
        catch: (cause) =>
          toProviderAdapterRequestError({
            provider: PROVIDER,
            threadId,
            method: "user-input/respond",
            cause,
          }),
      });

    const stopSession: ClaudeAdapterShape["stopSession"] = (threadId) =>
      Effect.sync(() => {
        manager.stopSession(threadId);
      });

    const listSessions: ClaudeAdapterShape["listSessions"] = () =>
      Effect.sync(() => manager.listSessions());

    const hasSession: ClaudeAdapterShape["hasSession"] = (threadId) =>
      Effect.sync(() => manager.hasSession(threadId));

    const stopAll: ClaudeAdapterShape["stopAll"] = () =>
      Effect.sync(() => {
        manager.stopAll();
      });

    const runtimeEventQueue = yield* Queue.unbounded<ProviderRuntimeEvent>();

    yield* Effect.acquireRelease(
      Effect.sync(() => {
        const listener = (event: ProviderRuntimeEvent) => {
          void Effect.runPromise(Queue.offer(runtimeEventQueue, event).pipe(Effect.asVoid));
        };

        manager.on("event", listener);
        return listener;
      }),
      (listener) =>
        Effect.gen(function* () {
          yield* Effect.sync(() => {
            manager.off("event", listener);
          });
          yield* Queue.shutdown(runtimeEventQueue);
        }),
    );

    return {
      provider: PROVIDER,
      capabilities: {
        sessionModelSwitch: "restart-session",
      },
      startSession,
      sendTurn,
      interruptTurn,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      readThread,
      rollbackThread,
      stopAll,
      streamEvents: Stream.fromQueue(runtimeEventQueue),
    } satisfies ClaudeAdapterShape;
  });

export const ClaudeAdapterLive = Layer.effect(ClaudeAdapter, makeClaudeAdapter());

export function makeClaudeAdapterLive(options?: ClaudeAdapterLiveOptions) {
  return Layer.effect(ClaudeAdapter, makeClaudeAdapter(options));
}
