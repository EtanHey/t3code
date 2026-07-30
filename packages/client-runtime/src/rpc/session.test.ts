import {
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  EnvironmentAuthorizationError,
  KeybindingsConfigError,
  ServerConfig,
  ServerSettingsError,
  T3_RPC_COMPATIBILITY,
  type ServerConfig as ServerConfigType,
  WS_METHODS,
  WsRpcGroup,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as MutableList from "effect/MutableList";
import * as Queue from "effect/Queue";
import * as References from "effect/References";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as Rpc from "effect/unstable/rpc/Rpc";
import * as Socket from "effect/unstable/socket/Socket";

import {
  ConnectionTransientError,
  PrimaryConnectionTarget,
  type PreparedConnection,
} from "../connection/model.ts";
import * as RuntimeClient from "./runtimeClient.ts";
import * as RpcSession from "./session.ts";

type SocketEventType = "open" | "message" | "close" | "error";
type SocketEvent = {
  readonly code?: number;
  readonly data?: unknown;
  readonly reason?: string;
  readonly type: SocketEventType;
};
type SocketListener = (event: SocketEvent) => void;
type SocketTraceEvent =
  | {
      readonly _tag: "Send";
      readonly index: number;
    }
  | {
      readonly _tag: "QueueStateBeforeSend";
      readonly buffered: ReadonlyArray<unknown>;
      readonly index: number;
    };

class TestWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  readyState = TestWebSocket.CONNECTING;
  beforeSend: ((index: number) => void) | undefined;
  readonly sent: string[] = [];
  readonly trace: SocketTraceEvent[] = [];
  readonly url: string;
  private readonly listeners = new Map<SocketEventType, Set<SocketListener>>();
  private readonly sentWaiters = new Map<number, Set<(data: string) => void>>();

  constructor(url: string) {
    this.url = url;
  }

  addEventListener(type: SocketEventType, listener: SocketListener) {
    const listeners = this.listeners.get(type) ?? new Set<SocketListener>();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type: SocketEventType, listener: SocketListener) {
    this.listeners.get(type)?.delete(listener);
  }

  send(data: string) {
    const index = this.sent.length;
    this.beforeSend?.(index);
    this.sent.push(data);
    this.trace.push({ _tag: "Send", index });
    const waiters = this.sentWaiters.get(index);
    if (waiters !== undefined) {
      this.sentWaiters.delete(index);
      for (const resolve of waiters) {
        resolve(data);
      }
    }
  }

  awaitSent(index: number): Promise<string> {
    const existing = this.sent[index];
    if (existing !== undefined) {
      return Promise.resolve(existing);
    }
    return new Promise((resolve) => {
      const waiters = this.sentWaiters.get(index) ?? new Set<(data: string) => void>();
      waiters.add(resolve);
      this.sentWaiters.set(index, waiters);
    });
  }

  close(code = 1000, reason = "") {
    if (this.readyState === TestWebSocket.CLOSED) {
      return;
    }
    this.readyState = TestWebSocket.CLOSED;
    this.emit("close", { code, reason, type: "close" });
  }

  open() {
    this.readyState = TestWebSocket.OPEN;
    this.emit("open", { type: "open" });
  }

  serverMessage(data: string) {
    this.emit("message", { data, type: "message" });
  }

  private emit(type: SocketEventType, event: SocketEvent) {
    for (const listener of this.listeners.get(type) ?? []) {
      listener(event);
    }
  }
}

const TARGET = new PrimaryConnectionTarget({
  environmentId: EnvironmentId.make("environment-1"),
  label: "Test environment",
  httpBaseUrl: "https://environment.example.test",
  wsBaseUrl: "wss://environment.example.test",
});

const PREPARED: PreparedConnection = {
  environmentId: TARGET.environmentId,
  label: TARGET.label,
  httpBaseUrl: TARGET.httpBaseUrl,
  socketUrl: "wss://environment.example.test/ws?wsTicket=test",
  httpAuthorization: null,
  target: TARGET,
};

const SERVER_CONFIG: ServerConfigType = {
  environment: {
    environmentId: TARGET.environmentId,
    label: TARGET.label,
    platform: {
      os: "darwin",
      arch: "arm64",
    },
    serverVersion: "0.0.0-test",
    capabilities: {
      repositoryIdentity: true,
      connectionProbe: true,
    },
  },
  auth: {
    policy: "loopback-browser",
    bootstrapMethods: ["one-time-token"],
    sessionMethods: ["browser-session-cookie", "bearer-access-token"],
    sessionCookieName: "t3_session",
  },
  cwd: "/tmp/workspace",
  keybindingsConfigPath: "/tmp/workspace/keybindings.json",
  keybindings: [],
  issues: [],
  providers: [],
  availableEditors: [],
  observability: {
    logsDirectoryPath: "/tmp/logs",
    localTracingEnabled: false,
    otlpTracesEnabled: false,
    otlpMetricsEnabled: false,
  },
  settings: DEFAULT_SERVER_SETTINGS,
};

const RpcRequest = Schema.TaggedStruct("Request", {
  id: Schema.Union([Schema.String, Schema.Number]),
  payload: Schema.Unknown,
  tag: Schema.String,
});
const decodeJson = Schema.decodeUnknownSync(Schema.UnknownFromJsonString);
const decodeRpcRequest = Schema.decodeUnknownSync(RpcRequest);
const encodeJson = Schema.encodeUnknownSync(Schema.UnknownFromJsonString);
const encodeDefect = Schema.encodeSync(Schema.Defect());
const encodeServerConfig = Schema.encodeSync(ServerConfig);
const serverGetConfigRpc = WsRpcGroup.requests.get(WS_METHODS.serverGetConfig);
if (serverGetConfigRpc === undefined) {
  throw new Error("Expected WsRpcGroup to contain server.getConfig.");
}
const encodeServerGetConfigExit = Schema.encodeSync(
  Schema.toCodecJson(Rpc.exitSchema(serverGetConfigRpc)),
);
const ENCODED_SERVER_CONFIG = encodeServerConfig(SERVER_CONFIG);
const COMPATIBLE_SERVER_CONFIG_VALUE = {
  ...SERVER_CONFIG,
  environment: {
    ...SERVER_CONFIG.environment,
    rpc: T3_RPC_COMPATIBILITY,
  },
} satisfies ServerConfigType;
const COMPATIBLE_SERVER_CONFIG = encodeServerConfig(COMPATIBLE_SERVER_CONFIG_VALUE);
const LEGACY_SERVER_CONFIG = {
  ...ENCODED_SERVER_CONFIG,
  environment: {
    ...ENCODED_SERVER_CONFIG.environment,
    capabilities: {
      repositoryIdentity: true,
    },
  },
};

/**
 * This transport harness accepts both factory effects structurally. The
 * runtime-client API's one-argument connect contract is defined by
 * RuntimeClientRpcSessionFactory, not inferred from this helper.
 */
const makeFactory = Effect.fn("TestRpcSessionFactory.make")(function* (
  factoryEffect: typeof RpcSession.make = RpcSession.make,
) {
  const sockets: TestWebSocket[] = [];
  let resolveFirstSocket: ((socket: TestWebSocket) => void) | undefined;
  const firstSocket = new Promise<TestWebSocket>((resolve) => {
    resolveFirstSocket = resolve;
  });
  const constructorLayer = Layer.succeed(Socket.WebSocketConstructor, (url) => {
    const socket = new TestWebSocket(url);
    sockets.push(socket);
    resolveFirstSocket?.(socket);
    resolveFirstSocket = undefined;
    return socket as unknown as globalThis.WebSocket;
  });
  const factory = yield* factoryEffect.pipe(Effect.provide(constructorLayer));
  return { factory, firstSocket, sockets };
});

const awaitSocket = Effect.fn("TestRpcSessionFactory.awaitSocket")(function* (
  socket: Promise<TestWebSocket>,
) {
  return yield* Effect.promise(() => socket);
});

const awaitRequest = Effect.fn("TestRpcSessionFactory.awaitRequest")(function* (
  socket: TestWebSocket,
  index = 0,
) {
  return decodeRpcRequest(decodeJson(yield* Effect.promise(() => socket.awaitSent(index))));
});

const completeInitialConfig = Effect.fn("TestRpcSessionFactory.completeInitialConfig")(function* (
  socket: TestWebSocket,
  config: unknown = COMPATIBLE_SERVER_CONFIG,
) {
  const request = yield* awaitRequest(socket);
  expect(request).toMatchObject({
    _tag: "Request",
    tag: WS_METHODS.serverGetConfig,
    payload: {},
  });
  socket.serverMessage(
    encodeJson({
      _tag: "Exit",
      requestId: request.id,
      exit: {
        _tag: "Success",
        value: config,
      },
    }),
  );
});

const failInitialConfig = Effect.fn("TestRpcSessionFactory.failInitialConfig")(function* (
  socket: TestWebSocket,
  error: EnvironmentAuthorizationError | KeybindingsConfigError | ServerSettingsError,
) {
  const request = yield* awaitRequest(socket);
  socket.serverMessage(
    encodeJson({
      _tag: "Exit",
      requestId: request.id,
      exit: encodeServerGetConfigExit(Exit.fail(error)),
    }),
  );
});

const connectReady = Effect.fn("TestRpcSessionFactory.connectReady")(function* (
  factory: RpcSession.RpcSessionFactory["Service"],
  firstSocket: Promise<TestWebSocket>,
  config: unknown = COMPATIBLE_SERVER_CONFIG,
  options?: {
    readonly requireRpcCompatibility?: boolean;
  },
) {
  const session = yield* factory.connect(PREPARED, options);
  const readyFiber = yield* Effect.forkChild(session.ready);
  const socket = yield* awaitSocket(firstSocket);
  socket.open();
  yield* completeInitialConfig(socket, config);
  const readyExit = yield* Fiber.join(readyFiber).pipe(Effect.exit);
  return { readyExit, session, socket };
});

const completeConnect = Effect.fn("TestRpcSessionFactory.completeConnect")(function* (
  connect: ReturnType<RpcSession.RpcSessionFactory["Service"]["connect"]>,
  firstSocket: Promise<TestWebSocket>,
  config: unknown,
) {
  const connectFiber = yield* Effect.forkChild(connect.pipe(Effect.exit));
  const socket = yield* awaitSocket(firstSocket);
  socket.open();
  yield* completeInitialConfig(socket, config);
  const connectExit = yield* Fiber.join(connectFiber);
  return { connectExit, socket };
});

const failureFromExit = <A, E>(exit: Exit.Exit<A, E>): unknown => {
  if (Exit.isSuccess(exit)) {
    return undefined;
  }
  const found = Cause.findError(exit.cause);
  return Result.isSuccess(found) ? found.success : undefined;
};

const awaitSentMessage = Effect.fn("TestRpcSessionFactory.awaitSentMessage")(function* (
  socket: TestWebSocket,
  index: number,
) {
  return decodeJson(yield* Effect.promise(() => socket.awaitSent(index)));
});

describe("RpcSessionFactory", () => {
  it.effect("owns one scoped websocket attempt and exposes readiness and closure", () =>
    Effect.gen(function* () {
      const { factory, firstSocket, sockets } = yield* makeFactory();
      const session = yield* factory.connect(PREPARED);
      const readyFiber = yield* Effect.forkChild(session.ready);
      const socket = yield* awaitSocket(firstSocket);

      expect(socket.url).toBe(PREPARED.socketUrl);
      socket.open();
      yield* completeInitialConfig(socket);
      yield* Fiber.join(readyFiber);

      const config = yield* session.initialConfig;
      expect(config).toEqual(COMPATIBLE_SERVER_CONFIG_VALUE);
      expect(socket.sent).toHaveLength(1);

      const probeFiber = yield* Effect.forkChild(session.probe);
      const probeRequest = yield* awaitRequest(socket, 1);
      expect(probeRequest).toMatchObject({
        _tag: "Request",
        tag: WS_METHODS.serverProbe,
        payload: {},
      });
      socket.serverMessage(
        encodeJson({
          _tag: "Exit",
          requestId: probeRequest.id,
          exit: {
            _tag: "Success",
            value: {},
          },
        }),
      );
      yield* Fiber.join(probeFiber);

      expect(socket.sent.map((request) => decodeRpcRequest(decodeJson(request)).tag)).toEqual([
        WS_METHODS.serverGetConfig,
        WS_METHODS.serverProbe,
      ]);

      socket.close(1012, "service restart");
      const error = yield* Effect.flip(session.closed);

      expect(error).toBeInstanceOf(ConnectionTransientError);
      expect(error).toMatchObject({
        reason: "transport",
        message: "Test environment disconnected.",
      });
      expect(sockets).toHaveLength(1);
    }),
  );

  it.effect("closes the websocket when the session scope is released", () =>
    Effect.gen(function* () {
      const { factory, firstSocket, sockets } = yield* makeFactory();

      yield* Effect.scoped(
        Effect.gen(function* () {
          const session = yield* factory.connect(PREPARED);
          const readyFiber = yield* Effect.forkChild(session.ready);
          const socket = yield* awaitSocket(firstSocket);
          socket.open();
          yield* completeInitialConfig(socket);
          yield* Fiber.join(readyFiber);
        }),
      );

      expect(sockets[0]?.readyState).toBe(TestWebSocket.CLOSED);
    }),
  );

  it.effect("uses the legacy config RPC for probes when the server lacks the capability", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { factory, firstSocket } = yield* makeFactory();
        const session = yield* factory.connect(PREPARED);
        const readyFiber = yield* Effect.forkChild(session.ready);
        const socket = yield* awaitSocket(firstSocket);

        socket.open();
        yield* completeInitialConfig(socket, LEGACY_SERVER_CONFIG);
        yield* Fiber.join(readyFiber);

        const probeFiber = yield* Effect.forkChild(session.probe);
        const probeRequest = yield* awaitRequest(socket, 1);
        expect(probeRequest).toMatchObject({
          _tag: "Request",
          tag: WS_METHODS.serverGetConfig,
          payload: {},
        });
        socket.serverMessage(
          encodeJson({
            _tag: "Exit",
            requestId: probeRequest.id,
            exit: {
              _tag: "Success",
              value: LEGACY_SERVER_CONFIG,
            },
          }),
        );
        yield* Fiber.join(probeFiber);

        expect(socket.sent.map((request) => decodeRpcRequest(decodeJson(request)).tag)).toEqual([
          WS_METHODS.serverGetConfig,
          WS_METHODS.serverGetConfig,
        ]);
      }),
    ),
  );

  it.effect("fails readiness when the websocket never opens", () =>
    Effect.gen(function* () {
      const { factory, firstSocket, sockets } = yield* makeFactory();

      const error = yield* Effect.scoped(
        Effect.gen(function* () {
          const session = yield* factory.connect(PREPARED);
          const readyFiber = yield* Effect.forkChild(Effect.flip(session.ready));
          yield* awaitSocket(firstSocket);

          yield* TestClock.adjust("15 seconds");
          return yield* Fiber.join(readyFiber);
        }),
      );

      expect(error).toBeInstanceOf(ConnectionTransientError);
      expect(error).toMatchObject({
        reason: "transport",
        message: "Test environment could not establish a WebSocket connection.",
      });
      expect(sockets[0]?.readyState).toBe(TestWebSocket.CLOSED);
    }).pipe(Effect.provide(TestClock.layer())),
  );

  it.effect("allows first-party sessions to connect when the RPC fingerprint differs", () =>
    Effect.gen(function* () {
      const { factory, firstSocket } = yield* makeFactory();
      const mismatch = {
        ...COMPATIBLE_SERVER_CONFIG,
        environment: {
          ...COMPATIBLE_SERVER_CONFIG.environment,
          rpc: {
            ...T3_RPC_COMPATIBILITY,
            contractFingerprint: "different-contracts",
          },
        },
      };
      const { readyExit } = yield* connectReady(factory, firstSocket, mismatch);

      expect(Exit.isSuccess(readyExit)).toBe(true);
    }),
  );

  it.effect("fails closed when a strict session sees a different RPC fingerprint", () =>
    Effect.gen(function* () {
      const { factory, firstSocket } = yield* makeFactory();
      const mismatch = {
        ...COMPATIBLE_SERVER_CONFIG,
        environment: {
          ...COMPATIBLE_SERVER_CONFIG.environment,
          rpc: {
            ...T3_RPC_COMPATIBILITY,
            contractFingerprint: "different-contracts",
          },
        },
      };
      const { connectExit } = yield* completeConnect(
        factory.connect(PREPARED, { requireRpcCompatibility: true }),
        firstSocket,
        mismatch,
      );

      expect(Exit.isFailure(connectExit)).toBe(true);
      expect(failureFromExit(connectExit)).toMatchObject({
        _tag: "ConnectionBlockedError",
        reason: "version_mismatch",
        message: "Test environment uses incompatible T3 RPC contracts.",
      });
    }),
  );

  it.effect("fails closed when a strict session sees no RPC compatibility descriptor", () =>
    Effect.gen(function* () {
      const { factory, firstSocket } = yield* makeFactory();
      const { connectExit } = yield* completeConnect(
        factory.connect(PREPARED, { requireRpcCompatibility: true }),
        firstSocket,
        ENCODED_SERVER_CONFIG,
      );

      expect(Exit.isFailure(connectExit)).toBe(true);
      expect(failureFromExit(connectExit)).toMatchObject({
        _tag: "ConnectionBlockedError",
        reason: "version_mismatch",
      });
    }),
  );

  it.effect("returns an exported runtime-client session after a compatible handshake", () =>
    Effect.gen(function* () {
      const { factory, firstSocket } = yield* makeFactory(RuntimeClient.makeRpcSessionFactory);
      const { connectExit } = yield* completeConnect(
        factory.connect(PREPARED),
        firstSocket,
        COMPATIBLE_SERVER_CONFIG,
      );

      expect(Exit.isSuccess(connectExit)).toBe(true);
      if (Exit.isSuccess(connectExit)) {
        expect(yield* connectExit.value.initialConfig).toEqual(COMPATIBLE_SERVER_CONFIG_VALUE);
      }
    }),
  );

  it.effect("rejects an incompatible server before returning a runtime-client session", () =>
    Effect.gen(function* () {
      const { factory, firstSocket } = yield* makeFactory(RuntimeClient.makeRpcSessionFactory);
      yield* Effect.forkChild(
        Effect.gen(function* () {
          const socket = yield* awaitSocket(firstSocket);
          socket.open();
          yield* completeInitialConfig(socket, ENCODED_SERVER_CONFIG);
        }),
      );

      const connectExit = yield* factory.connect(PREPARED).pipe(Effect.exit);

      expect(Exit.isFailure(connectExit)).toBe(true);
      expect(failureFromExit(connectExit)).toMatchObject({
        _tag: "ConnectionBlockedError",
        reason: "version_mismatch",
      });
    }),
  );

  it.effect("fails strict connect from authoritative disconnect when the socket never opens", () =>
    Effect.gen(function* () {
      const { factory, firstSocket, sockets } = yield* makeFactory(
        RuntimeClient.makeRpcSessionFactory,
      );

      const error = yield* Effect.scoped(
        Effect.gen(function* () {
          const connectFiber = yield* Effect.forkChild(Effect.flip(factory.connect(PREPARED)));
          yield* awaitSocket(firstSocket);

          yield* TestClock.adjust("15 seconds");
          return yield* Fiber.join(connectFiber);
        }),
      );

      expect(error).toBeInstanceOf(ConnectionTransientError);
      expect(error).toMatchObject({
        reason: "transport",
      });
      expect(error.message).toBe("Test environment could not establish a WebSocket connection.");
      expect(sockets[0]?.readyState).toBe(TestWebSocket.CLOSED);
    }).pipe(Effect.provide(TestClock.layer())),
  );

  it("does not export a strict layer or the permissive RpcSessionFactory tag", () => {
    expect(RuntimeClient).not.toHaveProperty("rpcSessionFactoryLayer");
    expect(RuntimeClient).not.toHaveProperty("RpcSessionFactory");
  });

  it.effect("preserves actionable authorization errors from server.getConfig", () =>
    Effect.gen(function* () {
      const { factory, firstSocket } = yield* makeFactory();
      const session = yield* factory.connect(PREPARED);
      const readyFiber = yield* Effect.forkChild(session.ready.pipe(Effect.exit));
      const socket = yield* awaitSocket(firstSocket);
      socket.open();
      yield* failInitialConfig(
        socket,
        new EnvironmentAuthorizationError({
          message: "The authenticated token is missing required scope: orchestration:read.",
          requiredScope: "orchestration:read",
        }),
      );
      const error = failureFromExit(yield* Fiber.join(readyFiber));

      expect(error).toMatchObject({
        _tag: "ConnectionBlockedError",
        reason: "permission",
        message: "The authenticated token is missing required scope: orchestration:read.",
      });
    }),
  );

  it.effect("preserves actionable keybindings parse errors from server.getConfig", () =>
    Effect.gen(function* () {
      const { factory, firstSocket } = yield* makeFactory();
      const session = yield* factory.connect(PREPARED);
      const readyFiber = yield* Effect.forkChild(session.ready.pipe(Effect.exit));
      const socket = yield* awaitSocket(firstSocket);
      socket.open();
      yield* failInitialConfig(
        socket,
        new KeybindingsConfigError({
          configPath: "/tmp/workspace/keybindings.json",
          detail: "Unexpected token at line 4.",
        }),
      );
      const error = failureFromExit(yield* Fiber.join(readyFiber));

      expect(error).toMatchObject({
        _tag: "ConnectionTransientError",
        reason: "remote-unavailable",
        message:
          "Unable to parse keybindings config at /tmp/workspace/keybindings.json: Unexpected token at line 4.",
      });
    }),
  );

  it.effect("preserves actionable settings errors from server.getConfig", () =>
    Effect.gen(function* () {
      const { factory, firstSocket } = yield* makeFactory();
      const session = yield* factory.connect(PREPARED);
      const readyFiber = yield* Effect.forkChild(session.ready.pipe(Effect.exit));
      const socket = yield* awaitSocket(firstSocket);
      socket.open();
      yield* failInitialConfig(
        socket,
        new ServerSettingsError({
          settingsPath: "/tmp/workspace/settings.json",
          operation: "read-file",
          cause: new Error("read failed"),
        }),
      );
      const error = failureFromExit(yield* Fiber.join(readyFiber));

      expect(error).toMatchObject({
        _tag: "ConnectionTransientError",
        reason: "remote-unavailable",
        message: "Server settings read-file failed at /tmp/workspace/settings.json.",
      });
    }),
  );

  it.effect("does not include connection credentials in version mismatch errors", () =>
    Effect.gen(function* () {
      const credential = "credential-that-must-not-escape";
      const prepared = {
        ...PREPARED,
        socketUrl: `wss://environment.example.test/ws?wsTicket=${credential}`,
      };
      const { factory, firstSocket } = yield* makeFactory();
      const { connectExit } = yield* completeConnect(
        factory.connect(prepared, { requireRpcCompatibility: true }),
        firstSocket,
        {
          ...COMPATIBLE_SERVER_CONFIG,
          environment: {
            ...COMPATIBLE_SERVER_CONFIG.environment,
            rpc: {
              ...T3_RPC_COMPATIBILITY,
              contractFingerprint: credential,
            },
          },
        },
      );
      const error = failureFromExit(connectExit);

      expect(Exit.isFailure(connectExit)).toBe(true);
      expect(`${String(error)} ${encodeJson(error)}`).not.toContain(credential);
    }),
  );

  it.effect("exchanges Effect RPC heartbeat frames without exposing them as requests", () =>
    Effect.gen(function* () {
      const { factory, firstSocket } = yield* makeFactory();
      const { readyExit, socket } = yield* connectReady(factory, firstSocket);
      expect(Exit.isSuccess(readyExit)).toBe(true);

      yield* TestClock.adjust("5 seconds");
      const firstPing = yield* awaitSentMessage(socket, 1);
      expect(firstPing).toEqual({ _tag: "Ping" });

      socket.serverMessage(encodeJson({ _tag: "Pong" }));
      yield* TestClock.adjust("5 seconds");
      const secondPing = yield* awaitSentMessage(socket, 2);
      expect(secondPing).toEqual({ _tag: "Ping" });
    }).pipe(Effect.provide(TestClock.layer())),
  );

  it.effect("logs and maps an Effect RPC Defect without exposing credentials", () => {
    const logs: Array<{
      readonly annotations: Readonly<Record<string, unknown>>;
      readonly message: string;
    }> = [];
    const logger = Logger.make(({ fiber, message }) => {
      logs.push({
        annotations: fiber.getRef(References.CurrentLogAnnotations),
        message: String(message),
      });
    });

    return Effect.gen(function* () {
      const credential = "defect-secret-that-must-not-escape";
      const { factory, firstSocket } = yield* makeFactory();
      const { readyExit, session, socket } = yield* connectReady(factory, firstSocket);
      expect(Exit.isSuccess(readyExit)).toBe(true);

      const probeFiber = yield* Effect.forkChild(session.probe.pipe(Effect.exit));
      yield* awaitRequest(socket, 1);
      socket.serverMessage(
        encodeJson({
          _tag: "Defect",
          defect: encodeDefect(new Error(credential)),
        }),
      );
      const probeExit = yield* Fiber.join(probeFiber);
      const error = failureFromExit(probeExit);

      expect(error).toMatchObject({
        _tag: "ConnectionTransientError",
        reason: "transport",
        message: "Test environment RPC protocol failed.",
      });
      expect(`${String(error)} ${encodeJson(error)}`).not.toContain(credential);
      const defectLog = logs.find((entry) => entry.message === "RPC session defect.");
      expect(defectLog?.annotations).toMatchObject({
        errorType: "error",
        errorName: "Error",
      });
      expect(encodeJson(logs)).not.toContain(credential);
    }).pipe(Effect.provide(Logger.layer([logger], { mergeWithExisting: false })));
  });

  it.effect("maps a ClientProtocolError to a credential-free connection error", () =>
    Effect.gen(function* () {
      const credential = "malformed-frame-secret-that-must-not-escape";
      const { factory, firstSocket } = yield* makeFactory();
      const { readyExit, session, socket } = yield* connectReady(factory, firstSocket);
      expect(Exit.isSuccess(readyExit)).toBe(true);

      const probeFiber = yield* Effect.forkChild(session.probe.pipe(Effect.exit));
      yield* awaitRequest(socket, 1);
      socket.serverMessage(`not-json:${credential}`);
      const probeExit = yield* Fiber.join(probeFiber);
      const error = failureFromExit(probeExit);

      expect(error).toMatchObject({
        _tag: "ConnectionTransientError",
        reason: "transport",
        message: "Test environment RPC protocol failed.",
      });
      expect(`${String(error)} ${encodeJson(error)}`).not.toContain(credential);
    }),
  );

  it.effect("ACKs a Chunk only after the bounded stream queue accepts it", () =>
    Effect.gen(function* () {
      const { factory, firstSocket } = yield* makeFactory();
      const { readyExit, session, socket } = yield* connectReady(factory, firstSocket);
      expect(Exit.isSuccess(readyExit)).toBe(true);

      yield* Effect.scoped(
        Effect.gen(function* () {
          const queue = yield* session.client[WS_METHODS.subscribeDiscoveredLocalServers](
            {},
            { asQueue: true, streamBufferSize: 1 },
          );
          const streamRequest = yield* awaitRequest(socket, 1);
          const first = { servers: [], scannedAt: "first" };
          const second = { servers: [], scannedAt: "second" };
          socket.beforeSend = (index) => {
            if (index !== 3) {
              return;
            }
            socket.trace.push({
              _tag: "QueueStateBeforeSend",
              buffered: MutableList.toArray(queue.messages),
              index,
            });
          };

          socket.serverMessage(
            encodeJson({
              _tag: "Chunk",
              requestId: streamRequest.id,
              values: [first],
            }),
          );
          expect(yield* awaitSentMessage(socket, 2)).toEqual({
            _tag: "Ack",
            requestId: streamRequest.id,
          });

          socket.serverMessage(
            encodeJson({
              _tag: "Chunk",
              requestId: streamRequest.id,
              values: [second],
            }),
          );
          expect(yield* Queue.take(queue)).toEqual(first);
          expect(yield* awaitSentMessage(socket, 3)).toEqual({
            _tag: "Ack",
            requestId: streamRequest.id,
          });
          const queueStateIndex = socket.trace.findIndex(
            (event) => event._tag === "QueueStateBeforeSend" && event.index === 3,
          );
          const secondAckIndex = socket.trace.findIndex(
            (event) => event._tag === "Send" && event.index === 3,
          );
          expect(socket.trace[queueStateIndex]).toEqual({
            _tag: "QueueStateBeforeSend",
            buffered: [second],
            index: 3,
          });
          expect(secondAckIndex).toBeGreaterThan(queueStateIndex);
          expect(yield* Queue.take(queue)).toEqual(second);
        }),
      );
    }),
  );

  it.effect("sends Interrupt when a stream consumer scope is cancelled", () =>
    Effect.gen(function* () {
      const { factory, firstSocket } = yield* makeFactory();
      const { readyExit, session, socket } = yield* connectReady(factory, firstSocket);
      expect(Exit.isSuccess(readyExit)).toBe(true);

      const streamRequest = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* session.client[WS_METHODS.subscribeDiscoveredLocalServers](
            {},
            { asQueue: true, streamBufferSize: 1 },
          );
          return yield* awaitRequest(socket, 1);
        }),
      );

      expect(yield* awaitSentMessage(socket, 2)).toMatchObject({
        _tag: "Interrupt",
        requestId: streamRequest.id,
      });
    }),
  );
});
