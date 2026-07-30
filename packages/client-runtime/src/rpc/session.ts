import {
  T3_RPC_COMPATIBILITY,
  type EnvironmentId,
  type RpcCompatibilityDescriptor,
  type ServerConfig,
  WS_METHODS,
} from "@t3tools/contracts/runtime-client";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import type * as Scope from "effect/Scope";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as Socket from "effect/unstable/socket/Socket";

import { makeWsRpcProtocolClient, type WsRpcProtocolClient } from "./protocol.ts";
import type { ConnectionAttemptError, ConnectionTransientError } from "../connection/model.ts";
import {
  ConnectionBlockedError,
  ConnectionTransientError as ConnectionTransientErrorClass,
} from "../connection/model.ts";
import { safeErrorLogAttributes } from "../errors/safeLog.ts";

const SOCKET_OPEN_TIMEOUT = "15 seconds";

export interface RpcSession {
  readonly client: WsRpcProtocolClient;
  readonly initialConfig: Effect.Effect<ServerConfig, ConnectionAttemptError>;
  readonly ready: Effect.Effect<void, ConnectionAttemptError>;
  readonly probe: Effect.Effect<void, ConnectionAttemptError>;
  readonly closed: Effect.Effect<never, ConnectionTransientError>;
}

/** Deliberately excludes HTTP authorization and connection target metadata. */
export interface RpcSessionConnection {
  readonly environmentId: EnvironmentId;
  readonly label: string;
  readonly socketUrl: string;
}

export interface RpcSessionConnectOptions {
  readonly requireRpcCompatibility?: boolean;
}

export class RpcSessionFactory extends Context.Service<
  RpcSessionFactory,
  {
    readonly connect: (
      connection: RpcSessionConnection,
      options?: RpcSessionConnectOptions,
    ) => Effect.Effect<RpcSession, ConnectionAttemptError, Scope.Scope>;
  }
>()("@t3tools/client-runtime/rpc/session/RpcSessionFactory") {}

type InitialConfigError = Effect.Error<
  ReturnType<WsRpcProtocolClient[typeof WS_METHODS.serverGetConfig]>
>;
type ProbeError = Effect.Error<ReturnType<WsRpcProtocolClient[typeof WS_METHODS.serverProbe]>>;

function protocolFailure(label: string): ConnectionTransientError {
  return new ConnectionTransientErrorClass({
    reason: "transport",
    detail: `${label} RPC protocol failed.`,
  });
}

function mapSessionRpcError(
  error: InitialConfigError | ProbeError,
  label: string,
): ConnectionAttemptError {
  switch (error._tag) {
    case "EnvironmentAuthorizationError":
      return new ConnectionBlockedError({
        reason: "permission",
        detail: error.message,
      });
    case "KeybindingsConfigParseError":
    case "ServerSettingsError":
      return new ConnectionTransientErrorClass({
        reason: "remote-unavailable",
        detail: error.message,
      });
    case "RpcClientError":
      return protocolFailure(label);
  }
}

function catchProtocolDefects<A, E, R>(
  effect: Effect.Effect<A, E, R>,
  connection: RpcSessionConnection,
): Effect.Effect<A, E | ConnectionTransientError, R> {
  return effect.pipe(
    Effect.tapDefect((defect) =>
      Effect.logError("RPC session defect.").pipe(
        Effect.annotateLogs({
          "connection.environment.id": connection.environmentId,
          "connection.environment.label": connection.label,
          ...safeErrorLogAttributes(defect),
        }),
      ),
    ),
    Effect.catchDefect(() => Effect.fail(protocolFailure(connection.label))),
  );
}

function isCompatibleRpcDescriptor(descriptor: RpcCompatibilityDescriptor | undefined): boolean {
  return (
    descriptor?.protocol === T3_RPC_COMPATIBILITY.protocol &&
    descriptor.transport === T3_RPC_COMPATIBILITY.transport &&
    descriptor.serialization === T3_RPC_COMPATIBILITY.serialization &&
    descriptor.contractFingerprint === T3_RPC_COMPATIBILITY.contractFingerprint
  );
}

const validateRpcCompatibility = Effect.fn("RpcSession.validateRpcCompatibility")(function* (
  config: ServerConfig,
  label: string,
) {
  if (isCompatibleRpcDescriptor(config.environment.rpc)) {
    return config;
  }
  return yield* new ConnectionBlockedError({
    reason: "version_mismatch",
    detail: `${label} uses incompatible T3 RPC contracts.`,
  });
});

export const make = Effect.gen(function* () {
  const webSocketConstructor = yield* Socket.WebSocketConstructor;

  const connect = Effect.fnUntraced(function* (
    connection: RpcSessionConnection,
    options?: RpcSessionConnectOptions,
  ) {
    yield* Effect.annotateCurrentSpan({
      "connection.environment.id": connection.environmentId,
    });

    const connected = yield* Deferred.make<void>();
    const disconnected = yield* Deferred.make<never, ConnectionTransientError>();
    const hooks = RpcClient.ConnectionHooks.of({
      onConnect: Deferred.succeed(connected, undefined).pipe(Effect.asVoid),
      onDisconnect: Deferred.isDone(connected).pipe(
        Effect.flatMap((wasConnected) =>
          Deferred.fail(
            disconnected,
            new ConnectionTransientErrorClass({
              reason: "transport",
              detail: wasConnected
                ? `${connection.label} disconnected.`
                : `${connection.label} could not establish a WebSocket connection.`,
            }),
          ),
        ),
        Effect.asVoid,
      ),
    });
    const socketLayer = Socket.layerWebSocket(connection.socketUrl, {
      openTimeout: SOCKET_OPEN_TIMEOUT,
    }).pipe(Layer.provide(Layer.succeed(Socket.WebSocketConstructor, webSocketConstructor)));
    const protocolLayer = Layer.effect(
      RpcClient.Protocol,
      RpcClient.makeProtocolSocket({
        retryTransientErrors: false,
        retryPolicy: Schedule.recurs(0),
      }),
    ).pipe(
      Layer.provide(
        Layer.mergeAll(
          socketLayer,
          RpcSerialization.layerJson,
          Layer.succeed(RpcClient.ConnectionHooks, hooks),
        ),
      ),
    );
    const protocolContext = yield* Layer.build(protocolLayer).pipe(
      Effect.withSpan("environment.websocket.connect"),
    );
    const client = yield* makeWsRpcProtocolClient.pipe(Effect.provide(protocolContext));
    const mapRpcError = (error: InitialConfigError | ProbeError) =>
      mapSessionRpcError(error, connection.label);
    const initialConfig = yield* Effect.cached(
      client[WS_METHODS.serverGetConfig]({}).pipe(
        Effect.mapError(mapRpcError),
        (effect) => catchProtocolDefects(effect, connection),
        Effect.flatMap((config) =>
          options?.requireRpcCompatibility === true
            ? validateRpcCompatibility(config, connection.label)
            : Effect.succeed(config),
        ),
        Effect.withSpan("environment.initialSync"),
      ),
    );
    const probe = Effect.gen(function* () {
      const config = yield* initialConfig;
      if (config.environment.capabilities.connectionProbe === true) {
        yield* client[WS_METHODS.serverProbe]({}).pipe(Effect.mapError(mapRpcError));
        return;
      }
      yield* client[WS_METHODS.serverGetConfig]({}).pipe(Effect.mapError(mapRpcError));
    }).pipe(
      (effect) => catchProtocolDefects(effect, connection),
      Effect.asVoid,
      Effect.withSpan("clientRuntime.connection.rpcSession.probe"),
    );

    if (options?.requireRpcCompatibility === true) {
      yield* Deferred.await(connected).pipe(
        Effect.andThen(initialConfig),
        Effect.raceFirst(Deferred.await(disconnected)),
      );
    }

    return {
      client,
      initialConfig,
      ready: Deferred.await(connected).pipe(
        Effect.andThen(initialConfig),
        Effect.asVoid,
        Effect.raceFirst(Deferred.await(disconnected)),
      ),
      probe,
      closed: Deferred.await(disconnected),
    } satisfies RpcSession;
  });

  return RpcSessionFactory.of({ connect });
});

export const layer = Layer.effect(RpcSessionFactory, make);
