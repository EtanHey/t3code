export * from "@t3tools/contracts/runtime-client";

import * as Effect from "effect/Effect";

export { makeWsRpcProtocolClient, type WsRpcProtocolClient } from "./protocol.ts";
export { applyShellStreamEvent } from "../state/shellReducer.ts";
export { applyThreadDetailEvent, type ThreadDetailReducerResult } from "../state/threadReducer.ts";
import {
  make as makeSharedRpcSessionFactory,
  type RpcSessionConnection,
  type RpcSessionFactory,
} from "./session.ts";

export { type RpcSession, type RpcSessionConnection } from "./session.ts";

export interface RuntimeClientRpcSessionFactory {
  readonly connect: (
    connection: RpcSessionConnection,
  ) => ReturnType<RpcSessionFactory["Service"]["connect"]>;
}

/**
 * Builds the version-locked session factory exposed to external runtime
 * clients. The returned connect operation has no policy override: every call
 * validates compatibility before returning a usable session.
 */
export const makeRpcSessionFactory = makeSharedRpcSessionFactory.pipe(
  Effect.map(
    (factory): RuntimeClientRpcSessionFactory => ({
      connect: (connection) => factory.connect(connection, { requireRpcCompatibility: true }),
    }),
  ),
);
