import { WsRpcGroup } from "@t3tools/contracts/runtime-client";
import type * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";
import * as Rpc from "effect/unstable/rpc/Rpc";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError";
import * as RpcGroup from "effect/unstable/rpc/RpcGroup";

type WsRpcs = RpcGroup.Rpcs<typeof WsRpcGroup>;
export type WsRpcProtocolClient = RpcClient.RpcClient<WsRpcs, RpcClientError>;

export const makeWsRpcProtocolClient: Effect.Effect<
  WsRpcProtocolClient,
  never,
  RpcClient.Protocol | Rpc.MiddlewareClient<WsRpcs> | Scope.Scope
> = RpcClient.make(WsRpcGroup);
