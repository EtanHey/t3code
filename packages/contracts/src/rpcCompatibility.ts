import * as Schema from "effect/Schema";

import { TrimmedNonEmptyString } from "./baseSchemas.ts";

export const T3_RPC_PROTOCOL = "effect-rpc" as const;
export const T3_RPC_TRANSPORT = "websocket" as const;
export const T3_RPC_SERIALIZATION = "json" as const;
export const T3_RPC_EFFECT_VERSION = "4.0.0-beta.102" as const;
export const T3_RPC_CONTRACT_FINGERPRINT =
  `t3-rpc-v1:effect@${T3_RPC_EFFECT_VERSION}:json-websocket` as const;

/**
 * Runtime identity for the supported WebSocket RPC seam. The fields accept
 * unknown future values so a client can decode an older or newer descriptor
 * and return version_mismatch instead of failing during ServerConfig decoding.
 */
export const RpcCompatibilityDescriptor = Schema.Struct({
  protocol: TrimmedNonEmptyString,
  transport: TrimmedNonEmptyString,
  serialization: TrimmedNonEmptyString,
  contractFingerprint: TrimmedNonEmptyString,
});
export type RpcCompatibilityDescriptor = typeof RpcCompatibilityDescriptor.Type;

export const T3_RPC_COMPATIBILITY = {
  protocol: T3_RPC_PROTOCOL,
  transport: T3_RPC_TRANSPORT,
  serialization: T3_RPC_SERIALIZATION,
  contractFingerprint: T3_RPC_CONTRACT_FINGERPRINT,
} as const satisfies RpcCompatibilityDescriptor;
