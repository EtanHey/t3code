import { describe, expect, it } from "vite-plus/test";
import effectPackage from "effect/package.json" with { type: "json" };

import * as RpcCompatibility from "./rpcCompatibility.ts";
import * as RuntimeClient from "./runtimeClient.ts";

describe("T3 RPC compatibility", () => {
  it("pins the fingerprint to the installed Effect runtime", () => {
    expect(RpcCompatibility.T3_RPC_EFFECT_VERSION).toBe(effectPackage.version);
    expect(RpcCompatibility.T3_RPC_CONTRACT_FINGERPRINT).toBe(
      `t3-rpc-v1:effect@${effectPackage.version}:json-websocket`,
    );
  });

  it("exports the pinned Effect version through the runtime-client boundary", () => {
    expect(RuntimeClient).toHaveProperty("T3_RPC_EFFECT_VERSION", effectPackage.version);
  });
});
