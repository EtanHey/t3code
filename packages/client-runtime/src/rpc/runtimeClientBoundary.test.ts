import { describe, expect, it } from "vite-plus/test";

import { EnvironmentId, T3_RPC_COMPATIBILITY, WS_METHODS } from "@t3tools/contracts/runtime-client";

describe("runtime-client RPC boundary", () => {
  it("provides the contracts needed by the canonical RPC session", () => {
    expect(EnvironmentId).toBeDefined();
    expect(T3_RPC_COMPATIBILITY.protocol).toBe("effect-rpc");
    expect(WS_METHODS.serverGetConfig).toBeDefined();
  });
});
