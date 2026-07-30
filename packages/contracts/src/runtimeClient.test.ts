import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import { EnvironmentId, type ServerConfig } from "./runtimeClient.ts";

const decodeEnvironmentId = Schema.decodeUnknownSync(EnvironmentId);

describe("runtime-client contract boundary", () => {
  it("exports the environment identifier schema and server configuration type", () => {
    const environmentId = decodeEnvironmentId("environment-1");
    const config: Pick<ServerConfig["environment"], "environmentId"> = { environmentId };

    expect(config.environmentId).toBe(environmentId);
  });
});
