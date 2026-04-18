import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";

import { afterEach, describe, it } from "vitest";

import {
  codexAppServerLaunchArgs,
  isCodexAppServerBridgeBinary,
  probeCodexDiscovery,
} from "./codexAppServer.ts";

const tempDirs: string[] = [];

async function makeBridgeBinary(): Promise<string> {
  const dir = await mkdtemp(path.join(os.tmpdir(), "t3-cmuxlayer-bridge-"));
  tempDirs.push(dir);
  const binaryPath = path.join(dir, "cmuxlayer-app-server");
  const scriptPath = path.join(dir, "bridge.js");

  const script = `#!/usr/bin/env node
const readline = require("node:readline");

if (process.argv.slice(2).length > 0) {
  console.error("unexpected args: " + process.argv.slice(2).join(" "));
  process.exit(64);
}

const rl = readline.createInterface({
  input: process.stdin,
  crlfDelay: Infinity,
});

rl.on("line", (line) => {
  if (!line.trim()) return;
  const message = JSON.parse(line);

  if (message.method === "initialize") {
    process.stdout.write(JSON.stringify({ id: message.id, result: {} }) + "\\n");
    return;
  }

  if (message.method === "skills/list") {
    process.stdout.write(
      JSON.stringify({
        id: message.id,
        error: { message: "Method not found: skills/list" },
      }) + "\\n",
    );
    return;
  }

  if (message.method === "account/read") {
    process.stdout.write(
      JSON.stringify({
        id: message.id,
        result: {
          account: {
            type: "unknown",
            planType: null,
            sparkEnabled: true,
          },
        },
      }) + "\\n",
    );
  }
});
`;

  const wrapper = `#!/bin/sh
exec node ${JSON.stringify(scriptPath)} "$@"
`;

  await writeFile(scriptPath, script, "utf8");
  await writeFile(binaryPath, wrapper, "utf8");
  await chmod(scriptPath, 0o755);
  await chmod(binaryPath, 0o755);

  return binaryPath;
}

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

describe("codex app-server bridge helpers", () => {
  it("treats bridge binaries as already speaking the app-server protocol", () => {
    assert.equal(isCodexAppServerBridgeBinary("codex"), false);
    assert.equal(isCodexAppServerBridgeBinary("/usr/local/bin/cmuxlayer-app-server"), true);
    assert.deepEqual(codexAppServerLaunchArgs("codex"), ["app-server"]);
    assert.deepEqual(codexAppServerLaunchArgs("/usr/local/bin/cmuxlayer-app-server"), []);
  });

  it("tolerates bridge discovery when skills/list is unavailable", async () => {
    const binaryPath = await makeBridgeBinary();

    const snapshot = await probeCodexDiscovery({
      binaryPath,
      cwd: process.cwd(),
    });

    assert.deepEqual(snapshot.skills, []);
    assert.equal(snapshot.account.sparkEnabled, true);
  });
});
