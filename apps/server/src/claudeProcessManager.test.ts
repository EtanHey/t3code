import { describe, expect, it, vi } from "vitest";
import { chmodSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { Effect, Layer, Sink, Stream } from "effect";
import { ThreadId, TurnId } from "@t3tools/contracts";
import { ChildProcessSpawner } from "effect/unstable/process";

import {
  buildClaudeCliArgs,
  checkClaudeCliHealth,
  ClaudeProcessManager,
  resolveClaudeCommandSpec,
} from "./claudeProcessManager";

const asThreadId = (value: string): ThreadId => ThreadId.makeUnsafe(value);
const asTurnId = (value: string): TurnId => TurnId.makeUnsafe(value);
const encoder = new TextEncoder();

function mockHandle(result: { stdout: string; stderr: string; code: number }) {
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(1),
    exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(result.code)),
    isRunning: Effect.succeed(false),
    kill: () => Effect.void,
    stdin: Sink.drain,
    stdout: Stream.make(encoder.encode(result.stdout)),
    stderr: Stream.make(encoder.encode(result.stderr)),
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });
}

function mockSpawnerLayer(
  handler: (args: ReadonlyArray<string>) => { stdout: string; stderr: string; code: number },
) {
  return Layer.succeed(
    ChildProcessSpawner.ChildProcessSpawner,
    ChildProcessSpawner.make((command) => {
      const currentCommand = command as unknown as { args: ReadonlyArray<string> };
      return Effect.succeed(mockHandle(handler(currentCommand.args)));
    }),
  );
}

function createFakeClaudeBinary() {
  const tmpDir = mkdtempSync(path.join(os.tmpdir(), "fake-claude-cli-"));
  const binaryPath = path.join(tmpDir, "claude");
  const logPath = path.join(tmpDir, "claude.log");

  const script = `#!/usr/bin/env node
const { appendFileSync } = require("node:fs");
const { randomUUID } = require("node:crypto");
const readline = require("node:readline");

const args = process.argv.slice(2);
const logPath = process.env.CLAUDE_FAKE_LOG_PATH;
const sessionId = process.env.CLAUDE_FAKE_SESSION_ID || randomUUID();
const authLoggedIn = process.env.CLAUDE_FAKE_AUTH !== "0";
let turnCount = 0;

function log(entry) {
  if (!logPath) return;
  appendFileSync(logPath, JSON.stringify(entry) + "\\n");
}

function usage() {
  return {
    input_tokens: 0,
    output_tokens: 0,
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: 0,
    server_tool_use: {
      web_search_requests: 0,
      web_fetch_requests: 0,
    },
    service_tier: "standard",
    cache_creation: {
      ephemeral_1h_input_tokens: 0,
      ephemeral_5m_input_tokens: 0,
    },
    inference_geo: "",
    iterations: [],
    speed: "standard",
  };
}

function emit(payload) {
  process.stdout.write(JSON.stringify(payload) + "\\n");
}

function readFlag(flag) {
  const index = args.indexOf(flag);
  if (index === -1) return undefined;
  return args[index + 1];
}

if (args.length === 1 && args[0] === "--version") {
  process.stdout.write("2.1.74 (Claude Code)\\n");
  process.exit(0);
}

if (args.length >= 2 && args[0] === "auth" && args[1] === "status") {
  process.stdout.write(JSON.stringify({ loggedIn: authLoggedIn, authMethod: "api_key" }) + "\\n");
  process.exit(authLoggedIn ? 0 : 1);
}

log({ type: "argv", args });

process.on("SIGINT", () => {
  log({ type: "signal", signal: "SIGINT" });
  emit({
    type: "system",
    subtype: "interrupted",
    session_id: sessionId,
    uuid: randomUUID(),
  });
  emit({
    type: "result",
    subtype: "error",
    is_error: true,
    duration_ms: 1,
    duration_api_ms: 1,
    num_turns: turnCount,
    result: "interrupted",
    stop_reason: "interrupted",
    session_id: sessionId,
    total_cost_usd: 0,
    usage: usage(),
    modelUsage: {},
    permission_denials: [],
    fast_mode_state: "off",
    uuid: randomUUID(),
  });
});

process.on("SIGTERM", () => {
  log({ type: "signal", signal: "SIGTERM" });
  process.exit(0);
});

emit({
  type: "system",
  subtype: "init",
  cwd: process.cwd(),
  session_id: sessionId,
  tools: ["Read", "Edit"],
  mcp_servers: [],
  model: readFlag("--model") || "claude-sonnet-4-6",
  permissionMode: readFlag("--permission-mode") || "default",
  slash_commands: [],
  apiKeySource: "ANTHROPIC_API_KEY",
  claude_code_version: "2.1.74",
  output_style: "default",
  agents: [],
  skills: [],
  plugins: [],
  uuid: randomUUID(),
  fast_mode_state: "off",
});

const rl = readline.createInterface({ input: process.stdin });
rl.on("line", (line) => {
  log({ type: "stdin", line });
  const parsed = JSON.parse(line);
  const blocks = Array.isArray(parsed?.message?.content) ? parsed.message.content : [];
  const textBlock = blocks.find((block) => block && block.type === "text");
  const text = textBlock && typeof textBlock.text === "string" ? textBlock.text : "";
  turnCount += 1;

  emit({
    type: "user",
    message: parsed.message,
    session_id: sessionId,
    parent_tool_use_id: parsed.parent_tool_use_id ?? null,
    uuid: randomUUID(),
    isReplay: true,
  });
  emit({
    type: "assistant",
    message: {
      id: randomUUID(),
      container: null,
      model: readFlag("--model") || "claude-sonnet-4-6",
      role: "assistant",
      stop_reason: "end_turn",
      stop_sequence: null,
      type: "message",
      usage: usage(),
      content: [
        {
          type: "text",
          text: "echo:" + text,
        },
      ],
      context_management: null,
    },
    parent_tool_use_id: null,
    session_id: sessionId,
    uuid: randomUUID(),
  });
  emit({
    type: "result",
    subtype: "success",
    is_error: false,
    duration_ms: 1,
    duration_api_ms: 1,
    num_turns: turnCount,
    result: "echo:" + text,
    stop_reason: "end_turn",
    session_id: sessionId,
    total_cost_usd: 0,
    usage: usage(),
    modelUsage: {},
    permission_denials: [],
    fast_mode_state: "off",
    uuid: randomUUID(),
  });
});
`;

  writeFileSync(binaryPath, script, "utf8");
  chmodSync(binaryPath, 0o755);

  return {
    binaryPath,
    logPath,
    cleanup() {
      rmSync(tmpDir, { recursive: true, force: true });
    },
  };
}

function readFakeLogEntries(logPath: string): Array<Record<string, unknown>> {
  try {
    const content = readFileSync(logPath, "utf8");
    return content
      .trim()
      .split("\n")
      .filter((line) => line.length > 0)
      .map((line) => JSON.parse(line) as Record<string, unknown>);
  } catch {
    return [];
  }
}

describe("buildClaudeCliArgs", () => {
  it("builds streaming cli args with model, permissions, and tool filters", () => {
    expect(
      buildClaudeCliArgs({
        runtimeMode: "full-access",
        interactionMode: "plan",
        model: "claude-sonnet-4-6",
        allowedTools: ["Read", "Edit"],
        disallowedTools: ["Bash"],
        resumeSessionId: "session-123",
        sessionId: "9ed0df4e-cf9b-4f37-aa75-d2af24795ef4",
        appendSystemPrompt: "Keep it brief",
      }),
    ).toEqual([
      "-p",
      "--input-format",
      "stream-json",
      "--output-format",
      "stream-json",
      "--replay-user-messages",
      "--include-partial-messages",
      "--permission-mode",
      "plan",
      "--model",
      "claude-sonnet-4-6",
      "--allowed-tools",
      "Read,Edit",
      "--disallowed-tools",
      "Bash",
      "--resume",
      "session-123",
      "--session-id",
      "9ed0df4e-cf9b-4f37-aa75-d2af24795ef4",
      "--append-system-prompt",
      "Keep it brief",
    ]);
  });
});

describe("resolveClaudeCommandSpec", () => {
  it("wraps Claude in wsl.exe on Windows when only a WSL binary is available", () => {
    const spec = resolveClaudeCommandSpec(
      {
        cwd: "C:\\Users\\etan\\repo",
        claudeArgs: ["-p", "--output-format", "stream-json"],
        wslDistro: "Ubuntu",
      },
      {
        platform: "win32",
        env: {},
        hasCommand: () => false,
        resolveWslBinaryPath: () => "/home/etan/.local/bin/claude",
        resolveWslPath: () => "/mnt/c/Users/etan/repo",
      },
    );

    expect(spec).toEqual({
      command: "wsl.exe",
      args: [
        "-d",
        "Ubuntu",
        "sh",
        "-lc",
        "cd '/mnt/c/Users/etan/repo' && exec '/home/etan/.local/bin/claude' '-p' '--output-format' 'stream-json'",
      ],
      env: {},
      shell: false,
      viaWsl: true,
      resolvedBinaryPath: "/home/etan/.local/bin/claude",
    });
  });
});

it("checkClaudeCliHealth reads version and auth from the Claude CLI", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const status = yield* checkClaudeCliHealth({
        binaryPath: "claude",
      });

      expect(status.available).toBe(true);
      expect(status.authStatus).toBe("authenticated");
      expect(status.status).toBe("ready");
      expect(status.version).toBe("2.1.74");
    }).pipe(
      Effect.provide(
        mockSpawnerLayer((args) => {
          const joined = args.join(" ");
          if (joined === "--version") {
            return { stdout: "2.1.74 (Claude Code)\n", stderr: "", code: 0 };
          }
          if (joined === "auth status") {
            return {
              stdout: JSON.stringify({ loggedIn: true, authMethod: "api_key" }),
              stderr: "",
              code: 0,
            };
          }
          throw new Error(`Unexpected args: ${joined}`);
        }),
      ),
    ),
  );
});

describe("ClaudeProcessManager", () => {
  it("starts a Claude process, streams follow-up turns over stdin, and tracks session state", async () => {
    const fake = createFakeClaudeBinary();
    const manager = new ClaudeProcessManager();
    const events: Array<{ kind: string; method: string; turnId?: TurnId }> = [];

    manager.on("event", (event) => {
      events.push({
        kind: event.kind,
        method: event.method,
        ...(event.turnId ? { turnId: event.turnId } : {}),
      });
    });

    try {
      const session = await manager.startSession({
        threadId: asThreadId("thread-1"),
        runtimeMode: "approval-required",
        model: "claude-sonnet-4-6",
        binaryPath: fake.binaryPath,
        allowedTools: ["Read", "Edit"],
        env: {
          ...process.env,
          CLAUDE_FAKE_LOG_PATH: fake.logPath,
          CLAUDE_FAKE_SESSION_ID: "session-test-1",
        },
      });

      expect(session.status).toBe("ready");
      expect(session.resumeCursor).toEqual({ sessionId: "session-test-1" });

      const firstTurn = await manager.sendTurn({
        threadId: session.threadId,
        input: "first turn",
      });
      expect(firstTurn.threadId).toBe("thread-1");

      await vi.waitFor(async () => {
        const activeSession = manager.listSessions()[0];
        expect(activeSession?.status).toBe("ready");
        expect(activeSession?.activeTurnId).toBeUndefined();
      });

      const secondTurn = await manager.sendTurn({
        threadId: session.threadId,
        input: "second turn",
      });

      await vi.waitFor(async () => {
        const logEntries = readFakeLogEntries(fake.logPath).filter(
          (entry) => entry.type === "stdin",
        );
        expect(logEntries).toHaveLength(2);
      });

      const stdinEntries = readFakeLogEntries(fake.logPath).filter(
        (entry) => entry.type === "stdin",
      );
      expect(JSON.parse(String(stdinEntries[0]?.line))).toEqual({
        type: "user",
        message: {
          role: "user",
          content: [{ type: "text", text: "first turn" }],
        },
        parent_tool_use_id: null,
      });
      expect(JSON.parse(String(stdinEntries[1]?.line))).toEqual({
        type: "user",
        message: {
          role: "user",
          content: [{ type: "text", text: "second turn" }],
        },
        parent_tool_use_id: null,
      });

      expect(events).toEqual(
        expect.arrayContaining([
          { kind: "session", method: "session/connecting" },
          { kind: "session", method: "session/ready" },
          { kind: "stream", method: "stream/system/init" },
          { kind: "stream", method: "stream/user", turnId: firstTurn.turnId },
          { kind: "stream", method: "stream/result", turnId: firstTurn.turnId },
          { kind: "stream", method: "stream/user", turnId: secondTurn.turnId },
          { kind: "stream", method: "stream/result", turnId: secondTurn.turnId },
        ]),
      );
    } finally {
      manager.stopAll();
      fake.cleanup();
    }
  });

  it("sends SIGINT to interrupt the active Claude turn", async () => {
    const fake = createFakeClaudeBinary();
    const manager = new ClaudeProcessManager();

    try {
      const session = await manager.startSession({
        threadId: asThreadId("thread-2"),
        runtimeMode: "full-access",
        binaryPath: fake.binaryPath,
        env: {
          ...process.env,
          CLAUDE_FAKE_LOG_PATH: fake.logPath,
        },
      });

      await manager.sendTurn({
        threadId: session.threadId,
        input: "interrupt me",
      });
      await manager.interruptTurn(session.threadId, asTurnId("ignored-turn"));

      await vi.waitFor(async () => {
        const signalEntry = readFakeLogEntries(fake.logPath).find(
          (entry) => entry.type === "signal" && entry.signal === "SIGINT",
        );
        expect(signalEntry).toBeDefined();
      });
    } finally {
      manager.stopAll();
      fake.cleanup();
    }
  });

  it("sends SIGTERM when stopping the session", async () => {
    const fake = createFakeClaudeBinary();
    const manager = new ClaudeProcessManager();

    try {
      const session = await manager.startSession({
        threadId: asThreadId("thread-3"),
        runtimeMode: "approval-required",
        binaryPath: fake.binaryPath,
        env: {
          ...process.env,
          CLAUDE_FAKE_LOG_PATH: fake.logPath,
        },
      });

      manager.stopSession(session.threadId);

      await vi.waitFor(async () => {
        const signalEntry = readFakeLogEntries(fake.logPath).find(
          (entry) => entry.type === "signal" && entry.signal === "SIGTERM",
        );
        expect(signalEntry).toBeDefined();
      });

      expect(manager.hasSession(session.threadId)).toBe(false);
    } finally {
      manager.stopAll();
      fake.cleanup();
    }
  });
});
