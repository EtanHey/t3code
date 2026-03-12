import { spawn, spawnSync, type ChildProcessWithoutNullStreams } from "node:child_process";
import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import readline from "node:readline";

import {
  EventId,
  ThreadId,
  TurnId,
  type ProviderInteractionMode,
  type RuntimeMode,
} from "@t3tools/contracts";
import { Effect, Option, Result, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { isCommandAvailable } from "./open";

const CLAUDE_INIT_TIMEOUT_MS = 15_000;
const CLAUDE_HEALTH_TIMEOUT_MS = 4_000;

export type ClaudeProcessSessionStatus = "connecting" | "ready" | "running" | "error" | "closed";
export type ClaudePermissionMode =
  | "acceptEdits"
  | "bypassPermissions"
  | "default"
  | "dontAsk"
  | "plan"
  | "auto";
export type ClaudeCliAuthStatus = "authenticated" | "unauthenticated" | "unknown";

export interface ClaudeProcessSession {
  readonly provider: "claude";
  readonly status: ClaudeProcessSessionStatus;
  readonly runtimeMode: RuntimeMode;
  readonly cwd?: string | undefined;
  readonly model?: string | undefined;
  readonly threadId: ThreadId;
  readonly resumeCursor?:
    | {
        readonly sessionId: string;
      }
    | undefined;
  readonly activeTurnId?: TurnId | undefined;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly lastError?: string | undefined;
}

export interface ClaudeProcessStartSessionInput {
  readonly threadId: ThreadId;
  readonly cwd?: string | undefined;
  readonly model?: string | undefined;
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode?: ProviderInteractionMode | undefined;
  readonly permissionMode?: ClaudePermissionMode | undefined;
  readonly allowedTools?: ReadonlyArray<string> | undefined;
  readonly disallowedTools?: ReadonlyArray<string> | undefined;
  readonly appendSystemPrompt?: string | undefined;
  readonly systemPrompt?: string | undefined;
  readonly resumeSessionId?: string | undefined;
  readonly sessionId?: string | undefined;
  readonly binaryPath?: string | undefined;
  readonly env?: NodeJS.ProcessEnv | undefined;
  readonly wslDistro?: string | undefined;
}

export interface ClaudeProcessSendTurnInput {
  readonly threadId: ThreadId;
  readonly input: string;
  readonly parentToolUseId?: string | null | undefined;
}

export interface ClaudeCliHealthInput {
  readonly binaryPath?: string | undefined;
  readonly env?: NodeJS.ProcessEnv | undefined;
  readonly cwd?: string | undefined;
  readonly wslDistro?: string | undefined;
}

export interface ClaudeCliHealthStatus {
  readonly status: "ready" | "warning" | "error";
  readonly available: boolean;
  readonly authStatus: ClaudeCliAuthStatus;
  readonly checkedAt: string;
  readonly message?: string | undefined;
  readonly version?: string | undefined;
}

export interface ClaudeProcessEvent {
  readonly id: EventId;
  readonly kind: "session" | "stream" | "error";
  readonly provider: "claude";
  readonly threadId: ThreadId;
  readonly createdAt: string;
  readonly method: string;
  readonly message?: string | undefined;
  readonly turnId?: TurnId | undefined;
  readonly payload?: unknown;
  readonly raw?:
    | {
        readonly line: string;
        readonly source: "stdout" | "stderr";
      }
    | undefined;
}

export interface ClaudeStreamJsonMessage {
  readonly type: string;
  readonly subtype?: string;
  readonly session_id?: string;
  readonly uuid?: string;
  readonly [key: string]: unknown;
}

export interface ClaudeCliCommandResult {
  readonly stdout: string;
  readonly stderr: string;
  readonly code: number;
}

export interface ClaudeCommandResolutionOptions {
  readonly platform?: NodeJS.Platform | undefined;
  readonly env?: NodeJS.ProcessEnv | undefined;
  readonly hasCommand?: (command: string, env: NodeJS.ProcessEnv) => boolean;
  readonly resolveWslBinaryPath?: (env: NodeJS.ProcessEnv, distro?: string) => string | undefined;
  readonly resolveWslPath?: (
    filePath: string,
    env: NodeJS.ProcessEnv,
    distro?: string,
  ) => string | undefined;
}

export interface ClaudeCommandSpec {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd?: string | undefined;
  readonly env: NodeJS.ProcessEnv;
  readonly shell: boolean;
  readonly viaWsl: boolean;
  readonly resolvedBinaryPath: string;
}

interface PendingStart {
  readonly timeout: ReturnType<typeof setTimeout>;
  readonly resolve: (session: ClaudeProcessSession) => void;
  readonly reject: (error: Error) => void;
}

interface ClaudeSessionContext {
  session: ClaudeProcessSession;
  readonly child: ChildProcessWithoutNullStreams;
  readonly output: readline.Interface;
  readonly spec: ClaudeCommandSpec;
  stopping: boolean;
  pendingStart: PendingStart | undefined;
}

function trimToUndefined(value: string | undefined | null): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function shellEscape(value: string): string {
  return `'${value.replaceAll("'", `'\\''`)}'`;
}

function nonEmptyTrimmed(value: string | undefined): string | undefined {
  return trimToUndefined(value);
}

function detailFromResult(result: ClaudeCliCommandResult): string | undefined {
  const stderr = nonEmptyTrimmed(result.stderr);
  if (stderr) return stderr;
  const stdout = nonEmptyTrimmed(result.stdout);
  if (stdout) return stdout;
  if (result.code !== 0) {
    return `Command exited with code ${result.code}.`;
  }
  return undefined;
}

function isCommandMissingCause(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  const lower = error.message.toLowerCase();
  return lower.includes("command not found: claude") || lower.includes("spawn claude enoent");
}

function parseClaudeVersion(output: string): string | undefined {
  const match = output.match(/(\d+\.\d+\.\d+)/);
  return match?.[1];
}

export function parseClaudeAuthStatusFromOutput(result: ClaudeCliCommandResult): {
  readonly status: "ready" | "warning" | "error";
  readonly authStatus: ClaudeCliAuthStatus;
  readonly message?: string;
} {
  const combined = `${result.stdout}\n${result.stderr}`.toLowerCase();

  if (combined.includes("not logged in") || combined.includes("login required")) {
    return {
      status: "error",
      authStatus: "unauthenticated",
      message: "Claude CLI is not authenticated. Run `claude auth login` and try again.",
    };
  }

  const trimmed = result.stdout.trim();
  if (trimmed.startsWith("{")) {
    try {
      const parsed = JSON.parse(trimmed) as { loggedIn?: unknown };
      if (parsed.loggedIn === true) {
        return { status: "ready", authStatus: "authenticated" };
      }
      if (parsed.loggedIn === false) {
        return {
          status: "error",
          authStatus: "unauthenticated",
          message: "Claude CLI is not authenticated. Run `claude auth login` and try again.",
        };
      }
    } catch {
      return {
        status: "warning",
        authStatus: "unknown",
        message: "Could not parse Claude authentication status output.",
      };
    }
  }

  if (result.code === 0) {
    return { status: "ready", authStatus: "authenticated" };
  }

  const detail = detailFromResult(result);
  return {
    status: "warning",
    authStatus: "unknown",
    message: detail
      ? `Could not verify Claude authentication status. ${detail}`
      : "Could not verify Claude authentication status.",
  };
}

function collectStreamAsString<E>(stream: Stream.Stream<Uint8Array, E>): Effect.Effect<string, E> {
  return Stream.runFold(
    stream,
    () => "",
    (acc, chunk) => acc + new TextDecoder().decode(chunk),
  );
}

function resolveDefaultWslBinaryPath(env: NodeJS.ProcessEnv, distro?: string): string | undefined {
  const distroArgs = distro ? ["-d", distro] : [];
  try {
    const result = spawnSync("wsl.exe", [...distroArgs, "sh", "-lc", "command -v claude"], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      env,
      windowsHide: true,
    });
    return trimToUndefined(result.stdout ?? undefined);
  } catch {
    return undefined;
  }
}

function resolveDefaultWslPath(
  filePath: string,
  env: NodeJS.ProcessEnv,
  distro?: string,
): string | undefined {
  const distroArgs = distro ? ["-d", distro] : [];
  try {
    const result = spawnSync("wsl.exe", [...distroArgs, "wslpath", "-a", filePath], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      env,
      windowsHide: true,
    });
    return trimToUndefined(result.stdout ?? undefined);
  } catch {
    return undefined;
  }
}

function shouldUseWsl(binaryPath: string | undefined, platform: NodeJS.Platform): boolean {
  if (platform !== "win32") {
    return false;
  }
  if (!binaryPath) {
    return false;
  }
  return binaryPath.startsWith("/") || binaryPath.startsWith("~/");
}

export function resolveClaudeCommandSpec(
  input: {
    readonly binaryPath?: string | undefined;
    readonly cwd?: string | undefined;
    readonly env?: NodeJS.ProcessEnv | undefined;
    readonly wslDistro?: string | undefined;
    readonly claudeArgs: ReadonlyArray<string>;
  },
  options: ClaudeCommandResolutionOptions = {},
): ClaudeCommandSpec {
  const platform = options.platform ?? process.platform;
  const env = input.env ?? options.env ?? process.env;
  const hasCommand =
    options.hasCommand ??
    ((command: string, currentEnv: NodeJS.ProcessEnv) =>
      isCommandAvailable(command, { env: currentEnv, platform }));
  const resolveWslBinaryPath = options.resolveWslBinaryPath ?? resolveDefaultWslBinaryPath;
  const resolveWslPath = options.resolveWslPath ?? resolveDefaultWslPath;
  const binaryPath = trimToUndefined(input.binaryPath) ?? "claude";

  if (platform !== "win32") {
    return {
      command: binaryPath,
      args: input.claudeArgs,
      cwd: input.cwd,
      env,
      shell: false,
      viaWsl: false,
      resolvedBinaryPath: binaryPath,
    };
  }

  if (
    shouldUseWsl(binaryPath, platform) ||
    (!hasCommand(binaryPath, env) && !hasCommand("claude", env))
  ) {
    const resolvedBinaryPath = shouldUseWsl(binaryPath, platform)
      ? binaryPath
      : resolveWslBinaryPath(env, input.wslDistro);
    if (resolvedBinaryPath) {
      const wslCwd = input.cwd ? resolveWslPath(input.cwd, env, input.wslDistro) : undefined;
      const commandParts = [
        ...(wslCwd ? [`cd ${shellEscape(wslCwd)}`, "&&"] : []),
        "exec",
        shellEscape(resolvedBinaryPath),
        ...input.claudeArgs.map(shellEscape),
      ];
      return {
        command: "wsl.exe",
        args: [
          ...(input.wslDistro ? ["-d", input.wslDistro] : []),
          "sh",
          "-lc",
          commandParts.join(" "),
        ],
        env,
        shell: false,
        viaWsl: true,
        resolvedBinaryPath,
      };
    }
  }

  return {
    command: binaryPath,
    args: input.claudeArgs,
    cwd: input.cwd,
    env,
    shell: true,
    viaWsl: false,
    resolvedBinaryPath: binaryPath,
  };
}

function mapClaudePermissionMode(input: {
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode?: ProviderInteractionMode | undefined;
  readonly permissionMode?: ClaudePermissionMode | undefined;
}): ClaudePermissionMode {
  if (input.permissionMode) {
    return input.permissionMode;
  }
  if (input.interactionMode === "plan") {
    return "plan";
  }
  return input.runtimeMode === "full-access" ? "bypassPermissions" : "default";
}

export function buildClaudeCliArgs(
  input: Omit<
    ClaudeProcessStartSessionInput,
    "threadId" | "cwd" | "binaryPath" | "env" | "wslDistro"
  >,
): ReadonlyArray<string> {
  const args: string[] = [
    "-p",
    "--input-format",
    "stream-json",
    "--output-format",
    "stream-json",
    "--replay-user-messages",
    "--include-partial-messages",
    "--permission-mode",
    mapClaudePermissionMode(input),
  ];

  const model = trimToUndefined(input.model);
  if (model) {
    args.push("--model", model);
  }
  if (input.allowedTools && input.allowedTools.length > 0) {
    args.push("--allowed-tools", input.allowedTools.join(","));
  }
  if (input.disallowedTools && input.disallowedTools.length > 0) {
    args.push("--disallowed-tools", input.disallowedTools.join(","));
  }
  if (input.resumeSessionId) {
    args.push("--resume", input.resumeSessionId);
  }
  if (input.sessionId) {
    args.push("--session-id", input.sessionId);
  }
  if (input.systemPrompt) {
    args.push("--system-prompt", input.systemPrompt);
  }
  if (input.appendSystemPrompt) {
    args.push("--append-system-prompt", input.appendSystemPrompt);
  }

  return args;
}

function toClaudeUserEnvelope(input: ClaudeProcessSendTurnInput): Record<string, unknown> {
  return {
    type: "user",
    message: {
      role: "user",
      content: [{ type: "text", text: input.input }],
    },
    parent_tool_use_id: input.parentToolUseId ?? null,
  };
}

function killChildTree(
  child: ChildProcessWithoutNullStreams,
  signal: NodeJS.Signals = "SIGTERM",
): void {
  if (process.platform === "win32" && child.pid !== undefined) {
    try {
      spawnSync("taskkill", ["/pid", String(child.pid), "/T", "/F"], { stdio: "ignore" });
      return;
    } catch {
      // fall back to direct kill
    }
  }
  child.kill(signal);
}

function readSessionId(message: ClaudeStreamJsonMessage): string | undefined {
  const sessionId = message.session_id;
  return typeof sessionId === "string" && sessionId.length > 0 ? sessionId : undefined;
}

function readResultErrorMessage(message: ClaudeStreamJsonMessage): string | undefined {
  const result = message.result;
  if (typeof result === "string" && result.length > 0) {
    return result;
  }
  const error = message.error;
  return typeof error === "string" && error.length > 0 ? error : undefined;
}

function methodForMessage(message: ClaudeStreamJsonMessage): string {
  if (message.type === "system" && typeof message.subtype === "string") {
    return `stream/system/${message.subtype}`;
  }
  return `stream/${message.type}`;
}

function runClaudeCommand(input: ClaudeCliHealthInput & { readonly args: ReadonlyArray<string> }) {
  return Effect.gen(function* () {
    const spec = resolveClaudeCommandSpec({
      binaryPath: input.binaryPath,
      cwd: input.cwd,
      env: input.env,
      wslDistro: input.wslDistro,
      claudeArgs: input.args,
    });
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const command = ChildProcess.make(spec.command, [...spec.args], {
      shell: spec.shell,
      cwd: spec.cwd,
      env: spec.env,
    });
    const child = yield* spawner.spawn(command);
    const [stdout, stderr, exitCode] = yield* Effect.all(
      [
        collectStreamAsString(child.stdout),
        collectStreamAsString(child.stderr),
        child.exitCode.pipe(Effect.map(Number)),
      ],
      { concurrency: "unbounded" },
    );
    return { stdout, stderr, code: exitCode } satisfies ClaudeCliCommandResult;
  }).pipe(Effect.scoped);
}

export const checkClaudeCliHealth = (
  input: ClaudeCliHealthInput = {},
): Effect.Effect<ClaudeCliHealthStatus, never, ChildProcessSpawner.ChildProcessSpawner> =>
  Effect.gen(function* () {
    const checkedAt = new Date().toISOString();

    const versionProbe = yield* runClaudeCommand({ ...input, args: ["--version"] }).pipe(
      Effect.timeoutOption(CLAUDE_HEALTH_TIMEOUT_MS),
      Effect.result,
    );
    if (Result.isFailure(versionProbe)) {
      const error = versionProbe.failure;
      return {
        status: "error" as const,
        available: false,
        authStatus: "unknown" as const,
        checkedAt,
        message: isCommandMissingCause(error)
          ? "Claude CLI (`claude`) is not installed or not on PATH."
          : `Failed to execute Claude CLI health check: ${error instanceof Error ? error.message : String(error)}.`,
      } satisfies ClaudeCliHealthStatus;
    }
    if (Option.isNone(versionProbe.success)) {
      return {
        status: "error" as const,
        available: false,
        authStatus: "unknown" as const,
        checkedAt,
        message: "Claude CLI timed out while running `claude --version`.",
      } satisfies ClaudeCliHealthStatus;
    }

    const version = versionProbe.success.value;
    if (version.code !== 0) {
      const detail = detailFromResult(version);
      return {
        status: "error" as const,
        available: false,
        authStatus: "unknown" as const,
        checkedAt,
        message: detail
          ? `Claude CLI is installed but failed to run. ${detail}`
          : "Claude CLI is installed but failed to run.",
      } satisfies ClaudeCliHealthStatus;
    }

    const authProbe = yield* runClaudeCommand({ ...input, args: ["auth", "status"] }).pipe(
      Effect.timeoutOption(CLAUDE_HEALTH_TIMEOUT_MS),
      Effect.result,
    );
    if (Result.isFailure(authProbe)) {
      const error = authProbe.failure;
      return {
        status: "warning" as const,
        available: true,
        authStatus: "unknown" as const,
        checkedAt,
        version: parseClaudeVersion(`${version.stdout}\n${version.stderr}`),
        message:
          error instanceof Error
            ? `Could not verify Claude authentication status: ${error.message}.`
            : "Could not verify Claude authentication status.",
      } satisfies ClaudeCliHealthStatus;
    }
    if (Option.isNone(authProbe.success)) {
      return {
        status: "warning" as const,
        available: true,
        authStatus: "unknown" as const,
        checkedAt,
        version: parseClaudeVersion(`${version.stdout}\n${version.stderr}`),
        message: "Could not verify Claude authentication status. Timed out while running command.",
      } satisfies ClaudeCliHealthStatus;
    }

    const parsedAuth = parseClaudeAuthStatusFromOutput(authProbe.success.value);
    return {
      status: parsedAuth.status,
      available: true,
      authStatus: parsedAuth.authStatus,
      checkedAt,
      version: parseClaudeVersion(`${version.stdout}\n${version.stderr}`),
      ...(parsedAuth.message ? { message: parsedAuth.message } : {}),
    } satisfies ClaudeCliHealthStatus;
  });

export interface ClaudeProcessManagerEvents {
  event: [event: ClaudeProcessEvent];
}

export class ClaudeProcessManager extends EventEmitter<ClaudeProcessManagerEvents> {
  private readonly sessions = new Map<ThreadId, ClaudeSessionContext>();

  async startSession(input: ClaudeProcessStartSessionInput): Promise<ClaudeProcessSession> {
    if (this.sessions.has(input.threadId)) {
      throw new Error(`Session already exists for thread: ${input.threadId}`);
    }

    const now = new Date().toISOString();
    const spec = resolveClaudeCommandSpec({
      binaryPath: input.binaryPath,
      cwd: input.cwd ?? process.cwd(),
      env: input.env,
      wslDistro: input.wslDistro,
      claudeArgs: buildClaudeCliArgs(input),
    });
    const child = spawn(spec.command, [...spec.args], {
      cwd: spec.cwd,
      env: spec.env,
      stdio: ["pipe", "pipe", "pipe"],
      shell: spec.shell,
    });
    const output = readline.createInterface({ input: child.stdout });

    const context: ClaudeSessionContext = {
      session: {
        provider: "claude",
        status: "connecting",
        runtimeMode: input.runtimeMode,
        ...(spec.cwd ? { cwd: spec.cwd } : {}),
        ...(trimToUndefined(input.model) ? { model: trimToUndefined(input.model) } : {}),
        threadId: input.threadId,
        createdAt: now,
        updatedAt: now,
      },
      child,
      output,
      spec,
      stopping: false,
      pendingStart: undefined,
    };

    this.sessions.set(input.threadId, context);
    this.attachProcessListeners(context);
    this.emitLifecycleEvent(context, "session/connecting", "Starting Claude CLI process");

    try {
      const session = await new Promise<ClaudeProcessSession>((resolve, reject) => {
        const timeout = setTimeout(() => {
          context.pendingStart = undefined;
          reject(new Error("Timed out waiting for Claude session init."));
        }, CLAUDE_INIT_TIMEOUT_MS);

        context.pendingStart = {
          timeout,
          resolve,
          reject,
        };
      });
      return session;
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to start Claude session.";
      this.updateSession(context, {
        status: "error",
        lastError: message,
      });
      this.emitErrorEvent(context, "session/startFailed", message);
      this.stopSession(input.threadId);
      throw error instanceof Error ? error : new Error(message);
    }
  }

  async sendTurn(input: ClaudeProcessSendTurnInput): Promise<{
    readonly threadId: ThreadId;
    readonly turnId: TurnId;
    readonly resumeCursor?: { readonly sessionId: string };
  }> {
    const context = this.requireSession(input.threadId);
    if (context.session.activeTurnId) {
      throw new Error(`Claude turn already running for thread: ${input.threadId}`);
    }

    const turnId = TurnId.makeUnsafe(randomUUID());
    this.updateSession(context, {
      status: "running",
      activeTurnId: turnId,
    });
    this.writeMessage(context, toClaudeUserEnvelope(input));

    return {
      threadId: input.threadId,
      turnId,
      ...(context.session.resumeCursor ? { resumeCursor: context.session.resumeCursor } : {}),
    };
  }

  async interruptTurn(threadId: ThreadId, turnId?: TurnId): Promise<void> {
    const context = this.requireSession(threadId);
    const activeTurnId = turnId ?? context.session.activeTurnId;
    if (!activeTurnId) {
      return;
    }
    this.emitLifecycleEvent(context, "turn/interruptRequested", "Interrupting Claude turn");
    context.child.kill("SIGINT");
  }

  stopSession(threadId: ThreadId): void {
    const context = this.sessions.get(threadId);
    if (!context) {
      return;
    }

    context.stopping = true;
    if (context.pendingStart) {
      clearTimeout(context.pendingStart.timeout);
      context.pendingStart.reject(new Error("Session stopped before init completed."));
      context.pendingStart = undefined;
    }
    context.output.close();
    if (!context.child.killed) {
      killChildTree(context.child, "SIGTERM");
    }
    this.updateSession(context, {
      status: "closed",
      activeTurnId: undefined,
    });
    this.emitLifecycleEvent(context, "session/closed", "Session stopped");
    this.sessions.delete(threadId);
  }

  listSessions(): ReadonlyArray<ClaudeProcessSession> {
    return Array.from(this.sessions.values(), ({ session }) => ({ ...session }));
  }

  hasSession(threadId: ThreadId): boolean {
    return this.sessions.has(threadId);
  }

  stopAll(): void {
    for (const threadId of this.sessions.keys()) {
      this.stopSession(threadId);
    }
  }

  private requireSession(threadId: ThreadId): ClaudeSessionContext {
    const context = this.sessions.get(threadId);
    if (!context) {
      throw new Error(`Unknown session for thread: ${threadId}`);
    }
    if (context.session.status === "closed") {
      throw new Error(`Session is closed for thread: ${threadId}`);
    }
    return context;
  }

  private attachProcessListeners(context: ClaudeSessionContext): void {
    context.output.on("line", (line) => {
      this.handleStdoutLine(context, line);
    });

    context.child.stderr.on("data", (chunk: Buffer) => {
      const raw = chunk.toString();
      for (const rawLine of raw.split(/\r?\n/g)) {
        const line = rawLine.trim();
        if (!line) continue;
        this.emitErrorEvent(context, "process/stderr", line, {
          line,
          source: "stderr",
        });
      }
    });

    context.child.on("error", (error) => {
      if (context.pendingStart) {
        clearTimeout(context.pendingStart.timeout);
        context.pendingStart.reject(new Error(error.message));
        context.pendingStart = undefined;
      }
      this.updateSession(context, {
        status: "error",
        lastError: error.message,
      });
      this.emitErrorEvent(context, "process/error", error.message);
    });

    context.child.on("exit", (code, signal) => {
      if (context.pendingStart) {
        clearTimeout(context.pendingStart.timeout);
        context.pendingStart.reject(
          new Error(
            `Claude process exited before init (code=${code ?? "null"}, signal=${signal ?? "null"}).`,
          ),
        );
        context.pendingStart = undefined;
      }
      if (context.stopping) {
        return;
      }
      const message = `Claude process exited (code=${code ?? "null"}, signal=${signal ?? "null"}).`;
      this.updateSession(context, {
        status: "closed",
        activeTurnId: undefined,
        ...(code !== 0 ? { lastError: message } : {}),
      });
      this.emitLifecycleEvent(context, "session/exited", message);
      this.sessions.delete(context.session.threadId);
    });
  }

  private handleStdoutLine(context: ClaudeSessionContext, line: string): void {
    let parsed: ClaudeStreamJsonMessage;
    try {
      parsed = JSON.parse(line) as ClaudeStreamJsonMessage;
    } catch {
      this.emitErrorEvent(
        context,
        "protocol/parseError",
        "Received invalid JSON from Claude CLI stdout.",
        {
          line,
          source: "stdout",
        },
      );
      return;
    }

    if (!parsed || typeof parsed !== "object" || typeof parsed.type !== "string") {
      this.emitErrorEvent(
        context,
        "protocol/invalidMessage",
        "Received Claude protocol message in an unknown shape.",
        {
          line,
          source: "stdout",
        },
      );
      return;
    }

    this.handleStreamMessage(context, parsed, line);
  }

  private handleStreamMessage(
    context: ClaudeSessionContext,
    message: ClaudeStreamJsonMessage,
    rawLine: string,
  ): void {
    const turnId = context.session.activeTurnId;
    this.emitEvent({
      id: EventId.makeUnsafe(randomUUID()),
      kind: "stream",
      provider: "claude",
      threadId: context.session.threadId,
      createdAt: new Date().toISOString(),
      method: methodForMessage(message),
      ...(turnId ? { turnId } : {}),
      payload: message,
      raw: {
        line: rawLine,
        source: "stdout",
      },
    });

    const sessionId = readSessionId(message);
    if (message.type === "system" && message.subtype === "init") {
      this.updateSession(context, {
        status: "ready",
        ...(sessionId ? { resumeCursor: { sessionId } } : {}),
      });
      if (context.pendingStart) {
        clearTimeout(context.pendingStart.timeout);
        context.pendingStart.resolve({ ...context.session });
        context.pendingStart = undefined;
      }
      this.emitLifecycleEvent(context, "session/ready", "Claude CLI initialized");
      return;
    }

    if (sessionId && !context.session.resumeCursor) {
      this.updateSession(context, {
        resumeCursor: { sessionId },
      });
    }

    if (message.type === "result") {
      this.updateSession(context, {
        status: message.is_error === true ? "error" : "ready",
        activeTurnId: undefined,
        ...(readResultErrorMessage(message) ? { lastError: readResultErrorMessage(message) } : {}),
      });
    }
  }

  private updateSession(
    context: ClaudeSessionContext,
    updates: Partial<ClaudeProcessSession>,
  ): void {
    context.session = {
      ...context.session,
      ...updates,
      updatedAt: new Date().toISOString(),
    };
  }

  private writeMessage(context: ClaudeSessionContext, message: unknown): void {
    if (!context.child.stdin.writable) {
      throw new Error("Cannot write to Claude CLI stdin.");
    }
    context.child.stdin.write(`${JSON.stringify(message)}\n`);
  }

  private emitLifecycleEvent(context: ClaudeSessionContext, method: string, message: string): void {
    this.emitEvent({
      id: EventId.makeUnsafe(randomUUID()),
      kind: "session",
      provider: "claude",
      threadId: context.session.threadId,
      createdAt: new Date().toISOString(),
      method,
      message,
    });
  }

  private emitErrorEvent(
    context: ClaudeSessionContext,
    method: string,
    message: string,
    raw?: ClaudeProcessEvent["raw"],
  ): void {
    this.emitEvent({
      id: EventId.makeUnsafe(randomUUID()),
      kind: "error",
      provider: "claude",
      threadId: context.session.threadId,
      createdAt: new Date().toISOString(),
      method,
      message,
      ...(raw ? { raw } : {}),
    });
  }

  private emitEvent(event: ClaudeProcessEvent): void {
    this.emit("event", event);
  }
}
