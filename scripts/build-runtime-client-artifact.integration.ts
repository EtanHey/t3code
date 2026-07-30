#!/usr/bin/env node

// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { T3_RPC_COMPATIBILITY, T3_RPC_EFFECT_VERSION } from "@t3tools/contracts/runtime-client";
import { assertPackedMembersClean, assertPatchedBundle } from "./build-runtime-client-artifact.ts";

const repoRoot = NodePath.resolve(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)), "..");
const builderPath = NodePath.join(repoRoot, "scripts/build-runtime-client-artifact.ts");
const tsgoPath = NodePath.join(repoRoot, "node_modules/.bin/tsgo");
const artifactName = "t3tools-runtime-client-0.0.31-rpc.1.tgz";
const expectedInventory = [
  "package/LICENSE",
  "package/dist/index.d.mts",
  "package/dist/index.mjs",
  "package/package.json",
];
const forbiddenRoutes = [
  "/oauth/token",
  "/api/auth/pairing-token",
  "/api/auth/websocket-ticket",
  "/api/auth/pairing-links/revoke",
  "/api/connect/mint-credential",
  "/api/connect/relay-config",
  "/v1/client/environment-links",
  "/v1/mobile/live-activities",
  "/.well-known/oauth-authorization-server",
  "/.well-known/oauth-protected-resource",
];

interface Receipt {
  readonly artifact: string;
  readonly bytes: number;
  readonly compatibility: Record<string, unknown>;
  readonly inventory: ReadonlyArray<string>;
  readonly packageName: string;
  readonly publishable: boolean;
  readonly sha256: string;
  readonly sourceRevision: string;
  readonly toolchain: {
    readonly node: string;
    readonly npm: string;
    readonly tar: string;
  };
  readonly version: string;
}

function run(
  command: string,
  args: ReadonlyArray<string>,
  options: {
    readonly cwd?: string;
    readonly env?: NodeJS.ProcessEnv;
  } = {},
): string {
  const result = NodeChildProcess.spawnSync(command, args, {
    cwd: options.cwd,
    encoding: "utf8",
    env: options.env,
  });
  if (result.error !== undefined) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(
      [
        `Command failed (${String(result.status)}): ${command} ${args.join(" ")}`,
        result.stdout,
        result.stderr,
      ].join("\n"),
    );
  }
  return result.stdout;
}

function readJson<T>(path: string): T {
  return JSON.parse(NodeFS.readFileSync(path, "utf8")) as T;
}

function sha256(path: string): string {
  return NodeCrypto.createHash("sha256").update(NodeFS.readFileSync(path)).digest("hex");
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) {
    throw new Error(message);
  }
}

function assertDeepEqual(actual: unknown, expected: unknown, label: string): void {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(
      `${label} mismatch.\nExpected: ${JSON.stringify(expected)}\nActual: ${JSON.stringify(actual)}`,
    );
  }
}

function buildInto(outputDir: string): Receipt {
  const output = run(process.execPath, [builderPath, "--allow-dirty", "--output-dir", outputDir]);
  const lastLine = output
    .trim()
    .split(/\r?\n/)
    .findLast((line) => line.startsWith("{"));
  assert(lastLine !== undefined, "Artifact builder did not print a JSON receipt.");
  return JSON.parse(lastLine) as Receipt;
}

const tempRoot = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-runtime-distribution-"));

try {
  const firstOutput = NodePath.join(tempRoot, "first");
  const secondOutput = NodePath.join(tempRoot, "second");
  const extractedRoot = NodePath.join(tempRoot, "extracted");
  const consumerRoot = NodePath.join(tempRoot, "consumer");
  const head = run("git", ["rev-parse", "HEAD"], { cwd: repoRoot }).trim();
  const expectedRevision = `${head}-dirty`;

  const firstReceipt = buildInto(firstOutput);
  const secondReceipt = buildInto(secondOutput);
  const firstArtifact = NodePath.join(firstOutput, artifactName);
  const secondArtifact = NodePath.join(secondOutput, artifactName);
  const firstBytes = NodeFS.readFileSync(firstArtifact);
  const secondBytes = NodeFS.readFileSync(secondArtifact);

  assert(firstBytes.equals(secondBytes), "Two review builds are not byte-identical.");
  assertDeepEqual(firstReceipt, secondReceipt, "deterministic receipts");
  assert(firstReceipt.publishable === false, "Dirty receipt must be non-publishable.");
  assert(firstReceipt.sourceRevision === expectedRevision, "Dirty receipt revision is dishonest.");
  assert(firstReceipt.toolchain.node === process.version, "Receipt Node version is not actual.");
  assert(firstReceipt.toolchain.npm.length > 0, "Receipt npm version is missing.");
  assert(firstReceipt.toolchain.tar.length > 0, "Receipt tar version is missing.");

  NodeFS.mkdirSync(extractedRoot, { recursive: true });
  run("tar", ["-xzf", firstArtifact, "-C", extractedRoot]);
  const extractedPackage = NodePath.join(extractedRoot, "package");
  assertPackedMembersClean(extractedPackage, [repoRoot, tempRoot]);

  const manifest = readJson<Record<string, unknown>>(
    NodePath.join(extractedPackage, "package.json"),
  );
  const receipt = readJson<Receipt>(`${firstArtifact}.json`);
  const license = NodeFS.readFileSync(NodePath.join(extractedPackage, "LICENSE"), "utf8");
  const javascript = NodeFS.readFileSync(NodePath.join(extractedPackage, "dist/index.mjs"), "utf8");
  const declarations = NodeFS.readFileSync(
    NodePath.join(extractedPackage, "dist/index.d.mts"),
    "utf8",
  );
  const actualSha256 = sha256(firstArtifact);
  const adjacentSha256 = NodeFS.readFileSync(`${firstArtifact}.sha256`, "utf8").trim();

  assert(manifest.license === "MIT", "Packed manifest license is not MIT.");
  assert(license.startsWith("MIT License\n"), "Packed license text is not MIT.");
  assert(manifest.dependencies === undefined, "Packed manifest has runtime dependencies.");
  assertDeepEqual(
    manifest.peerDependencies,
    { effect: T3_RPC_EFFECT_VERSION },
    "exact Effect peer dependency",
  );
  assert(manifest.t3SourceRevision === expectedRevision, "Manifest dirty revision is dishonest.");
  assert(manifest.t3Publishable === false, "Dirty manifest must be non-publishable.");
  assertDeepEqual(
    manifest.t3RpcCompatibility,
    {
      ...T3_RPC_COMPATIBILITY,
      effectVersion: T3_RPC_EFFECT_VERSION,
      effectPatchSha256: "71215759e1ac0a7f65d7b75d816986687ae6c3a6cba02d928d184ca71790d488",
    },
    "canonical compatibility metadata",
  );
  assertPatchedBundle(javascript);
  for (const route of forbiddenRoutes) {
    assert(!javascript.includes(route), `Runtime bundle leaks unrelated route literal '${route}'.`);
  }
  assert(
    !/readonly (?:rootPath|ignoreWhitespace|limit)\?: [^;\n]+ \| undefined;/.test(declarations),
    "Declarations widen a canonical exact optional property.",
  );
  assert(
    !/import\s*\{\s*Schema\s*\}\s*from\s*["']effect["']/.test(declarations),
    "Declarations contain the prohibited Effect Schema barrel import.",
  );
  assertDeepEqual(receipt.inventory, expectedInventory, "packed inventory");
  assert(receipt.sha256 === actualSha256, "Receipt checksum does not match tarball.");
  assert(
    adjacentSha256 === `${actualSha256}  ${artifactName}`,
    "Adjacent checksum does not match tarball.",
  );
  assert(receipt.bytes === NodeFS.statSync(firstArtifact).size, "Receipt size is incorrect.");

  NodeFS.mkdirSync(consumerRoot, { recursive: true });
  NodeFS.writeFileSync(
    NodePath.join(consumerRoot, "package.json"),
    `${JSON.stringify(
      {
        name: "t3-runtime-client-consumer",
        private: true,
        type: "module",
        dependencies: {
          "@t3tools/runtime-client": `file:${firstArtifact}`,
          effect: T3_RPC_EFFECT_VERSION,
        },
      },
      null,
      2,
    )}\n`,
  );
  NodeFS.writeFileSync(
    NodePath.join(consumerRoot, "tsconfig.json"),
    `${JSON.stringify(
      {
        compilerOptions: {
          exactOptionalPropertyTypes: true,
          module: "NodeNext",
          moduleResolution: "NodeNext",
          noEmit: true,
          skipLibCheck: true,
          strict: true,
          target: "ESNext",
        },
        include: ["smoke.ts"],
      },
      null,
      2,
    )}\n`,
  );
  NodeFS.writeFileSync(
    NodePath.join(consumerRoot, "tsconfig.negative.json"),
    `${JSON.stringify(
      {
        extends: "./tsconfig.json",
        include: ["negative.ts"],
      },
      null,
      2,
    )}\n`,
  );
  NodeFS.writeFileSync(
    NodePath.join(consumerRoot, "negative.ts"),
    [
      'import type { OrchestrationGetTurnDiffInput } from "@t3tools/runtime-client";',
      "",
      "declare const valid: OrchestrationGetTurnDiffInput;",
      "const invalid: OrchestrationGetTurnDiffInput = {",
      "  ...valid,",
      "  ignoreWhitespace: undefined,",
      "};",
      "void invalid;",
      "",
    ].join("\n"),
  );
  NodeFS.writeFileSync(
    NodePath.join(consumerRoot, "smoke.ts"),
    [
      'import * as Cause from "effect/Cause";',
      'import * as Effect from "effect/Effect";',
      'import * as Exit from "effect/Exit";',
      'import * as Option from "effect/Option";',
      'import * as Schema from "effect/Schema";',
      'import * as Socket from "effect/unstable/socket/Socket";',
      "import {",
      "  ClientOrchestrationCommand,",
      "  EnvironmentId,",
      "  ORCHESTRATION_WS_METHODS,",
      "  T3_RPC_COMPATIBILITY,",
      "  T3_RPC_EFFECT_VERSION,",
      "  ThreadTurnStartCommand,",
      "  WS_METHODS,",
      "  makeRpcSessionFactory,",
      "  makeWsRpcProtocolClient,",
      '} from "@t3tools/runtime-client";',
      "",
      "const decoded = Schema.decodeUnknownSync(ClientOrchestrationCommand)({",
      '  type: "thread.turn.start",',
      '  commandId: "command-1",',
      '  threadId: "thread-1",',
      '  message: { messageId: "message-1", role: "user", text: "hello", attachments: [] },',
      '  runtimeMode: "approval-required",',
      '  interactionMode: "default",',
      '  createdAt: "2026-07-30T00:00:00.000Z",',
      "});",
      'if (decoded.type !== "thread.turn.start") throw new Error("command decode failed");',
      'if (!ThreadTurnStartCommand) throw new Error("thread command export missing");',
      'if (ORCHESTRATION_WS_METHODS.dispatchCommand !== "orchestration.dispatchCommand") {',
      '  throw new Error("RPC method mismatch");',
      "}",
      'if (WS_METHODS.serverProbe !== "server.probe") throw new Error("server RPC mismatch");',
      `if (T3_RPC_EFFECT_VERSION !== "${T3_RPC_EFFECT_VERSION}") throw new Error("Effect mismatch");`,
      `if (T3_RPC_COMPATIBILITY.contractFingerprint !== "${T3_RPC_COMPATIBILITY.contractFingerprint}") {`,
      '  throw new Error("compatibility mismatch");',
      "}",
      'if (!Effect.isEffect(makeRpcSessionFactory)) throw new Error("session factory is not Effect");',
      'if (!Effect.isEffect(makeWsRpcProtocolClient)) throw new Error("protocol factory is not Effect");',
      "",
      "const factory = Effect.runSync(",
      "  Effect.provideService(",
      "    makeRpcSessionFactory,",
      "    Socket.WebSocketConstructor,",
      '    () => { throw new Error("expected WebSocket construction failure"); },',
      "  ),",
      ");",
      "const exit = await Effect.runPromiseExit(",
      "  Effect.scoped(",
      "    factory.connect({",
      '      environmentId: Schema.decodeUnknownSync(EnvironmentId)("environment-1"),',
      '      label: "probe",',
      '      socketUrl: "ws://127.0.0.1:1",',
      "    }),",
      "  ),",
      ");",
      'if (!Exit.isFailure(exit)) throw new Error("failing WebSocket unexpectedly connected");',
      "const failure = Option.getOrThrow(Cause.findErrorOption(exit.cause));",
      'if (failure._tag !== "ConnectionTransientError" || failure.reason !== "transport") {',
      "  throw new Error(`unexpected typed failure: ${JSON.stringify(failure)}`);",
      "}",
      "console.log(JSON.stringify(failure));",
      "",
    ].join("\n"),
  );

  run("npm", ["install", "--ignore-scripts", "--no-audit", "--no-fund", "--package-lock=false"], {
    cwd: consumerRoot,
    env: {
      ...process.env,
      npm_config_audit: "false",
      npm_config_fund: "false",
    },
  });
  run(tsgoPath, ["--project", NodePath.join(consumerRoot, "tsconfig.json")], {
    cwd: consumerRoot,
  });

  const negative = NodeChildProcess.spawnSync(
    tsgoPath,
    ["--project", NodePath.join(consumerRoot, "tsconfig.negative.json")],
    {
      cwd: consumerRoot,
      encoding: "utf8",
    },
  );
  if (negative.error !== undefined) {
    throw negative.error;
  }
  const negativeOutput = `${negative.stdout}${negative.stderr}`;
  assert(negative.status !== 0, "Explicit undefined unexpectedly passed strict typechecking.");
  assert(
    negativeOutput.includes("ignoreWhitespace") && negativeOutput.includes("undefined"),
    `Negative typecheck failed for the wrong reason:\n${negativeOutput}`,
  );

  const runtimeOutput = run(process.execPath, [NodePath.join(consumerRoot, "smoke.ts")], {
    cwd: consumerRoot,
  }).trim();
  assert(
    runtimeOutput.includes('"_tag":"ConnectionTransientError"') &&
      runtimeOutput.includes('"reason":"transport"'),
    `Executed runtime did not return the typed transport failure:\n${runtimeOutput}`,
  );

  process.stdout.write(
    `${JSON.stringify({
      artifact: artifactName,
      bytes: firstReceipt.bytes,
      declarationBytes: Buffer.byteLength(declarations),
      javascriptBytes: Buffer.byteLength(javascript),
      publishable: false,
      runtimeFailure: JSON.parse(runtimeOutput) as unknown,
      sha256: firstReceipt.sha256,
      sourceRevision: expectedRevision,
      toolchain: firstReceipt.toolchain,
    })}\n`,
  );
} finally {
  NodeFS.rmSync(tempRoot, { force: true, recursive: true });
}
