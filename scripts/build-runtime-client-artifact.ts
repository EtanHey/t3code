#!/usr/bin/env node

// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { T3_RPC_COMPATIBILITY, T3_RPC_EFFECT_VERSION } from "@t3tools/contracts/runtime-client";
import { build } from "vite-plus/pack";

const PACKAGE_NAME = "@t3tools/runtime-client";
const PACKAGE_VERSION = "0.0.31-rpc.1";
const EFFECT_PATCH_SHA256 = "71215759e1ac0a7f65d7b75d816986687ae6c3a6cba02d928d184ca71790d488";
const EFFECT_PATCH_MARKER = "effect/rpc/RpcClient/RequestHooks";
const expectedStageInventory = ["LICENSE", "dist/index.d.mts", "dist/index.mjs", "package.json"];

const repoRoot = NodePath.resolve(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)), "..");
const packageDir = NodePath.join(repoRoot, "packages/runtime-client-artifact");
const patchPath = NodePath.join(repoRoot, `patches/effect@${T3_RPC_EFFECT_VERSION}.patch`);
const declarationConfigPath = NodePath.join(packageDir, "tsconfig.declarations.json");
const tsgoPath = NodePath.join(repoRoot, "node_modules/.bin/tsgo");

export interface BuildOptions {
  readonly allowDirty: boolean;
  readonly outputDir: string;
}

export interface Provenance {
  readonly publishable: boolean;
  readonly sourceRevision: string;
}

interface PackedManifest {
  readonly name: string;
  readonly version: string;
  readonly description: string;
  readonly license: "MIT";
  readonly type: "module";
  readonly files: ReadonlyArray<string>;
  readonly exports: {
    readonly ".": {
      readonly types: string;
      readonly import: string;
    };
  };
  readonly peerDependencies: {
    readonly effect: typeof T3_RPC_EFFECT_VERSION;
  };
  readonly engines: {
    readonly node: string;
  };
  readonly t3Publishable: boolean;
  readonly t3RpcCompatibility: ReturnType<typeof makeCompatibilityMetadata>;
  readonly t3SourceRevision: string;
}

export function parseOptions(args: ReadonlyArray<string>): BuildOptions {
  let allowDirty = false;
  let outputDir = NodePath.join(packageDir, "dist");

  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    if (argument === "--allow-dirty") {
      allowDirty = true;
      continue;
    }
    if (argument === "--output-dir") {
      const value = args[index + 1];
      if (value === undefined) {
        throw new Error(
          "Usage: build-runtime-client-artifact.ts [--allow-dirty] [--output-dir <directory>]",
        );
      }
      outputDir = NodePath.resolve(value);
      index += 1;
      continue;
    }
    throw new Error(
      "Usage: build-runtime-client-artifact.ts [--allow-dirty] [--output-dir <directory>]",
    );
  }

  return { allowDirty, outputDir };
}

export function resolveProvenance(head: string, status: string, allowDirty: boolean): Provenance {
  if (!/^[0-9a-f]{40}$/i.test(head)) {
    throw new Error(`Git HEAD is not a full commit SHA: '${head}'.`);
  }
  const dirty = status.trim().length > 0;
  if (dirty && !allowDirty) {
    throw new Error(
      "Runtime-client artifact build refused a dirty worktree. Review builds require explicit --allow-dirty.",
    );
  }
  return {
    publishable: !dirty,
    sourceRevision: dirty ? `${head}-dirty` : head,
  };
}

export function makeCompatibilityMetadata() {
  return {
    ...T3_RPC_COMPATIBILITY,
    effectVersion: T3_RPC_EFFECT_VERSION,
    effectPatchSha256: EFFECT_PATCH_SHA256,
  } as const;
}

export function makePackedManifest(provenance: Provenance): PackedManifest {
  return {
    name: PACKAGE_NAME,
    version: PACKAGE_VERSION,
    description: "Standalone, compatibility-pinned T3 Effect RPC client runtime.",
    license: "MIT",
    type: "module",
    files: ["dist"],
    exports: {
      ".": {
        types: "./dist/index.d.mts",
        import: "./dist/index.mjs",
      },
    },
    peerDependencies: {
      effect: T3_RPC_EFFECT_VERSION,
    },
    engines: {
      node: ">=24.13.1",
    },
    t3Publishable: provenance.publishable,
    t3RpcCompatibility: makeCompatibilityMetadata(),
    t3SourceRevision: provenance.sourceRevision,
  };
}

export function assertEffectVersion(actualVersion: string): void {
  if (actualVersion !== T3_RPC_EFFECT_VERSION) {
    throw new Error(
      `Effect version mismatch: expected ${T3_RPC_EFFECT_VERSION}, received ${actualVersion}.`,
    );
  }
}

export function assertPatchedBundle(contents: string): void {
  if (!contents.includes(EFFECT_PATCH_MARKER)) {
    throw new Error(`Built runtime is missing Effect patch marker '${EFFECT_PATCH_MARKER}'.`);
  }
}

export function assertPathContained(parent: string, candidate: string): void {
  const relative = NodePath.relative(NodePath.resolve(parent), NodePath.resolve(candidate));
  if (relative === "" || relative === ".." || relative.startsWith(`..${NodePath.sep}`)) {
    throw new Error(`Artifact path is outside temporary staging root: ${candidate}`);
  }
}

export function readRequiredToolVersion(command: string, args: ReadonlyArray<string>): string {
  const result = NodeChildProcess.spawnSync(command, args, { encoding: "utf8" });
  if (result.error !== undefined) {
    const error = result.error as NodeJS.ErrnoException;
    if (error.code === "ENOENT") {
      throw new Error(`Required tool '${command}' is unavailable on PATH.`);
    }
    throw error;
  }
  if (result.status !== 0) {
    throw new Error(
      `Required tool '${command}' failed its version check with exit code ${String(result.status)}.`,
    );
  }
  const output = `${result.stdout}${result.stderr}`.trim();
  const firstLine = output.split(/\r?\n/, 1)[0];
  if (firstLine === undefined || firstLine.length === 0) {
    throw new Error(`Required tool '${command}' returned an empty version.`);
  }
  return firstLine;
}

function sha256File(path: string): string {
  return NodeCrypto.createHash("sha256").update(NodeFS.readFileSync(path)).digest("hex");
}

function listFiles(root: string): ReadonlyArray<string> {
  const entries: Array<string> = [];
  const visit = (directory: string): void => {
    for (const entry of NodeFS.readdirSync(directory, { withFileTypes: true })) {
      const absolutePath = NodePath.join(directory, entry.name);
      if (entry.isDirectory()) {
        visit(absolutePath);
      } else if (entry.isFile()) {
        entries.push(NodePath.relative(root, absolutePath).split(NodePath.sep).join("/"));
      } else {
        throw new Error(`Artifact staging contains a non-file entry: ${absolutePath}`);
      }
    }
  };
  visit(root);
  return entries.sort();
}

function packInventory(tarballPath: string): ReadonlyArray<string> {
  return NodeChildProcess.execFileSync("tar", ["-tzf", tarballPath], {
    cwd: repoRoot,
    encoding: "utf8",
  })
    .trim()
    .split(/\r?\n/)
    .filter((entry) => entry.length > 0)
    .sort();
}

function writeJson(path: string, value: unknown): void {
  NodeFS.writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
}

export function assertPackedMembersClean(
  root: string,
  forbiddenPaths: ReadonlyArray<string> = [],
): void {
  const forbidden = [
    "workspace:",
    "catalog:",
    "sourceMappingURL",
    repoRoot,
    ...forbiddenPaths,
  ].filter((marker, index, markers) => marker.length > 0 && markers.indexOf(marker) === index);

  for (const relativePath of listFiles(root)) {
    const contents = NodeFS.readFileSync(NodePath.join(root, relativePath), "utf8");
    for (const marker of forbidden) {
      if (contents.includes(marker)) {
        throw new Error(`Packed ${relativePath} contains forbidden marker '${marker}'.`);
      }
    }
    const privateImport = contents.match(/(?:\bfrom\s*|\bimport\s*\(?\s*)["']@t3tools\/[^"']+["']/);
    if (privateImport !== null) {
      throw new Error(
        `Packed ${relativePath} contains an unresolved private-package import: ${privateImport[0]}`,
      );
    }
    if (/import\s*\{\s*Schema\s*\}\s*from\s*["']effect["']/.test(contents)) {
      throw new Error(`Packed ${relativePath} contains a prohibited Effect barrel import.`);
    }
    if (/^\/\/#(?:end)?region\b/m.test(contents)) {
      throw new Error(`Packed ${relativePath} contains a declaration generator region marker.`);
    }
  }
}

function gitText(args: ReadonlyArray<string>): string {
  return NodeChildProcess.execFileSync("git", args, {
    cwd: repoRoot,
    encoding: "utf8",
  }).trim();
}

function installedEffect(): { readonly directory: string; readonly version: string } {
  const packageRequire = NodeModule.createRequire(NodePath.join(packageDir, "package.json"));
  const packageJsonPath = packageRequire.resolve("effect/package.json");
  const packageJson = JSON.parse(NodeFS.readFileSync(packageJsonPath, "utf8")) as {
    readonly version?: unknown;
  };
  if (typeof packageJson.version !== "string") {
    throw new Error(`Installed Effect package has no string version: ${packageJsonPath}`);
  }
  return {
    directory: NodePath.dirname(packageJsonPath),
    version: packageJson.version,
  };
}

function normalizedDeclarations(contents: string): string {
  return contents
    .replace(/^\/\/#(?:end)?region.*(?:\r?\n|$)/gm, "")
    .replace(
      /^import\s*\{\s*Schema\s*\}\s*from\s*["']effect["'];?$/gm,
      'import type * as Schema from "effect/Schema";',
    );
}

export async function buildArtifact(options: BuildOptions): Promise<void> {
  const provenance = resolveProvenance(
    gitText(["rev-parse", "HEAD"]),
    gitText(["status", "--porcelain=v1", "--untracked-files=normal"]),
    options.allowDirty,
  );
  const toolchain = {
    node: process.version,
    npm: readRequiredToolVersion("npm", ["--version"]),
    tar: readRequiredToolVersion("tar", ["--version"]),
  };

  const actualPatchSha256 = sha256File(patchPath);
  if (actualPatchSha256 !== EFFECT_PATCH_SHA256) {
    throw new Error(
      `Effect patch checksum changed: expected ${EFFECT_PATCH_SHA256}, received ${actualPatchSha256}.`,
    );
  }
  const effect = installedEffect();
  assertEffectVersion(effect.version);
  assertPatchedBundle(
    NodeFS.readFileSync(NodePath.join(effect.directory, "dist/unstable/rpc/RpcClient.js"), "utf8"),
  );

  const tempRoot = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-runtime-artifact-"));
  const stageDir = NodePath.join(tempRoot, "stage");
  const stageDistDir = NodePath.join(stageDir, "dist");
  const declarationDir = NodePath.join(tempRoot, "declarations");
  const declarationBundleDir = NodePath.join(tempRoot, "declaration-bundle");
  const packDir = NodePath.join(tempRoot, "pack");

  try {
    for (const path of [stageDir, stageDistDir, declarationDir, declarationBundleDir, packDir]) {
      assertPathContained(tempRoot, path);
    }
    NodeFS.mkdirSync(stageDir, { recursive: true });
    NodeFS.mkdirSync(packDir, { recursive: true });

    await build({
      entry: [NodePath.join(packageDir, "src/index.ts")],
      outDir: stageDistDir,
      clean: true,
      format: ["esm"],
      platform: "node",
      target: "node24",
      treeshake: true,
      minify: true,
      sourcemap: false,
      dts: false,
      report: false,
      deps: {
        alwaysBundle: [/^effect(?:\/|$)/, /^@t3tools\//],
        onlyBundle: false,
      },
    });
    assertPatchedBundle(NodeFS.readFileSync(NodePath.join(stageDistDir, "index.mjs"), "utf8"));

    NodeChildProcess.execFileSync(
      tsgoPath,
      ["--project", declarationConfigPath, "--outDir", declarationDir],
      {
        cwd: packageDir,
        stdio: "pipe",
      },
    );
    await build({
      entry: [NodePath.join(declarationDir, "packages/runtime-client-artifact/src/index.d.ts")],
      outDir: declarationBundleDir,
      clean: true,
      format: ["esm"],
      platform: "node",
      report: false,
      deps: {
        neverBundle: (id) => id === "effect" || id.startsWith("effect/"),
      },
      alias: {
        "@t3tools/client-runtime/runtime-client": NodePath.join(
          declarationDir,
          "packages/client-runtime/src/rpc/runtimeClient.d.ts",
        ),
        "@t3tools/contracts/runtime-client": NodePath.join(
          declarationDir,
          "packages/contracts/src/runtimeClient.d.ts",
        ),
      },
      dts: {
        dtsInput: true,
        emitDtsOnly: true,
        newContext: true,
        resolver: "tsc",
      },
    });
    const bundledDeclarations = normalizedDeclarations(
      NodeFS.readFileSync(NodePath.join(declarationBundleDir, "index.d.ts"), "utf8"),
    );
    NodeFS.writeFileSync(NodePath.join(stageDistDir, "index.d.mts"), bundledDeclarations);

    NodeFS.copyFileSync(NodePath.join(repoRoot, "LICENSE"), NodePath.join(stageDir, "LICENSE"));
    writeJson(NodePath.join(stageDir, "package.json"), makePackedManifest(provenance));

    const stageInventory = listFiles(stageDir);
    if (JSON.stringify(stageInventory) !== JSON.stringify(expectedStageInventory)) {
      throw new Error(
        `Unexpected artifact stage inventory: ${JSON.stringify(stageInventory, null, 2)}`,
      );
    }
    assertPackedMembersClean(stageDir, [tempRoot]);

    NodeChildProcess.execFileSync(
      "npm",
      ["pack", "--ignore-scripts", "--pack-destination", packDir],
      {
        cwd: stageDir,
        env: {
          ...process.env,
          npm_config_audit: "false",
          npm_config_fund: "false",
        },
        stdio: "pipe",
      },
    );

    const packed = NodeFS.readdirSync(packDir).filter((entry) => entry.endsWith(".tgz"));
    if (packed.length !== 1 || packed[0] === undefined) {
      throw new Error(`Expected one packed tarball, received ${packed.length}.`);
    }

    NodeFS.mkdirSync(options.outputDir, { recursive: true });
    const artifactName = packed[0];
    const artifactPath = NodePath.join(options.outputDir, artifactName);
    NodeFS.copyFileSync(NodePath.join(packDir, artifactName), artifactPath);

    const artifactSha256 = sha256File(artifactPath);
    const inventory = packInventory(artifactPath);
    const receipt = {
      artifact: artifactName,
      bytes: NodeFS.statSync(artifactPath).size,
      compatibility: makeCompatibilityMetadata(),
      inventory,
      packageName: PACKAGE_NAME,
      publishable: provenance.publishable,
      sha256: artifactSha256,
      sourceRevision: provenance.sourceRevision,
      toolchain,
      version: PACKAGE_VERSION,
    };
    writeJson(NodePath.join(options.outputDir, `${artifactName}.json`), receipt);
    NodeFS.writeFileSync(
      NodePath.join(options.outputDir, `${artifactName}.sha256`),
      `${artifactSha256}  ${artifactName}\n`,
    );
    process.stdout.write(`${JSON.stringify(receipt)}\n`);
  } finally {
    NodeFS.rmSync(tempRoot, { recursive: true, force: true });
  }
}

if (import.meta.main) {
  await buildArtifact(parseOptions(process.argv.slice(2)));
}
