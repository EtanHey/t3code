// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { assert, describe, it } from "@effect/vitest";
import { T3_RPC_COMPATIBILITY, T3_RPC_EFFECT_VERSION } from "@t3tools/contracts/runtime-client";

const repoRoot = NodePath.resolve(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)), "..");
const artifactScript = NodePath.join(repoRoot, "scripts/build-runtime-client-artifact.ts");
const currentRuntimePackageDir = NodePath.join(repoRoot, "packages/client-runtime");
const artifactSourcePackagePath = NodePath.join(
  repoRoot,
  "packages/runtime-client-artifact/package.json",
);

function readJson(path: string): Record<string, unknown> {
  return JSON.parse(NodeFS.readFileSync(path, "utf8")) as Record<string, unknown>;
}

interface BuilderModule {
  readonly assertEffectVersion?: (version: string) => void;
  readonly assertPackedMembersClean?: (
    root: string,
    forbiddenPaths?: ReadonlyArray<string>,
  ) => void;
  readonly assertPatchedBundle?: (contents: string) => void;
  readonly assertPathContained?: (parent: string, candidate: string) => void;
  readonly makeCompatibilityMetadata?: () => Record<string, unknown>;
  readonly makePackedManifest?: (provenance: {
    readonly sourceRevision: string;
    readonly publishable: boolean;
  }) => Record<string, unknown>;
  readonly parseOptions?: (args: ReadonlyArray<string>) => {
    readonly allowDirty: boolean;
    readonly outputDir: string;
  };
  readonly readRequiredToolVersion?: (command: string, args: ReadonlyArray<string>) => string;
  readonly resolveProvenance?: (
    head: string,
    status: string,
    allowDirty: boolean,
  ) => { readonly sourceRevision: string; readonly publishable: boolean };
}

async function loadBuilder(): Promise<BuilderModule> {
  return (await import(
    `${NodeURL.pathToFileURL(artifactScript).href}?unit-contract`
  )) as BuilderModule;
}

describe("runtime-client artifact unit contract", () => {
  it("can be imported without executing an artifact build", async () => {
    const originalArgv = process.argv;
    try {
      process.argv = [process.execPath, artifactScript, "--unit-test-import"];
      await import(`${NodeURL.pathToFileURL(artifactScript).href}?unit-test-import`);
    } finally {
      process.argv = originalArgv;
    }
  });

  it("preserves the portable P2 source-package distribution proof", () => {
    const manifest = readJson(NodePath.join(currentRuntimePackageDir, "package.json"));
    const dependencies = manifest.dependencies as Record<string, unknown>;
    const exports = manifest.exports as Record<string, { readonly default?: unknown }>;
    const dryRun = NodeChildProcess.spawnSync(
      "bun",
      ["pm", "pack", "--dry-run", "--ignore-scripts"],
      {
        cwd: currentRuntimePackageDir,
        encoding: "utf8",
      },
    );

    assert.equal(manifest.private, true);
    assert.equal(manifest.version, undefined);
    assert.equal(exports["./runtime-client"]?.default, "./src/rpc/runtimeClient.ts");
    assert.deepStrictEqual(dependencies, {
      "@t3tools/contracts": "workspace:*",
      "@t3tools/shared": "workspace:*",
      effect: "catalog:",
    });
    if (dryRun.error === undefined) {
      assert.notEqual(dryRun.status, 0);
    } else {
      assert.equal((dryRun.error as NodeJS.ErrnoException).code, "ENOENT");
    }
  });

  it("keeps artifact packing out of the root build graph", () => {
    const manifest = readJson(artifactSourcePackagePath);
    const rootManifest = readJson(NodePath.join(repoRoot, "package.json"));
    const scripts = manifest.scripts as Record<string, unknown>;
    const rootScripts = rootManifest.scripts as Record<string, unknown>;

    assert.equal(manifest.name, "@t3tools/runtime-client-artifact-source");
    assert.equal(manifest.version, undefined);
    assert.equal(manifest.exports, undefined);
    assert.equal(scripts.build, undefined);
    assert.equal(scripts.artifact, "node ../../scripts/build-runtime-client-artifact.ts");
    assert.equal(
      scripts["artifact:test"],
      "node ../../scripts/build-runtime-client-artifact.integration.ts",
    );
    for (const scriptName of ["build", "test"]) {
      const rootScript = rootScripts[scriptName];
      assert.equal(typeof rootScript, "string");
      assert.notInclude(rootScript as string, "runtime-client-artifact");
      assert.notInclude(rootScript as string, "npm install");
      assert.notInclude(rootScript as string, "npm pack");
    }
  });

  it("uses only the canonical runtime-client export-map boundary", () => {
    const artifactEntry = NodeFS.readFileSync(
      NodePath.join(repoRoot, "packages/runtime-client-artifact/src/index.ts"),
      "utf8",
    );
    const canonicalSources = [
      "packages/client-runtime/src/rpc/session.ts",
      "packages/client-runtime/src/rpc/protocol.ts",
      "packages/client-runtime/src/connection/model.ts",
    ].map((relativePath) => NodeFS.readFileSync(NodePath.join(repoRoot, relativePath), "utf8"));

    assert.include(artifactEntry, '"@t3tools/contracts/runtime-client"');
    assert.include(artifactEntry, '"@t3tools/client-runtime/runtime-client"');
    assert.notInclude(artifactEntry, "../../contracts/src/");
    assert.notInclude(artifactEntry, "../../client-runtime/src/");
    for (const source of canonicalSources) {
      assert.notMatch(source, /from ["']@t3tools\/contracts["']/);
      assert.include(source, '"@t3tools/contracts/runtime-client"');
    }
  });

  it("requires explicit review mode for dirty provenance", async () => {
    const builder = await loadBuilder();
    assert.isFunction(builder.resolveProvenance);

    const head = "a".repeat(40);
    assert.deepStrictEqual(builder.resolveProvenance?.(head, "", false), {
      publishable: true,
      sourceRevision: head,
    });
    assert.throws(
      () => builder.resolveProvenance?.(head, " M scripts/example.ts", false),
      /dirty.*--allow-dirty/i,
    );
    assert.deepStrictEqual(builder.resolveProvenance?.(head, " M scripts/example.ts", true), {
      publishable: false,
      sourceRevision: `${head}-dirty`,
    });
  });

  it("parses review mode without making it the default", async () => {
    const builder = await loadBuilder();
    assert.isFunction(builder.parseOptions);

    assert.equal(builder.parseOptions?.([]).allowDirty, false);
    assert.equal(builder.parseOptions?.(["--allow-dirty"]).allowDirty, true);
    assert.equal(
      builder.parseOptions?.(["--output-dir", "artifact-output", "--allow-dirty"]).allowDirty,
      true,
    );
  });

  it("derives compatibility and manifest metadata from canonical constants", async () => {
    const builder = await loadBuilder();
    assert.isFunction(builder.makeCompatibilityMetadata);
    assert.isFunction(builder.makePackedManifest);

    const metadata = builder.makeCompatibilityMetadata?.();
    assert.deepInclude(metadata, T3_RPC_COMPATIBILITY);
    assert.equal(metadata?.effectVersion, T3_RPC_EFFECT_VERSION);

    const manifest = builder.makePackedManifest?.({
      publishable: true,
      sourceRevision: "b".repeat(40),
    });
    assert.equal(manifest?.license, "MIT");
    assert.equal(manifest?.dependencies, undefined);
    assert.deepStrictEqual(manifest?.peerDependencies, {
      effect: T3_RPC_EFFECT_VERSION,
    });
    assert.equal(manifest?.t3Publishable, true);
  });

  it("rejects the wrong Effect version and a bundle without the patch marker", async () => {
    const builder = await loadBuilder();
    assert.isFunction(builder.assertEffectVersion);
    assert.isFunction(builder.assertPatchedBundle);

    assert.doesNotThrow(() => builder.assertEffectVersion?.(T3_RPC_EFFECT_VERSION));
    assert.throws(() => builder.assertEffectVersion?.("4.0.0-beta.101"), /Effect version mismatch/);
    assert.doesNotThrow(() => builder.assertPatchedBundle?.('"effect/rpc/RpcClient/RequestHooks"'));
    assert.throws(() => builder.assertPatchedBundle?.("unpatched bundle"), /patch marker/);
  });

  it("confines staging and inspects extracted packed members", async () => {
    const builder = await loadBuilder();
    assert.isFunction(builder.assertPathContained);
    assert.isFunction(builder.assertPackedMembersClean);

    const tempRoot = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "artifact-unit-"));
    try {
      const stage = NodePath.join(tempRoot, "stage");
      NodeFS.mkdirSync(NodePath.join(stage, "dist"), { recursive: true });
      NodeFS.writeFileSync(NodePath.join(stage, "package.json"), '{"license":"MIT"}\n');
      NodeFS.writeFileSync(NodePath.join(stage, "dist/index.mjs"), "export {};\n");
      NodeFS.writeFileSync(
        NodePath.join(stage, "dist/index.d.mts"),
        'export type Ready = "ready";\n',
      );

      assert.doesNotThrow(() => builder.assertPathContained?.(tempRoot, stage));
      assert.throws(
        () => builder.assertPathContained?.(stage, tempRoot),
        /outside temporary staging root/,
      );
      assert.doesNotThrow(() => builder.assertPackedMembersClean?.(stage, [tempRoot]));

      NodeFS.writeFileSync(
        NodePath.join(stage, "dist/index.d.mts"),
        `export type LeakedPath = "${tempRoot}";\n`,
      );
      assert.throws(
        () => builder.assertPackedMembersClean?.(stage, [tempRoot]),
        /forbidden marker/,
      );

      NodeFS.writeFileSync(
        NodePath.join(stage, "dist/index.d.mts"),
        "//#region generated\nexport type Ready = true;\n//#endregion\n",
      );
      assert.throws(
        () => builder.assertPackedMembersClean?.(stage, [tempRoot]),
        /generator region marker/,
      );

      NodeFS.writeFileSync(
        NodePath.join(stage, "dist/index.d.mts"),
        'export * from "@t3tools/contracts";\n',
      );
      assert.throws(
        () => builder.assertPackedMembersClean?.(stage, [tempRoot]),
        /private-package import/,
      );
    } finally {
      NodeFS.rmSync(tempRoot, { force: true, recursive: true });
    }
  });

  it("reports a clear error when a required pack tool is unavailable", async () => {
    const builder = await loadBuilder();
    assert.isFunction(builder.readRequiredToolVersion);

    assert.throws(
      () => builder.readRequiredToolVersion?.("__t3_missing_pack_tool__", ["--version"]),
      /Required tool '__t3_missing_pack_tool__' is unavailable on PATH/,
    );
  });

  it("uses the strict repository declaration configuration", () => {
    const config = readJson(
      NodePath.join(repoRoot, "packages/runtime-client-artifact/tsconfig.declarations.json"),
    );
    const compilerOptions = config.compilerOptions as Record<string, unknown>;
    const baseConfig = readJson(NodePath.join(repoRoot, "tsconfig.base.json"));
    const baseCompilerOptions = baseConfig.compilerOptions as Record<string, unknown>;
    const builderSource = NodeFS.readFileSync(artifactScript, "utf8");

    assert.equal(config.extends, "../../tsconfig.base.json");
    assert.equal(compilerOptions.declaration, true);
    assert.equal(compilerOptions.emitDeclarationOnly, true);
    assert.equal(baseCompilerOptions.strict, true);
    assert.equal(baseCompilerOptions.exactOptionalPropertyTypes, true);
    assert.notInclude(builderSource, "--ignoreConfig");
  });
});
