// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { assert, describe, it } from "@effect/vitest";

const repoRoot = NodePath.resolve(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)), "..");
const verifierPath = NodePath.join(repoRoot, "scripts/verify-runtime-client-release.ts");
const workflowPath = NodePath.join(repoRoot, ".github/workflows/runtime-client-release.yml");
const integrationPath = NodePath.join(
  repoRoot,
  "scripts/build-runtime-client-artifact.integration.ts",
);

interface ReleaseRequest {
  readonly publish: boolean;
  readonly releaseTag: string;
  readonly reviewedSha256?: string;
  readonly sourceRevision: string;
  readonly version: string;
}

interface VerifiedReleaseAssets {
  readonly artifactName: string;
  readonly artifactPath: string;
  readonly checksumPath: string;
  readonly receiptPath: string;
  readonly sha256: string;
}

interface VerifierModule {
  readonly validateReleaseRequest?: (request: ReleaseRequest) => ReleaseRequest;
  readonly verifyReleaseAssets?: (
    outputDir: string,
    request: ReleaseRequest,
  ) => VerifiedReleaseAssets;
}

async function loadVerifier(): Promise<VerifierModule> {
  assert.isTrue(NodeFS.existsSync(verifierPath), "release verifier must exist");
  return (await import(
    `${NodeURL.pathToFileURL(verifierPath).href}?unit-contract`
  )) as VerifierModule;
}

function sha256(contents: Buffer): string {
  return NodeCrypto.createHash("sha256").update(contents).digest("hex");
}

function writeReleaseAssets(
  outputDir: string,
  request: ReleaseRequest,
  overrides: Readonly<Record<string, unknown>> = {},
): { readonly artifactName: string; readonly digest: string } {
  const artifactName = `t3tools-runtime-client-${request.version}.tgz`;
  const artifact = Buffer.from("reviewed runtime client\n");
  const digest = sha256(artifact);
  const receipt = {
    artifact: artifactName,
    bytes: artifact.byteLength,
    compatibility: {
      contractFingerprint: "t3-rpc-v1:effect@4.0.0-beta.102:json-websocket",
      effectPatchSha256: "71215759e1ac0a7f65d7b75d816986687ae6c3a6cba02d928d184ca71790d488",
      effectVersion: "4.0.0-beta.102",
      protocol: "effect-rpc",
      serialization: "json",
      transport: "websocket",
    },
    inventory: [
      "package/LICENSE",
      "package/dist/index.d.mts",
      "package/dist/index.mjs",
      "package/package.json",
    ],
    packageName: "@t3tools/runtime-client",
    publishable: true,
    sha256: digest,
    sourceRevision: request.sourceRevision,
    toolchain: {
      node: "v24.13.1",
      npm: "11.10.0",
      tar: "bsdtar 3.5.3",
    },
    version: request.version,
    ...overrides,
  };

  NodeFS.mkdirSync(outputDir, { recursive: true });
  NodeFS.writeFileSync(NodePath.join(outputDir, artifactName), artifact);
  NodeFS.writeFileSync(
    NodePath.join(outputDir, `${artifactName}.json`),
    `${JSON.stringify(receipt, null, 2)}\n`,
  );
  NodeFS.writeFileSync(
    NodePath.join(outputDir, `${artifactName}.sha256`),
    `${digest}  ${artifactName}\n`,
  );
  return { artifactName, digest };
}

describe("runtime-client immutable release contract", () => {
  it("validates a version-parameterized dry run and requires reviewed bytes before publishing", async () => {
    const verifier = await loadVerifier();
    assert.isFunction(verifier.validateReleaseRequest);

    const sourceRevision = "a".repeat(40);
    const dryRun = {
      publish: false,
      releaseTag: "runtime-client-v0.0.32-rpc.2",
      sourceRevision,
      version: "0.0.32-rpc.2",
    };
    assert.deepStrictEqual(verifier.validateReleaseRequest?.(dryRun), dryRun);
    assert.throws(
      () => verifier.validateReleaseRequest?.({ ...dryRun, publish: true }),
      /reviewed sha256/i,
    );
    assert.doesNotThrow(() =>
      verifier.validateReleaseRequest?.({
        ...dryRun,
        publish: true,
        reviewedSha256: "b".repeat(64),
      }),
    );
  });

  it("rejects malformed or inconsistent release coordinates", async () => {
    const verifier = await loadVerifier();
    assert.isFunction(verifier.validateReleaseRequest);

    const request = {
      publish: false,
      releaseTag: "runtime-client-v1.2.3-rc.1",
      sourceRevision: "c".repeat(40),
      version: "1.2.3-rc.1",
    };
    assert.throws(
      () => verifier.validateReleaseRequest?.({ ...request, version: "../escape" }),
      /version/i,
    );
    assert.throws(
      () => verifier.validateReleaseRequest?.({ ...request, releaseTag: "v1.2.3-rc.1" }),
      /release tag/i,
    );
    assert.throws(
      () => verifier.validateReleaseRequest?.({ ...request, sourceRevision: "main" }),
      /source revision/i,
    );
  });

  it("verifies the exact three publishable assets and their reviewed checksum", async () => {
    const verifier = await loadVerifier();
    assert.isFunction(verifier.verifyReleaseAssets);

    const tempRoot = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "runtime-release-unit-"));
    const request: ReleaseRequest = {
      publish: true,
      releaseTag: "runtime-client-v0.0.32-rpc.2",
      reviewedSha256: "",
      sourceRevision: "d".repeat(40),
      version: "0.0.32-rpc.2",
    };
    try {
      const { artifactName, digest } = writeReleaseAssets(tempRoot, request);
      const verified = verifier.verifyReleaseAssets?.(tempRoot, {
        ...request,
        reviewedSha256: digest,
      });
      assert.equal(verified?.artifactName, artifactName);
      assert.equal(verified?.sha256, digest);
      assert.equal(verified?.artifactPath, NodePath.join(tempRoot, artifactName));
    } finally {
      NodeFS.rmSync(tempRoot, { force: true, recursive: true });
    }
  });

  it("fails closed on unpublishable provenance, checksum drift, or unexpected files", async () => {
    const verifier = await loadVerifier();
    assert.isFunction(verifier.verifyReleaseAssets);

    const request: ReleaseRequest = {
      publish: false,
      releaseTag: "runtime-client-v2.0.0-beta.1",
      sourceRevision: "e".repeat(40),
      version: "2.0.0-beta.1",
    };
    const tempRoot = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "runtime-release-unit-"));
    try {
      const { artifactName } = writeReleaseAssets(tempRoot, request, { publishable: false });
      assert.throws(() => verifier.verifyReleaseAssets?.(tempRoot, request), /not publishable/i);

      writeReleaseAssets(tempRoot, request);
      NodeFS.writeFileSync(
        NodePath.join(tempRoot, `${artifactName}.sha256`),
        `${"0".repeat(64)}\n`,
      );
      assert.throws(() => verifier.verifyReleaseAssets?.(tempRoot, request), /checksum/i);

      writeReleaseAssets(tempRoot, request);
      NodeFS.writeFileSync(NodePath.join(tempRoot, "unexpected.txt"), "no\n");
      assert.throws(() => verifier.verifyReleaseAssets?.(tempRoot, request), /exactly three/i);

      NodeFS.rmSync(NodePath.join(tempRoot, "unexpected.txt"));
      writeReleaseAssets(tempRoot, request, { toolchain: null });
      assert.throws(() => verifier.verifyReleaseAssets?.(tempRoot, request), /toolchain/i);
    } finally {
      NodeFS.rmSync(tempRoot, { force: true, recursive: true });
    }
  });

  it("rejects a receipt whose artifact inventory or RPC/Effect compatibility drifts", async () => {
    const verifier = await loadVerifier();
    assert.isFunction(verifier.verifyReleaseAssets);

    const request: ReleaseRequest = {
      publish: false,
      releaseTag: "runtime-client-v2.0.0-rpc.2",
      sourceRevision: "f".repeat(40),
      version: "2.0.0-rpc.2",
    };
    const tempRoot = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "runtime-release-unit-"));
    try {
      writeReleaseAssets(tempRoot, request, { inventory: ["package/package.json"] });
      assert.throws(() => verifier.verifyReleaseAssets?.(tempRoot, request), /inventory/i);

      writeReleaseAssets(tempRoot, request, {
        compatibility: {
          contractFingerprint: "t3-rpc-v1:effect@4.0.0-beta.101:json-websocket",
          effectPatchSha256: "71215759e1ac0a7f65d7b75d816986687ae6c3a6cba02d928d184ca71790d488",
          effectVersion: "4.0.0-beta.101",
          protocol: "effect-rpc",
          serialization: "json",
          transport: "websocket",
        },
      });
      assert.throws(() => verifier.verifyReleaseAssets?.(tempRoot, request), /compatibility/i);
    } finally {
      NodeFS.rmSync(tempRoot, { force: true, recursive: true });
    }
  });

  it("keeps the CI release path dry-run-first, immutable, and registry-free", () => {
    assert.isTrue(NodeFS.existsSync(workflowPath), "runtime-client release workflow must exist");
    const workflow = NodeFS.readFileSync(workflowPath, "utf8");

    assert.include(workflow, "workflow_dispatch:");
    for (const input of [
      "source_revision:",
      "version:",
      "release_tag:",
      "reviewed_sha256:",
      "publish:",
    ]) {
      assert.include(workflow, input);
    }
    assert.notInclude(workflow, "0.0.31-rpc.1");
    assert.match(workflow, /actions\/checkout@[0-9a-f]{40}/);
    assert.match(workflow, /voidzero-dev\/setup-vp@[0-9a-f]{40}/);
    assert.include(workflow, "ref: refs/heads/main");
    assert.include(workflow, "fetch-depth: 0");
    assert.include(workflow, 'git checkout --detach "$SOURCE_REVISION"');
    assert.include(workflow, 'git merge-base --is-ancestor "$SOURCE_REVISION" origin/main');
    assert.notInclude(workflow, "git fetch --no-tags origin main");
    assert.include(workflow, 'node-version: "24.13.1"');
    assert.include(workflow, "build-runtime-client-artifact.test.ts");
    assert.include(workflow, "verify-runtime-client-release.test.ts");
    assert.include(workflow, "build-runtime-client-artifact.integration.ts");
    assert.include(workflow, "build-runtime-client-artifact.ts --output-dir");
    assert.notInclude(workflow, "--allow-dirty");
    assert.include(workflow, "status --porcelain=v1 --untracked-files=all");
    assert.include(workflow, "immutable-releases");

    const exactSourceGateIndex = workflow.indexOf("- name: Verify exact clean source on main");
    const setupToolchainIndex = workflow.indexOf("- name: Setup pinned repository toolchain");
    assert.isAtLeast(exactSourceGateIndex, 0);
    assert.isAbove(
      setupToolchainIndex,
      exactSourceGateIndex,
      "the requested SHA must be validated and checked out before installation",
    );

    const draftIndex = workflow.indexOf("gh release create");
    const uploadIndex = workflow.indexOf("gh release upload");
    const publishIndex = workflow.indexOf('gh release edit "$RELEASE_TAG" --draft=false');
    const verifyIndex = workflow.indexOf('gh release verify "$RELEASE_TAG"');
    assert.isAtLeast(draftIndex, 0);
    assert.isAbove(uploadIndex, draftIndex);
    assert.isAbove(publishIndex, uploadIndex);
    assert.isAbove(verifyIndex, publishIndex);
    assert.notInclude(workflow, "npm publish");
    assert.notInclude(workflow, "npm.pkg.github.com");
  });

  it("pins repository-compatible checkout v4 before full-depth exact-SHA validation", () => {
    const workflow = NodeFS.readFileSync(workflowPath, "utf8");
    const checkoutPin = "actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683 # v4.2.2";
    const checkoutIndex = workflow.indexOf(`uses: ${checkoutPin}`);
    const fullDepthIndex = workflow.indexOf("fetch-depth: 0", checkoutIndex);
    const exactSourceGateIndex = workflow.indexOf("- name: Verify exact clean source on main");

    assert.deepStrictEqual(
      [...workflow.matchAll(/uses: actions\/checkout@([^\s]+)/g)].map((match) => match[1]),
      ["11bd71901bbe5b1630ceea73d27597364c9af683"],
    );
    assert.isAtLeast(checkoutIndex, 0);
    assert.isAbove(fullDepthIndex, checkoutIndex);
    assert.isAbove(
      exactSourceGateIndex,
      fullDepthIndex,
      "full-history checkout must precede exact source validation",
    );
  });

  it("keeps privileged publication minimal and gates it behind a fixed environment", () => {
    const workflow = NodeFS.readFileSync(workflowPath, "utf8");
    const publishJob = workflow.slice(
      workflow.indexOf("\n  publish:\n    name: Publish immutable release"),
    );

    assert.notInclude(workflow, "publish_environment:");
    assert.include(publishJob, "environment: runtime-client-release");
    assert.include(publishJob, "GH_REPO: ${{ github.repository }}");
    assert.notInclude(publishJob, "voidzero-dev/setup-vp@");
    assert.notInclude(publishJob, "actions/checkout@");
    assert.include(publishJob, "actions/download-artifact@");
  });

  it("re-verifies a draft's downloaded assets and only resumes a matching draft", () => {
    const workflow = NodeFS.readFileSync(workflowPath, "utf8");
    const publishJob = workflow.slice(
      workflow.indexOf("\n  publish:\n    name: Publish immutable release"),
    );

    assert.include(publishJob, "target_commitish");
    assert.include(publishJob, "Existing draft does not target");
    assert.include(publishJob, "Tag '$RELEASE_TAG' collides with a different commit");
    assert.include(publishJob, "gh release delete-asset");
    assert.include(publishJob, 'draft_assets="$(gh release view');
    assert.include(publishJob, 'done <<< "$draft_assets"');
    assert.notInclude(publishJob, "done < <(gh release view");
    assert.include(publishJob, "gh api --include");
    assert.notInclude(publishJob, '*"HTTP 404"*');
    assert.include(publishJob, "gh release download");
    assert.include(publishJob, "cmp --silent");
    assert.isAbove(
      publishJob.indexOf("gh release download"),
      publishJob.indexOf("gh release upload"),
    );
    assert.isAbove(
      publishJob.indexOf('gh release edit "$RELEASE_TAG" --draft=false'),
      publishJob.indexOf("cmp --silent"),
    );
  });

  it("makes distribution verification work for clean CI builds without pinning an old filename", () => {
    const integration = NodeFS.readFileSync(integrationPath, "utf8");

    assert.notInclude(
      integration,
      'const artifactName = "t3tools-runtime-client-0.0.31-rpc.1.tgz"',
    );
    assert.include(integration, '"status", "--porcelain=v1"');
    assert.include(integration, "expectedPublishable");
  });
});
