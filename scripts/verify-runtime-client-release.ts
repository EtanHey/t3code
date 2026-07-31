#!/usr/bin/env node

// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

const PACKAGE_NAME = "@t3tools/runtime-client";
const PACKAGE_FILE_PREFIX = "t3tools-runtime-client-";
const RELEASE_TAG_PREFIX = "runtime-client-v";
const VERSION_PATTERN = /^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z]+(?:\.[0-9A-Za-z]+)*)?$/;
const COMMIT_PATTERN = /^[0-9a-f]{40}$/;
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const CANONICAL_ARTIFACT_INVENTORY = [
  "package/LICENSE",
  "package/dist/index.d.mts",
  "package/dist/index.mjs",
  "package/package.json",
] as const;
const CANONICAL_COMPATIBILITY = {
  contractFingerprint: "t3-rpc-v1:effect@4.0.0-beta.102:json-websocket",
  effectPatchSha256: "71215759e1ac0a7f65d7b75d816986687ae6c3a6cba02d928d184ca71790d488",
  effectVersion: "4.0.0-beta.102",
  protocol: "effect-rpc",
  serialization: "json",
  transport: "websocket",
} as const;

export interface ReleaseRequest {
  readonly publish: boolean;
  readonly releaseTag: string;
  readonly reviewedSha256?: string;
  readonly sourceRevision: string;
  readonly version: string;
}

interface RuntimeClientReceipt {
  readonly artifact: string;
  readonly bytes: number;
  readonly compatibility: Readonly<Record<string, unknown>>;
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

export interface VerifiedReleaseAssets {
  readonly artifactName: string;
  readonly artifactPath: string;
  readonly checksumPath: string;
  readonly receiptPath: string;
  readonly sha256: string;
}

interface CliOptions {
  readonly githubOutput?: string;
  readonly outputDir: string;
  readonly request: ReleaseRequest;
}

function assertNonEmptyString(value: unknown, label: string): asserts value is string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${label} must be a non-empty string.`);
  }
}

function readReceipt(path: string): RuntimeClientReceipt {
  const value = JSON.parse(NodeFS.readFileSync(path, "utf8")) as Partial<RuntimeClientReceipt>;
  assertNonEmptyString(value.artifact, "Receipt artifact");
  assertNonEmptyString(value.packageName, "Receipt package name");
  assertNonEmptyString(value.sha256, "Receipt SHA-256");
  assertNonEmptyString(value.sourceRevision, "Receipt source revision");
  assertNonEmptyString(value.version, "Receipt version");
  if (!Number.isSafeInteger(value.bytes) || (value.bytes ?? 0) <= 0) {
    throw new Error("Receipt byte count must be a positive safe integer.");
  }
  if (typeof value.publishable !== "boolean") {
    throw new Error("Receipt publishable field must be boolean.");
  }
  if (
    value.compatibility === undefined ||
    value.compatibility === null ||
    typeof value.compatibility !== "object" ||
    Array.isArray(value.compatibility)
  ) {
    throw new Error("Receipt compatibility metadata must be an object.");
  }
  if (
    !Array.isArray(value.inventory) ||
    !value.inventory.every((entry) => typeof entry === "string" && entry.length > 0)
  ) {
    throw new Error("Receipt inventory must contain non-empty strings.");
  }
  if (
    value.toolchain === undefined ||
    value.toolchain === null ||
    typeof value.toolchain.node !== "string" ||
    value.toolchain.node.length === 0 ||
    typeof value.toolchain.npm !== "string" ||
    value.toolchain.npm.length === 0 ||
    typeof value.toolchain.tar !== "string" ||
    value.toolchain.tar.length === 0
  ) {
    throw new Error("Receipt toolchain metadata is incomplete.");
  }
  return value as RuntimeClientReceipt;
}

function sha256File(path: string): string {
  return NodeCrypto.createHash("sha256").update(NodeFS.readFileSync(path)).digest("hex");
}

function assertRegularFile(path: string): void {
  const metadata = NodeFS.lstatSync(path);
  if (!metadata.isFile() || metadata.isSymbolicLink()) {
    throw new Error(`Release asset is not a regular file: ${path}`);
  }
}

function assertCanonicalReceiptContract(receipt: RuntimeClientReceipt): void {
  if (JSON.stringify(receipt.inventory) !== JSON.stringify(CANONICAL_ARTIFACT_INVENTORY)) {
    throw new Error(
      `Receipt inventory mismatch: expected ${JSON.stringify(CANONICAL_ARTIFACT_INVENTORY)}, received ${JSON.stringify(receipt.inventory)}.`,
    );
  }

  const compatibility = receipt.compatibility;
  const expectedKeys = Object.keys(CANONICAL_COMPATIBILITY).sort();
  const actualKeys = Object.keys(compatibility).sort();
  if (JSON.stringify(actualKeys) !== JSON.stringify(expectedKeys)) {
    throw new Error(
      `Receipt compatibility keys mismatch: expected ${JSON.stringify(expectedKeys)}, received ${JSON.stringify(actualKeys)}.`,
    );
  }
  for (const [key, expectedValue] of Object.entries(CANONICAL_COMPATIBILITY)) {
    if (compatibility[key] !== expectedValue) {
      throw new Error(
        `Receipt compatibility mismatch for '${key}': expected '${expectedValue}', received '${String(compatibility[key])}'.`,
      );
    }
  }
}

export function validateReleaseRequest(request: ReleaseRequest): ReleaseRequest {
  if (!VERSION_PATTERN.test(request.version)) {
    throw new Error(`Invalid runtime-client version: '${request.version}'.`);
  }
  const expectedTag = `${RELEASE_TAG_PREFIX}${request.version}`;
  if (request.releaseTag !== expectedTag) {
    throw new Error(
      `Release tag mismatch: expected '${expectedTag}', received '${request.releaseTag}'.`,
    );
  }
  if (!COMMIT_PATTERN.test(request.sourceRevision)) {
    throw new Error(
      `Source revision must be a lowercase full commit SHA, received '${request.sourceRevision}'.`,
    );
  }
  if (
    request.reviewedSha256 !== undefined &&
    request.reviewedSha256.length > 0 &&
    !SHA256_PATTERN.test(request.reviewedSha256)
  ) {
    throw new Error("Reviewed SHA256 must be a lowercase 64-character hexadecimal digest.");
  }
  if (request.publish && !SHA256_PATTERN.test(request.reviewedSha256 ?? "")) {
    throw new Error("Publishing requires the reviewed SHA256 from a prior dry run.");
  }
  return request;
}

export function verifyReleaseAssets(
  outputDir: string,
  request: ReleaseRequest,
): VerifiedReleaseAssets {
  validateReleaseRequest(request);
  const resolvedOutputDir = NodePath.resolve(outputDir);
  const artifactName = `${PACKAGE_FILE_PREFIX}${request.version}.tgz`;
  const expectedEntries = [artifactName, `${artifactName}.json`, `${artifactName}.sha256`].sort();
  const actualEntries = NodeFS.readdirSync(resolvedOutputDir).sort();
  if (JSON.stringify(actualEntries) !== JSON.stringify(expectedEntries)) {
    throw new Error(
      `Release output must contain exactly three expected assets. Received: ${JSON.stringify(actualEntries)}.`,
    );
  }

  const artifactPath = NodePath.join(resolvedOutputDir, artifactName);
  const receiptPath = `${artifactPath}.json`;
  const checksumPath = `${artifactPath}.sha256`;
  for (const path of [artifactPath, receiptPath, checksumPath]) {
    assertRegularFile(path);
  }

  const receipt = readReceipt(receiptPath);
  const actualSha256 = sha256File(artifactPath);
  const actualBytes = NodeFS.statSync(artifactPath).size;
  const expectedChecksum = `${actualSha256}  ${artifactName}\n`;
  const adjacentChecksum = NodeFS.readFileSync(checksumPath, "utf8");

  if (receipt.artifact !== artifactName) {
    throw new Error(
      `Receipt artifact mismatch: expected '${artifactName}', received '${receipt.artifact}'.`,
    );
  }
  if (receipt.packageName !== PACKAGE_NAME) {
    throw new Error(
      `Receipt package mismatch: expected '${PACKAGE_NAME}', received '${receipt.packageName}'.`,
    );
  }
  if (receipt.version !== request.version) {
    throw new Error(
      `Receipt version mismatch: expected '${request.version}', received '${receipt.version}'.`,
    );
  }
  if (receipt.sourceRevision !== request.sourceRevision) {
    throw new Error(
      `Receipt source revision mismatch: expected '${request.sourceRevision}', received '${receipt.sourceRevision}'.`,
    );
  }
  assertCanonicalReceiptContract(receipt);
  if (!receipt.publishable) {
    throw new Error("Runtime-client receipt is not publishable.");
  }
  if (receipt.bytes !== actualBytes) {
    throw new Error(
      `Receipt byte count mismatch: expected ${String(actualBytes)}, received ${String(receipt.bytes)}.`,
    );
  }
  if (receipt.sha256 !== actualSha256) {
    throw new Error(
      `Receipt checksum mismatch: expected '${actualSha256}', received '${receipt.sha256}'.`,
    );
  }
  if (adjacentChecksum !== expectedChecksum) {
    throw new Error("Adjacent checksum does not exactly match the runtime-client tarball.");
  }
  if (
    request.reviewedSha256 !== undefined &&
    request.reviewedSha256.length > 0 &&
    request.reviewedSha256 !== actualSha256
  ) {
    throw new Error(
      `Reviewed SHA256 mismatch: expected '${request.reviewedSha256}', received '${actualSha256}'.`,
    );
  }

  return {
    artifactName,
    artifactPath,
    checksumPath,
    receiptPath,
    sha256: actualSha256,
  };
}

function usage(): never {
  throw new Error(
    [
      "Usage: verify-runtime-client-release.ts verify",
      "--output-dir <directory>",
      "--version <version>",
      "--release-tag <tag>",
      "--source-revision <full-sha>",
      "[--reviewed-sha256 <digest>]",
      "[--publish]",
      "[--github-output <path>]",
    ].join(" "),
  );
}

export function parseCliOptions(args: ReadonlyArray<string>): CliOptions {
  if (args[0] !== "verify") {
    usage();
  }
  let githubOutput: string | undefined;
  let outputDir: string | undefined;
  let publish = false;
  let releaseTag: string | undefined;
  let reviewedSha256: string | undefined;
  let sourceRevision: string | undefined;
  let version: string | undefined;

  for (let index = 1; index < args.length; index += 1) {
    const argument = args[index];
    if (argument === "--publish") {
      publish = true;
      continue;
    }
    const value = args[index + 1];
    if (value === undefined) {
      usage();
    }
    switch (argument) {
      case "--github-output":
        githubOutput = value;
        break;
      case "--output-dir":
        outputDir = value;
        break;
      case "--release-tag":
        releaseTag = value;
        break;
      case "--reviewed-sha256":
        reviewedSha256 = value;
        break;
      case "--source-revision":
        sourceRevision = value;
        break;
      case "--version":
        version = value;
        break;
      default:
        usage();
    }
    index += 1;
  }

  if (
    outputDir === undefined ||
    releaseTag === undefined ||
    sourceRevision === undefined ||
    version === undefined
  ) {
    usage();
  }
  return {
    ...(githubOutput === undefined ? {} : { githubOutput: NodePath.resolve(githubOutput) }),
    outputDir: NodePath.resolve(outputDir),
    request: {
      publish,
      releaseTag,
      ...(reviewedSha256 === undefined ? {} : { reviewedSha256 }),
      sourceRevision,
      version,
    },
  };
}

function appendGitHubOutput(path: string, values: Readonly<Record<string, string>>): void {
  for (const [key, value] of Object.entries(values)) {
    if (/[\r\n]/.test(key) || /[\r\n]/.test(value)) {
      throw new Error(`GitHub output '${key}' contains a newline.`);
    }
    NodeFS.appendFileSync(path, `${key}=${value}\n`);
  }
}

if (import.meta.main) {
  const options = parseCliOptions(process.argv.slice(2));
  const verified = verifyReleaseAssets(options.outputDir, options.request);
  if (options.githubOutput !== undefined) {
    appendGitHubOutput(options.githubOutput, {
      artifact_name: verified.artifactName,
      artifact_path: verified.artifactPath,
      checksum_path: verified.checksumPath,
      receipt_path: verified.receiptPath,
      release_tag: options.request.releaseTag,
      sha256: verified.sha256,
      source_revision: options.request.sourceRevision,
      version: options.request.version,
    });
  }
  process.stdout.write(
    `${JSON.stringify({
      artifact: verified.artifactName,
      publish: options.request.publish,
      releaseTag: options.request.releaseTag,
      sha256: verified.sha256,
      sourceRevision: options.request.sourceRevision,
      version: options.request.version,
    })}\n`,
  );
}
