# Runtime-client artifact

The runtime-client artifact is the distribution boundary for consumers that
need the reviewed T3 WebSocket RPC client without depending on the T3 Code
workspace. Its source package stays private. The build script stages a separate
publishable manifest and emits `@t3tools/runtime-client@0.0.31-rpc.1`.

The public entry re-exports only:

- the canonical `@t3tools/contracts/runtime-client` schemas, RPC group, method
  names, orchestration commands, and compatibility descriptor;
- the reviewed WebSocket protocol-client and strict session-factory exports
  from `@t3tools/client-runtime/runtime-client`.

No schema or Effect RPC framing is copied into this package. The JavaScript
bundle is compiled from those canonical source modules. Effect is deliberately
bundled so the repository's pinned patch is retained, while the packed manifest
declares exact `effect@4.0.0-beta.102` as a peer for consumer imports,
declaration resolution, and cross-instance Context tag identity.

## Build

The default build is publishable only from a clean worktree. From the repository
root, using Node 24.13.1:

```sh
node scripts/build-runtime-client-artifact.ts
```

The builder rejects a dirty worktree before staging. For an explicitly reviewed
local artifact from an uncommitted worktree, opt in:

```sh
node scripts/build-runtime-client-artifact.ts --allow-dirty
```

Review artifacts record `<HEAD>-dirty` and `publishable: false` in both the
manifest and receipt. They are not publication inputs.

The command writes three files under
`packages/runtime-client-artifact/dist/`:

- `t3tools-runtime-client-0.0.31-rpc.1.tgz`;
- a `.tgz.json` receipt containing the source revision, compatibility metadata,
  publishability, inventory, size, SHA-256, and actual Node/npm/tar versions;
- a `.tgz.sha256` checksum file.

Pass `--output-dir <directory>` to put all three outputs elsewhere. Builds use
only builder-created temporary staging directories, validate containment before
cleaning, and copy the final outputs afterward. Strict declarations inherit the
repository's `strict` and `exactOptionalPropertyTypes` settings. Generator path
annotations are removed before packing.

The builder also fails if:

- a dirty worktree was not explicitly accepted with `--allow-dirty`;
- the resolved Effect package is not exactly `4.0.0-beta.102`;
- the repository Effect patch checksum differs from the reviewed checksum;
- the built JavaScript lacks the reviewed
  `effect/rpc/RpcClient/RequestHooks` patch marker;
- the staged inventory changes;
- the bundle, declarations, or manifest contains a workspace/catalog
  reference, unresolved private-package import, source-map marker, or local
  repository/temporary path;
- the declarations introduce `import { Schema } from "effect"`.

## Verification

Run the hermetic unit tests:

```sh
vp test run scripts/build-runtime-client-artifact.test.ts
```

These tests preserve the portable P2 source-package regression and cover
provenance, manifest, compatibility, Effect-patch, staging, and tool-failure
seams. They do not pack or access the network.

The heavyweight distribution test is opt-in and uses the npm registry to
install the exact Effect peer into a clean temporary consumer:

```sh
node scripts/build-runtime-client-artifact.integration.ts
```

It builds twice with `--allow-dirty`, compares tarball bytes, extracts and
inspects every packed member, installs the tarball with its exact peer,
typechecks positive and negative exact-optional fixtures, and executes the
session factory across the consumer's separate Effect instance with a failing
WebSocket stub.

## Publication contract

Do not publish locally and do not publish a dirty review artifact. Publication
must run in maintainer-controlled CI from an immutable, clean revision with npm
trusted publishing configured for OIDC. CI must build without `--allow-dirty`,
repeat the distribution checks against the already-built tarball, verify that
the receipt names the checked-out SHA and says `publishable: true`, and then
publish that verified tarball with provenance and the explicit prerelease tag:

```sh
npm publish ./t3tools-runtime-client-0.0.31-rpc.1.tgz --provenance --tag rpc
```

This repository does not create or run that release workflow as part of the
artifact build.
