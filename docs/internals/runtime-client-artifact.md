# Runtime-client artifact

The runtime-client artifact is the distribution boundary for consumers that
need the reviewed T3 WebSocket RPC client without depending on the T3 Code
workspace. Its source package stays private. The build script stages a separate
publishable manifest and emits `@t3tools/runtime-client@0.0.31-rpc.2`.

The public entry re-exports only:

- the canonical `@t3tools/contracts/runtime-client` schemas, RPC group, method
  names, orchestration commands, and compatibility descriptor;
- the reviewed WebSocket protocol-client and strict session-factory exports
  from `@t3tools/client-runtime/runtime-client`;
- the canonical thread-detail and shell-stream reducers from that same
  runtime-client boundary.

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

- `t3tools-runtime-client-0.0.31-rpc.2.tgz`;
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

## Immutable GitHub Release contract

Do not publish locally, to npm, or to GitHub Packages. Do not release a dirty
review artifact. The maintainer-only `Runtime Client Immutable Release` workflow
is the sole release path for this artifact.

The workflow is version-parameterized and requires the full lowercase source
commit SHA on `main`, the package version expected in the built receipt, the
exact `runtime-client-v<version>` tag, and a publish toggle that defaults to
`false`. Publication also requires the SHA-256 copied from a separately
reviewed dry run. Publication is protected by the fixed
`runtime-client-release` GitHub Environment; dry runs are not blocked by it.

Run the workflow with publishing disabled first. Its read-only job checks out
the exact source revision, installs the pinned Node 24.13.1/Pnpm 11.10.0
toolchain, runs unit and distribution verification, builds without
`--allow-dirty`, and verifies the receipt provenance, canonical RPC/Effect
compatibility, canonical packed inventory, publishability, byte count, and both
checksums. It retains only the `.tgz`, receipt, and `.sha256` as short-lived
workflow artifacts.

After the dry-run code and SHA-256 have been independently reviewed, rerun the
same coordinates with that digest and publishing enabled. The minimal
`contents: write` job runs no checkout or non-official build setup action. It
downloads the reviewed assets, requires repository release immutability already
be enabled without changing settings, and refuses any published release or
non-matching tag collision.

An interrupted matching draft is safe to resume only when its tag and target
SHA equal the reviewed coordinates. The job rejects unexpected draft assets,
removes only the known partial assets, and re-uploads the exact three files.
Before making a draft public, it re-downloads those assets, requires their exact
inventory, byte-compares the tarball, receipt, and checksum with the reviewed
inputs, and checks the tarball SHA-256 again. Post-publication it verifies the
immutable GitHub Release attestation and every local asset.

The workflow does not make an artifact production-ready by itself. The public
runtime-client export surface, including the canonical reducers, and a new
package version must be reviewed before cutting each immutable coordinate.
