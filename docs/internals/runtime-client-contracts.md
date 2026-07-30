# Runtime Client Contracts

T3 exposes one supported local package boundary for clients that need the
native WebSocket RPC protocol:

```ts
import {
  T3_RPC_COMPATIBILITY,
  WsRpcGroup,
  makeWsRpcProtocolClient,
  makeRpcSessionFactory,
} from "@t3tools/client-runtime/runtime-client";
```

This boundary is implemented by T3 Code and is shared with the web, desktop,
and mobile clients through `packages/client-runtime`. It is intended for
version-matched integrations such as T3Layer. It is not a second protocol or a
copy of T3 schemas.

## What the boundary owns

`@t3tools/contracts/runtime-client` exports:

- the repository's `WsRpcGroup`;
- the orchestration schemas and method names used by that group;
- the current RPC compatibility descriptor.

`@t3tools/client-runtime/runtime-client` re-exports those contracts and adds:

- the typed `RpcClient.make(WsRpcGroup)` client;
- a scoped WebSocket session factory that requires an exact RPC compatibility
  match;
- the JSON serialization, heartbeat, cancellation, and transport behavior
  provided by the repository-pinned Effect RPC runtime.

Consumers must not parse `Request`, `Exit`, `Chunk`, `Ack`, `Interrupt`,
heartbeat, defect, or client-protocol-error frames themselves. Those semantics
belong to Effect RPC.

## Compatibility handshake

`server.getConfig` includes `environment.rpc`:

```ts
{
  protocol: string;
  transport: string;
  serialization: string;
  contractFingerprint: string;
}
```

`protocol` identifies Effect RPC, `transport` identifies the socket carrier,
`serialization` identifies the wire codec, and `contractFingerprint` identifies
the version-locked T3 RPC/framing contract. The exported
`T3_RPC_COMPATIBILITY` value supplies the current values.

The descriptor schema deliberately accepts arbitrary non-empty values. This
lets a client decode a response from an older or newer server and classify it
as a version mismatch.

`makeRpcSessionFactory` does not return a session until every field equals
`T3_RPC_COMPATIBILITY`. A missing or unequal descriptor fails the connect
operation as a `ConnectionBlockedError` whose reason is `version_mismatch`, so
the returned RPC client has always completed its compatibility check. A socket
that cannot open or disconnects during that handshake fails the same connect
operation with the session lifecycle's structural transport error.

The runtime-client subpath deliberately exports the strict factory without a
Layer under the shared `RpcSessionFactory` service tag. T3's prebuilt connection
layer supplies its permissive first-party factory internally, so an outer layer
cannot override that policy. Version-locked consumers must obtain the strict
factory from `makeRpcSessionFactory` and wire or use it directly instead of
combining it with the prebuilt connection layer.

`makeWsRpcProtocolClient` remains a low-level typed protocol constructor. It
does not perform the config handshake by itself; version-locked session
consumers use `makeRpcSessionFactory`.

T3's first-party web, desktop, and mobile connection runtime does not require
this exact match. It continues connecting to older servers that omit the
descriptor and uses the existing release-version warning and update flow.
Strict matching belongs to version-locked integrations such as T3Layer, not to
the independently updated application surfaces.

`serverVersion` is informational and is not the contract fingerprint: desktop
and Nightly builds stamp it with their release version independently of the RPC
schema.

Any change to the exported RPC group, wire serialization, or framing behavior
that is not backward compatible must update
`T3_RPC_CONTRACT_FINGERPRINT`. Server and client changes ship together.

## Stream flow control

Effect RPC creates a bounded queue for every client stream. The default queue
capacity is owned by Effect, and callers can set `streamBufferSize` when they
need a stricter bound.

For WebSocket streams:

1. the server sends a `Chunk`;
2. the client decodes it with the RPC success schema;
3. Effect waits until the bounded queue accepts the values;
4. only then does Effect send `Ack`.

If the queue is full, the next ACK remains blocked until the consumer creates
capacity. Closing the consumer scope sends `Interrupt` for the request. T3Layer
must keep its own evidence and result queues bounded as well; this transport
guarantee does not make downstream storage unbounded or safe.

## Errors and credentials

The session maps protocol decode failures, Effect RPC defects, and transport
client errors to structural connection errors. It does not copy the original
defect or parser message into the error detail. Version mismatch errors likewise
omit the received descriptor and connection URL.

Authorization headers, bearer tokens, DPoP access tokens, WebSocket tickets,
and secret-bearing URLs must never be written to evidence. Integrations should
log structural error tags/reasons and the public compatibility constants only.

## Surface behavior

The same WebSocket route and contract group serve local web, hosted web,
desktop, mobile, relay, tunnel, and direct remote clients. Compatibility
validation is implemented once by the shared session factory, while the
exported runtime-client boundary opts into fail-closed enforcement. No
connection mode adds origin-specific framing or duplicated schemas.
