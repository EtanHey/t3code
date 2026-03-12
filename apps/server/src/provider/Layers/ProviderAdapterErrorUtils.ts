import type { ThreadId } from "@t3tools/contracts";

import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  ProviderAdapterSessionNotFoundError,
  type ProviderAdapterError,
} from "../Errors.ts";

export function toProviderAdapterMessage(cause: unknown, fallback: string): string {
  if (cause instanceof Error && cause.message.length > 0) {
    return cause.message;
  }
  return fallback;
}

export function toProviderAdapterSessionError(
  provider: string,
  threadId: ThreadId,
  cause: unknown,
): ProviderAdapterSessionNotFoundError | ProviderAdapterSessionClosedError | undefined {
  const normalized = toProviderAdapterMessage(cause, "").toLowerCase();

  if (
    normalized.includes("unknown session") ||
    normalized.includes("unknown provider session") ||
    normalized.includes("unknown thread") ||
    normalized.includes("missing thread") ||
    normalized.includes("missing session") ||
    normalized.includes("no such session")
  ) {
    return new ProviderAdapterSessionNotFoundError({
      provider,
      threadId,
      cause,
    });
  }

  if (
    normalized.includes("session is closed") ||
    normalized.includes("thread is closed") ||
    normalized.includes("session closed")
  ) {
    return new ProviderAdapterSessionClosedError({
      provider,
      threadId,
      cause,
    });
  }

  return undefined;
}

export function toProviderAdapterRequestError(input: {
  readonly provider: string;
  readonly threadId: ThreadId;
  readonly method: string;
  readonly cause: unknown;
}): ProviderAdapterError {
  const sessionError = toProviderAdapterSessionError(input.provider, input.threadId, input.cause);
  if (sessionError) {
    return sessionError;
  }

  return new ProviderAdapterRequestError({
    provider: input.provider,
    method: input.method,
    detail: toProviderAdapterMessage(input.cause, `${input.method} failed`),
    cause: input.cause,
  });
}

export function toProviderAdapterProcessError(input: {
  readonly provider: string;
  readonly threadId: ThreadId;
  readonly cause: unknown;
  readonly fallback: string;
}): ProviderAdapterProcessError {
  return new ProviderAdapterProcessError({
    provider: input.provider,
    threadId: input.threadId,
    detail: toProviderAdapterMessage(input.cause, input.fallback),
    cause: input.cause,
  });
}
