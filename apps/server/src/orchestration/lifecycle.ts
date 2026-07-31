import type {
  OrchestrationLatestTurnState,
  OrchestrationSessionStatus,
  OrchestrationThreadLifecycle,
} from "@t3tools/contracts";

export interface ThreadLifecycleInput {
  readonly sessionStatus: OrchestrationSessionStatus | null;
  readonly latestTurnState: OrchestrationLatestTurnState | null;
  readonly hasPendingApprovals: boolean;
  readonly hasPendingUserInput: boolean;
}

export function deriveThreadLifecycle(input: ThreadLifecycleInput): OrchestrationThreadLifecycle {
  switch (input.sessionStatus) {
    case "error":
    case "interrupted":
    case "stopped":
      return input.sessionStatus;
  }

  if (input.hasPendingApprovals || input.hasPendingUserInput) {
    return "awaiting-input";
  }

  if (input.sessionStatus === "starting" || input.sessionStatus === "running") {
    return input.latestTurnState === "completed" ||
      input.latestTurnState === "interrupted" ||
      input.latestTurnState === "error"
      ? "unknown"
      : input.sessionStatus;
  }

  switch (input.latestTurnState) {
    case "completed":
    case "interrupted":
    case "error":
      return input.latestTurnState;
  }

  if (input.sessionStatus === "ready" && input.latestTurnState === null) {
    return "ready";
  }

  return "unknown";
}
