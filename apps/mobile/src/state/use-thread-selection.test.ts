import {
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationThread,
} from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";

vi.mock("@react-navigation/native", () => ({
  useRoute: vi.fn(),
}));
vi.mock("../state/entities", () => ({
  useProject: vi.fn(),
  useThreadShell: vi.fn(),
}));
vi.mock("../state/threads", () => ({
  useEnvironmentThread: vi.fn(),
}));
vi.mock("./use-remote-environment-registry", () => ({
  useRemoteEnvironmentRuntime: vi.fn(),
  useSavedRemoteConnection: vi.fn(),
}));

import { threadDetailToShell } from "./use-thread-selection";

describe("threadDetailToShell", () => {
  it("preserves a decoded worker parent identity", () => {
    const parentThreadId = ThreadId.make("thread-parent");
    const thread: OrchestrationThread = {
      id: ThreadId.make("thread-worker"),
      projectId: ProjectId.make("project-1"),
      parentThreadId,
      title: "Worker",
      modelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5.4",
      },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      latestTurn: null,
      createdAt: "2026-07-31T08:30:00.000Z",
      updatedAt: "2026-07-31T08:30:00.000Z",
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      deletedAt: null,
      messages: [],
      proposedPlans: [],
      activities: [],
      checkpoints: [],
      session: null,
      lifecycle: "awaiting-input",
      isLifecycleEvidenceComplete: true,
      hasPendingApprovals: true,
      hasPendingUserInput: false,
    };
    const shell = threadDetailToShell(EnvironmentId.make("environment-1"), thread);
    expect(shell.parentThreadId).toBe(parentThreadId);
    expect(shell.lifecycle).toBe("awaiting-input");
    expect(shell.hasPendingApprovals).toBe(true);
    expect(shell.hasPendingUserInput).toBe(false);
  });
});
