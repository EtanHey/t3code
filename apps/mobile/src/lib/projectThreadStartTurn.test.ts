import { ProjectId, ProviderInstanceId } from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";

vi.mock("./composerImages", () => ({
  toUploadChatImageAttachments: (attachments: ReadonlyArray<unknown>) => attachments,
}));

import { buildProjectThreadStartTurnInput } from "./projectThreadStartTurn";

describe("buildProjectThreadStartTurnInput", () => {
  it("marks the shared immediate and outbox bootstrap payload as a root thread", () => {
    const input = buildProjectThreadStartTurnInput({
      projectId: ProjectId.make("project-1"),
      projectCwd: "/workspace",
      threadId: "thread-1",
      commandId: "command-1",
      messageId: "message-1",
      createdAt: "2026-07-31T08:30:00.000Z",
      text: "Start the task",
      attachments: [],
      modelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5.4",
      },
      runtimeMode: "full-access",
      interactionMode: "default",
      workspaceMode: "local",
      branch: null,
      worktreePath: null,
      startFromOrigin: false,
      worktreeBranchName: "t3/thread-1",
    });

    expect(input.bootstrap.createThread.parentThreadId).toBeNull();
  });
});
