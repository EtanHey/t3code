import { EnvironmentId, MessageId, ThreadId } from "@t3tools/contracts";
import { serializeAssistantCitation } from "@t3tools/shared/assistantCitations";
import { describe, expect, it } from "vite-plus/test";

import { formatInlineContextReference } from "./composerContextReferences";
import { composerPromptPreviewText } from "./composerPromptPreview";

describe("composerPromptPreviewText", () => {
  it("shows quoted text and context labels instead of their serialized links", () => {
    const quotedText = "Cache the parsed config per worktree.";
    const quote = serializeAssistantCitation({
      version: 1,
      environmentId: EnvironmentId.make("environment:test"),
      threadId: ThreadId.make("thread:test"),
      messageId: MessageId.make("message:assistant"),
      text: quotedText,
      start: 0,
      end: quotedText.length,
      prefix: "",
      suffix: "",
    });
    const terminal = formatInlineContextReference({
      kind: "terminal",
      contextId: "terminal_1",
      label: "Terminal 1",
    });

    expect(composerPromptPreviewText(` ${quote} Why per worktree? ${terminal} `)).toBe(
      `${quotedText} Why per worktree? Terminal 1`,
    );
  });
});
