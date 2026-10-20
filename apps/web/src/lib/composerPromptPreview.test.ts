import { EnvironmentId, MessageId, ThreadId } from "@t3tools/contracts";
import { serializeAssistantCitation } from "@t3tools/shared/assistantCitations";
import { describe, expect, it } from "vite-plus/test";

import { formatInlineContextReference } from "./composerContextReferences";
import { composerPromptPreviewText } from "./composerPromptPreview";

function quote(text: string, comment?: string): string {
  return serializeAssistantCitation({
    version: 1,
    environmentId: EnvironmentId.make("environment:test"),
    threadId: ThreadId.make("thread:test"),
    messageId: MessageId.make("message:assistant"),
    text,
    start: 0,
    end: text.length,
    prefix: "",
    suffix: "",
    ...(comment === undefined ? {} : { comment }),
  });
}

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

  it("shows file mentions by their file name, as their chips do", () => {
    expect(
      composerPromptPreviewText(
        'Compare [package.json](package.json) with @"apps/web/src/main.tsx" ',
      ),
    ).toBe("Compare package.json with main.tsx");
  });

  it("shows a quote's comment after its quoted text", () => {
    expect(composerPromptPreviewText(quote("Retry on 503.", "Also on 429?"))).toBe(
      "Retry on 503.\nComment: Also on 429?",
    );
  });

  it("shows every quote on a line in order with the typed text", () => {
    const preview = composerPromptPreviewText(
      `${quote("First claim.")} vs ${quote("Second claim.")} which holds?`,
    );

    expect(preview).toBe("First claim. vs Second claim. which holds?");
    expect(preview).not.toContain("t3-citation://");
  });
});
