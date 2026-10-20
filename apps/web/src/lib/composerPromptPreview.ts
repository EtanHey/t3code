import { assistantCitationsToPlainText } from "@t3tools/shared/assistantCitations";

import { splitPromptIntoComposerSegments } from "~/composer-editor-mentions";
import { basenameOfPath } from "~/pierre-icons";

/**
 * The draft as readable text instead of serialized links, for surfaces that show
 * it as plain text. Built from the editor's own segment parser so a new chip type
 * can't leak raw markup here. Skills stay `$name`: their chip label needs the
 * provider's skill list, and the source already reads as text.
 */
export function composerPromptPreviewText(prompt: string): string {
  return splitPromptIntoComposerSegments(prompt)
    .map((segment) => {
      switch (segment.type) {
        case "text":
          return segment.text;
        case "mention":
          return basenameOfPath(segment.path);
        case "citation":
          return assistantCitationsToPlainText(segment.source);
        case "context-reference":
          return segment.label;
        case "skill":
          return segment.source;
      }
    })
    .join("")
    .trim();
}
