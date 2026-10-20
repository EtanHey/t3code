import { assistantCitationsToPlainText } from "@t3tools/shared/assistantCitations";
import { replaceComposerContextReferences } from "@t3tools/shared/composerContextReferences";

/** The draft as the editor's chips read, for surfaces that show it as plain text. */
export function composerPromptPreviewText(prompt: string): string {
  return assistantCitationsToPlainText(
    replaceComposerContextReferences(prompt, (reference) => reference.label),
  ).trim();
}
