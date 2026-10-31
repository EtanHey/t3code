import { expect, it } from "@effect/vitest";

import { taskPrompt } from "./delegatedTaskModes.ts";

it("asks the child to end with JSON matching the resultSchema, after its role", () => {
  const resultSchema = { type: "object", required: ["verdict"] };
  expect(taskPrompt({ task: "Review the diff.", role: "review", resultSchema })).toBe(
    "Act as the review sub-agent for this task.\n\nReview the diff.\n\n" +
      'End your final message with one ```json block matching this JSON Schema:\n{"type":"object","required":["verdict"]}',
  );
  expect(taskPrompt({ task: "Review the diff." })).toBe("Review the diff.");
});
