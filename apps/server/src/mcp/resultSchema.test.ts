import { expect, it } from "@effect/vitest";
import * as Option from "effect/Option";

import { taskPrompt } from "./delegatedTaskModes.ts";
import { extractJson, validateJson } from "./resultSchema.ts";

it("reads the last fenced JSON block, or bare JSON", () => {
  expect(extractJson('first ```json\n{"a":1}\n``` then ```json\n{"a":2}\n```')).toEqual(
    Option.some({ a: 2 }),
  );
  expect(extractJson(' {"a":3} ')).toEqual(Option.some({ a: 3 }));
  expect(extractJson("no json here")).toEqual(Option.none());
});

it("never falls back to an earlier block when the final one is broken", () => {
  const accepted = '```json\n{"verdict":"accept"}\n```';
  expect(extractJson(`${accepted} then \`\`\`json\n{"verdict":\n\`\`\``)).toEqual(Option.none());
  expect(extractJson(`${accepted} then \`\`\`json\n{"verdict":"revise"}`)).toEqual(Option.none());
});

it("matches enum members that are objects or arrays by value", () => {
  const schema = { enum: [{ verdict: "accept", tags: ["a"] }, [1, 2]] };
  expect(validateJson(schema, { tags: ["a"], verdict: "accept" })).toBeNull();
  expect(validateJson(schema, [1, 2])).toBeNull();
  expect(validateJson(schema, { verdict: "accept", tags: ["b"] })).not.toBeNull();
  expect(validateJson(schema, [1, 2, 3])).not.toBeNull();
});

it("validates nested types, required keys, enums, and integers as numbers", () => {
  const schema = {
    type: "object",
    properties: {
      verdict: { enum: ["accept", "revise"] },
      items: {
        type: "array",
        items: { type: "object", required: ["line"], properties: { line: { type: "number" } } },
      },
    },
    required: ["items"],
  };
  expect(validateJson(schema, { verdict: "accept", items: [{ line: 3 }] })).toBeNull();
  expect(validateJson(schema, { items: [{ line: "3" }] })).toBe(
    "$.items[0].line: expected number, got string",
  );
  expect(validateJson(schema, {})).toBe("$.items: required");
  expect(validateJson({ required: ["constructor"] }, {})).toBe("$.constructor: required");
  expect(validateJson(schema, { verdict: "maybe", items: [] })).toBe(
    '$.verdict: not one of ["accept","revise"]',
  );
});

it("asks for a fenced JSON block only when the task has a resultSchema", () => {
  expect(taskPrompt({ task: "Review it." })).toBe("Review it.");
  const prompt = taskPrompt({
    task: "Review it.",
    resultSchema: { type: "object", required: ["verdict"] },
  });
  expect(prompt.startsWith("Review it.\n\n")).toBe(true);
  expect(prompt).toContain("```json");
  expect(prompt).toContain('{"type":"object","required":["verdict"]}');
});
