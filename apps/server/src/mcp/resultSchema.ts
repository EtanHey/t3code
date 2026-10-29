import type {
  OrchestrationV2ResultSchema,
  OrchestratorMcpDelegateTaskResult,
} from "@t3tools/contracts";
import * as Option from "effect/Option";

type ResultSchema = typeof OrchestrationV2ResultSchema.Type;

export function resultSchemaInstruction(schema: ResultSchema): string {
  return `End your final message with one \`\`\`json block matching this JSON Schema:\n${JSON.stringify(schema)}`;
}

/** The last fenced JSON block in a reply, or the whole reply when it is bare JSON. */
export function extractJson(text: string): Option.Option<unknown> {
  // An unclosed final fence must not fall back to an earlier, superseded block.
  if ((text.match(/```/g) ?? []).length % 2 === 1) return Option.none();
  const fences = [...text.matchAll(/```(?:json)?\s*\n([\s\S]*?)```/g)];
  const candidate = fences.length === 0 ? text : (fences.at(-1)?.[1] ?? "");
  try {
    return Option.some(JSON.parse(candidate.trim()));
  } catch {
    return Option.none();
  }
}

const typeOf = (value: unknown) =>
  value === null
    ? "null"
    : Array.isArray(value)
      ? "array"
      : Number.isInteger(value)
        ? "integer"
        : typeof value;

const isRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const sameJson = (a: unknown, b: unknown): boolean =>
  Array.isArray(a)
    ? Array.isArray(b) &&
      a.length === b.length &&
      a.every((item, index) => sameJson(item, b[index]))
    : isRecord(a)
      ? isRecord(b) &&
        Object.keys(a).length === Object.keys(b).length &&
        Object.entries(a).every(([key, item]) => Object.hasOwn(b, key) && sameJson(item, b[key]))
      : a === b;

/** The first place `value` breaks `schema`, or null. Other keywords are ignored. */
export function validateJson(schema: unknown, value: unknown, path = "$"): string | null {
  if (!isRecord(schema)) return null;
  const types = schema.type === undefined ? [] : [schema.type].flat();
  const actual = typeOf(value);
  if (
    types.length > 0 &&
    !types.includes(actual) &&
    !(actual === "integer" && types.includes("number"))
  ) {
    return `${path}: expected ${types.join(" or ")}, got ${actual}`;
  }
  if (Array.isArray(schema.enum) && !schema.enum.some((member) => sameJson(member, value))) {
    return `${path}: not one of ${JSON.stringify(schema.enum)}`;
  }
  if (isRecord(value)) {
    for (const key of Array.isArray(schema.required) ? schema.required : []) {
      if (!Object.hasOwn(value, String(key))) return `${path}.${String(key)}: required`;
    }
    for (const [key, property] of Object.entries(
      isRecord(schema.properties) ? schema.properties : {},
    )) {
      if (!Object.hasOwn(value, key)) continue;
      const mismatch = validateJson(property, value[key], `${path}.${key}`);
      if (mismatch !== null) return mismatch;
    }
  }
  if (Array.isArray(value) && schema.items !== undefined) {
    for (const [index, item] of value.entries()) {
      const mismatch = validateJson(schema.items, item, `${path}[${index}]`);
      if (mismatch !== null) return mismatch;
    }
  }
  return null;
}

/** A completed task's reply checked against the schema it was delegated with. */
export function withStructuredResult(
  schema: ResultSchema | undefined,
  result: OrchestratorMcpDelegateTaskResult,
): OrchestratorMcpDelegateTaskResult {
  if (schema === undefined || result.status !== "completed") return result;
  const parsed = extractJson(result.summary ?? "");
  if (Option.isNone(parsed)) return { ...result, invalidOutput: "No JSON in the reply." };
  const mismatch = validateJson(schema, parsed.value);
  return mismatch === null
    ? { ...result, structuredResult: parsed.value }
    : { ...result, invalidOutput: mismatch };
}
