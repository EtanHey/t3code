import { expect, it } from "@effect/vitest";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import { Tool } from "effect/ai";

import { advertisedJsonSchema } from "../../advertisedJsonSchema.ts";
import { OrchestratorToolkit } from "./tools.ts";

// Oversized page sizes clamp on decode, so the schema has no `maximum`; Codex also drops
// bounds from its TypeScript view. The description is the only place a model sees a limit.
it("states t3_thread_read page size limits in the parameter descriptions", () => {
  const schema = Tool.getJsonSchema(OrchestratorToolkit.tools.t3_thread_read) as {
    readonly properties: Readonly<Record<string, { readonly description?: string }>>;
  };
  const limits = { limit: "100", runLimit: "50", maxCharsPerItem: "50000" };
  for (const [name, maximum] of Object.entries(limits)) {
    expect(schema.properties[name]?.description, name).toContain(maximum);
  }
});

it("refuses a t3_thread_update whose fields do not fit its action", () => {
  const decode = Schema.decodeUnknownExit(
    OrchestratorToolkit.tools.t3_thread_update.parametersSchema,
  );
  expect(Exit.isFailure(decode({ action: "rename" }))).toBe(true);
  expect(Exit.isFailure(decode({ action: "regenerate_title", title: "New" }))).toBe(true);
  expect(Exit.isSuccess(decode({ action: "rename", title: "New", environmentId: undefined }))).toBe(
    true,
  );
});

// Ids and trimmed strings decode through a transform; JSON Schema shows only the encoded side.
it("advertises descriptions written on ids and trimmed strings", () => {
  type Properties = { readonly [name: string]: { description?: string; properties?: Properties } };
  const properties = (tool: Tool.Any) =>
    (advertisedJsonSchema(tool) as { readonly properties: Properties }).properties;
  const { delegate_task, orchestrator_capabilities, request_secret, t3_thread_list } =
    OrchestratorToolkit.tools;
  const delegate = properties(delegate_task);
  expect(delegate.target?.properties?.providerInstanceId?.description).toContain(
    "orchestrator_capabilities",
  );
  expect(delegate.clientRequestId?.description).toContain("retry the same call");
  expect(properties(t3_thread_list).environmentId?.description).toContain("Linked environment");
  expect(properties(request_secret).label?.description).toContain("Card title");
  expect(properties(orchestrator_capabilities).search?.description).toContain("Match model ids");
});
