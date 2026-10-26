import type { ModelSelection } from "@t3tools/contracts";

export function normalAgentModelSelection(
  selection: ModelSelection,
  driver: string,
): ModelSelection {
  const options = new Map((selection.options ?? []).map((option) => [option.id, option]));
  // Neither a parent tier nor a provider/CLI default authorizes a new task.
  if (driver === "codex" || options.has("serviceTier")) {
    options.set("serviceTier", { id: "serviceTier", value: "default" });
  }
  if (driver === "claudeAgent" || driver === "cursor" || options.has("fastMode")) {
    options.set("fastMode", { id: "fastMode", value: false });
  }
  return options.size === 0 ? selection : { ...selection, options: [...options.values()] };
}
