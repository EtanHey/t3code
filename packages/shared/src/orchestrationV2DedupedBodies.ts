import {
  OrchestrationV2CheckpointFileSummary,
  OrchestrationV2WorkflowAgent,
  type CheckpointId,
  type MessageId,
  type NodeId,
  type OrchestrationV2Checkpoint,
  type OrchestrationV2Subagent,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

/**
 * For clients that opt in, the server blanks a turn item's body that its record already carries
 * (message text, checkpoint files, a subagent's prompt and result) and marks the field. The client
 * copies the value back from the record. Only a body equal to the record's is blanked, so the
 * restore is exact.
 */
export interface DedupedBodySource {
  readonly messageText: (id: MessageId) => string | undefined;
  readonly checkpointFiles: (id: CheckpointId) => OrchestrationV2Checkpoint["files"] | undefined;
  readonly subagent: (id: NodeId) => WithSubagentBodies | undefined;
}

type SubagentBodies = Pick<OrchestrationV2Subagent, "prompt" | "result"> &
  Partial<Pick<OrchestrationV2Subagent, "workflow">>;
type WithSubagentBodies = SubagentBodies & {
  readonly promptOmitted?: true;
  readonly resultOmitted?: true;
  readonly unchangedAgentsOmitted?: true;
};

const sameCheckpointFiles = Schema.toEquivalence(
  Schema.Array(OrchestrationV2CheckpointFileSummary),
);
const sameWorkflowAgent = Schema.toEquivalence(OrchestrationV2WorkflowAgent);

/**
 * For clients that also accept workflow agent deltas: a repeated `subagent.updated` keeps only
 * the agents that differ from `previous`. Only agents stored at their own `index`, never fewer
 * than before, are thinned, so the restore is exact.
 */
export function dedupeWorkflowAgents<T extends WithSubagentBodies>(
  value: T,
  previous: SubagentBodies | undefined,
): T {
  const agents = value.workflow?.agents;
  const before = previous?.workflow?.agents;
  if (agents === undefined || before === undefined || agents.length < before.length) return value;
  if (!agents.every((agent, index) => agent.index === index)) return value;
  const changed = agents.filter(
    (agent, index) => before[index] === undefined || !sameWorkflowAgent(agent, before[index]),
  );
  if (changed.length === agents.length) return value;
  return {
    ...value,
    workflow: { ...value.workflow!, agents: changed },
    unchangedAgentsOmitted: true as const,
  };
}

/** For subagent items and for a repeated `subagent.updated`, whose `previous` was sent earlier. */
export function dedupeSubagentBodies<T extends WithSubagentBodies>(
  value: T,
  previous: SubagentBodies | undefined,
): T {
  const prompt = value.prompt !== "" && previous?.prompt === value.prompt;
  const result = value.result !== null && previous?.result === value.result;
  if (!prompt && !result) return value;
  return {
    ...value,
    ...(prompt ? { prompt: "", promptOmitted: true as const } : {}),
    ...(result ? { result: null, resultOmitted: true as const } : {}),
  };
}

/**
 * A marked body whose record is missing, or whose record field is itself still marked, stays
 * blank and marked; the server only blanks bodies it sent, so either means a server bug.
 */
export function restoreSubagentBodies<T extends WithSubagentBodies>(
  value: T,
  previous: WithSubagentBodies | undefined,
): T {
  if (previous === undefined) return value;
  const prompt = value.promptOmitted === true && previous.promptOmitted !== true;
  const result = value.resultOmitted === true && previous.resultOmitted !== true;
  const before = previous.unchangedAgentsOmitted === true ? undefined : previous.workflow?.agents;
  const workflow = value.unchangedAgentsOmitted === true ? value.workflow : undefined;
  const agents = workflow !== undefined && before !== undefined;
  if (!prompt && !result && !agents) return value;
  const { promptOmitted, resultOmitted, unchangedAgentsOmitted, ...rest } = value;
  const merged = [...(before ?? [])];
  for (const agent of workflow?.agents ?? []) merged[agent.index] = agent;
  // Dropping the optional markers from `T` and keeping the unrestored ones leaves a `T`.
  return {
    ...rest,
    ...(prompt ? { prompt: previous.prompt } : promptOmitted === true ? { promptOmitted } : {}),
    ...(result ? { result: previous.result } : resultOmitted === true ? { resultOmitted } : {}),
    ...(agents
      ? { workflow: { ...workflow, agents: merged } }
      : unchangedAgentsOmitted === true
        ? { unchangedAgentsOmitted }
        : {}),
  } as T;
}

export function dedupeTurnItemBodies(
  item: OrchestrationV2TurnItem,
  source: DedupedBodySource,
): OrchestrationV2TurnItem {
  switch (item.type) {
    case "user_message":
    case "assistant_message":
      return item.text !== "" && source.messageText(item.messageId) === item.text
        ? { ...item, text: "", textOmitted: true }
        : item;
    case "checkpoint": {
      const files = source.checkpointFiles(item.checkpointId);
      return item.files.length > 0 && files !== undefined && sameCheckpointFiles(files, item.files)
        ? { ...item, files: [], filesOmitted: true }
        : item;
    }
    case "subagent":
      return dedupeSubagentBodies(item, source.subagent(item.subagentId));
    default:
      return item;
  }
}

export function restoreTurnItemBodies(
  item: OrchestrationV2TurnItem,
  source: DedupedBodySource,
): OrchestrationV2TurnItem {
  switch (item.type) {
    case "user_message":
    case "assistant_message": {
      const text = item.textOmitted === true ? source.messageText(item.messageId) : undefined;
      if (text === undefined) return item;
      const { textOmitted: _textOmitted, ...rest } = item;
      return { ...rest, text };
    }
    case "checkpoint": {
      const files =
        item.filesOmitted === true ? source.checkpointFiles(item.checkpointId) : undefined;
      if (files === undefined) return item;
      const { filesOmitted: _filesOmitted, ...rest } = item;
      return { ...rest, files };
    }
    case "subagent":
      return item.promptOmitted === true || item.resultOmitted === true
        ? restoreSubagentBodies(item, source.subagent(item.subagentId))
        : item;
    default:
      return item;
  }
}

/** Builds each index on first lookup, so a snapshot without marked items builds none. */
function snapshotBodySource(projection: OrchestrationV2ThreadProjection): DedupedBodySource {
  let messages: Map<MessageId, string> | undefined;
  let checkpoints: Map<CheckpointId, OrchestrationV2Checkpoint["files"]> | undefined;
  let subagents: Map<NodeId, OrchestrationV2Subagent> | undefined;
  return {
    messageText: (id) =>
      (messages ??= new Map(projection.messages.map((message) => [message.id, message.text]))).get(
        id,
      ),
    checkpointFiles: (id) =>
      (checkpoints ??= new Map(
        projection.checkpoints.map((checkpoint) => [checkpoint.id, checkpoint.files]),
      )).get(id),
    subagent: (id) =>
      (subagents ??= new Map(projection.subagents.map((subagent) => [subagent.id, subagent]))).get(
        id,
      ),
  };
}

/**
 * Inherited rows belong to another thread, whose records this projection lacks. Maps each item
 * once, so an item shared by `visibleTurnItems` and `turnItems` stays one object.
 */
function mapLocalTurnItems(
  projection: OrchestrationV2ThreadProjection,
  map: (item: OrchestrationV2TurnItem) => OrchestrationV2TurnItem,
): OrchestrationV2ThreadProjection {
  const mapped = new Map<OrchestrationV2TurnItem, OrchestrationV2TurnItem>();
  let changed = false;
  const once = (item: OrchestrationV2TurnItem) => {
    let next = mapped.get(item);
    if (next === undefined) {
      next = map(item);
      mapped.set(item, next);
      changed ||= next !== item;
    }
    return next;
  };
  const visibleTurnItems = projection.visibleTurnItems.map((row) => {
    if (row.visibility !== "local" && row.sourceThreadId !== projection.thread.id) return row;
    const item = once(row.item);
    return item === row.item ? row : { ...row, item };
  });
  const turnItems = projection.turnItems.map(once);
  return changed ? { ...projection, visibleTurnItems, turnItems } : projection;
}

export function dedupeSnapshotBodies(
  projection: OrchestrationV2ThreadProjection,
): OrchestrationV2ThreadProjection {
  const source = snapshotBodySource(projection);
  return mapLocalTurnItems(projection, (item) => dedupeTurnItemBodies(item, source));
}

/** Call on every decoded snapshot before reducers or caches see it. */
export function restoreSnapshotBodies(
  projection: OrchestrationV2ThreadProjection,
): OrchestrationV2ThreadProjection {
  const source = snapshotBodySource(projection);
  return mapLocalTurnItems(projection, (item) => restoreTurnItemBodies(item, source));
}
