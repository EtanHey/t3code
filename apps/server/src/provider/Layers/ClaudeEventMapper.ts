import {
  ProviderItemId,
  RuntimeItemId,
  RuntimeRequestId,
  type CanonicalRequestType,
  type EventId,
  type ProviderKind,
  type ProviderRuntimeEvent,
  type ProviderUserInputAnswers,
  type ThreadId,
  type TurnId,
  type UserInputQuestion,
} from "@t3tools/contracts";

type ClaudeTextBlock = {
  readonly type: "text";
  readonly text: string;
};

type ClaudeToolUseBlock = {
  readonly type: "tool_use";
  readonly id: string;
  readonly name: string;
  readonly input?: unknown;
};

type ClaudeToolReferenceBlock = {
  readonly type: "tool_reference";
  readonly tool_name: string;
};

type ClaudeToolResultBlock = {
  readonly type: "tool_result";
  readonly tool_use_id: string;
  readonly content?: unknown;
  readonly is_error?: boolean;
};

type ClaudeContentBlock =
  | ClaudeTextBlock
  | ClaudeToolUseBlock
  | ClaudeToolReferenceBlock
  | ClaudeToolResultBlock
  | { readonly type: string; readonly [key: string]: unknown };

type ClaudeSystemEvent = {
  readonly type: "system";
  readonly subtype?: string;
  readonly session_id?: string;
  readonly cwd?: string;
  readonly model?: string;
  readonly permission_mode?: string;
  readonly tools?: unknown;
  readonly mcp_servers?: unknown;
  readonly [key: string]: unknown;
};

type ClaudeAssistantEvent = {
  readonly type: "assistant";
  readonly timestamp?: string;
  readonly message: {
    readonly id?: string;
    readonly role?: string;
    readonly content?: ReadonlyArray<ClaudeContentBlock>;
    readonly stop_reason?: string | null;
    readonly usage?: unknown;
    readonly model?: string;
    readonly [key: string]: unknown;
  };
  readonly [key: string]: unknown;
};

type ClaudeUserEvent = {
  readonly type: "user";
  readonly timestamp?: string;
  readonly message: {
    readonly role?: string;
    readonly content?: ReadonlyArray<ClaudeContentBlock> | string;
    readonly [key: string]: unknown;
  };
  readonly [key: string]: unknown;
};

type ClaudeResultEvent = {
  readonly type: "result";
  readonly subtype?: string;
  readonly duration_ms?: number;
  readonly duration_api_ms?: number;
  readonly is_error?: boolean;
  readonly num_turns?: number;
  readonly session_id?: string;
  readonly total_cost_usd?: number;
  readonly usage?: unknown;
  readonly result?: string;
  readonly error?: string;
  readonly stop_reason?: string | null;
  readonly [key: string]: unknown;
};

export type ClaudeStreamJsonEvent =
  | ClaudeSystemEvent
  | ClaudeAssistantEvent
  | ClaudeUserEvent
  | ClaudeResultEvent;

type ClaudeMapperStreamInput = {
  readonly kind: "stream";
  readonly turnId?: TurnId;
  readonly event: ClaudeStreamJsonEvent;
};

type ClaudeMapperRequestOpenedInput = {
  readonly kind: "request.opened";
  readonly turnId?: TurnId;
  readonly requestId: string;
  readonly requestType: CanonicalRequestType;
  readonly detail?: string;
  readonly args?: unknown;
};

type ClaudeMapperRequestResolvedInput = {
  readonly kind: "request.resolved";
  readonly turnId?: TurnId;
  readonly requestId: string;
  readonly requestType?: CanonicalRequestType;
  readonly decision?: string;
  readonly resolution?: unknown;
};

type ClaudeMapperUserInputRequestedInput = {
  readonly kind: "user-input.requested";
  readonly turnId?: TurnId;
  readonly requestId: string;
  readonly questions: ReadonlyArray<UserInputQuestion>;
};

type ClaudeMapperUserInputResolvedInput = {
  readonly kind: "user-input.resolved";
  readonly turnId?: TurnId;
  readonly requestId: string;
  readonly answers: ProviderUserInputAnswers;
};

type ClaudeMapperRuntimeWarningInput = {
  readonly kind: "runtime.warning";
  readonly turnId?: TurnId;
  readonly message: string;
  readonly detail?: unknown;
};

type ClaudeMapperRuntimeErrorInput = {
  readonly kind: "runtime.error";
  readonly turnId?: TurnId;
  readonly message: string;
  readonly detail?: unknown;
};

export type ClaudeMapperInput =
  | ClaudeMapperStreamInput
  | ClaudeMapperRequestOpenedInput
  | ClaudeMapperRequestResolvedInput
  | ClaudeMapperUserInputRequestedInput
  | ClaudeMapperUserInputResolvedInput
  | ClaudeMapperRuntimeWarningInput
  | ClaudeMapperRuntimeErrorInput;

type AssistantSnapshotState = {
  text: string;
  started: boolean;
  completed: boolean;
};

type ToolCallState = {
  inputHash: string | undefined;
  toolName: string | undefined;
  completed: boolean;
};

export interface ClaudeEventMapperOptions {
  readonly provider: ProviderKind;
  readonly threadId: ThreadId;
  readonly now?: () => string;
  readonly makeEventId?: () => EventId;
}

function trimToUndefined(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : undefined;
}

function asProviderItemId(itemId: string): ProviderItemId {
  return ProviderItemId.makeUnsafe(itemId);
}

function asRuntimeItemId(itemId: string): RuntimeItemId {
  return RuntimeItemId.makeUnsafe(itemId);
}

function asRuntimeRequestId(requestId: string): RuntimeRequestId {
  return RuntimeRequestId.makeUnsafe(requestId);
}

function stableStringify(value: unknown): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value === "string") return value;
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) {
    return `[${value.map((entry) => stableStringify(entry) ?? "null").join(",")}]`;
  }

  const entries = Object.entries(value).toSorted(([left], [right]) => left.localeCompare(right));
  return `{${entries
    .map(([key, entry]) => `${JSON.stringify(key)}:${stableStringify(entry) ?? "null"}`)
    .join(",")}}`;
}

function diffSuffix(previous: string, next: string): string {
  const maxPrefixLength = Math.min(previous.length, next.length);
  let prefixLength = 0;
  while (prefixLength < maxPrefixLength && previous[prefixLength] === next[prefixLength]) {
    prefixLength += 1;
  }
  return next.slice(prefixLength);
}

function isTextBlock(block: ClaudeContentBlock): block is ClaudeTextBlock {
  return block.type === "text" && typeof (block as { text?: unknown }).text === "string";
}

function isToolUseBlock(block: ClaudeContentBlock): block is ClaudeToolUseBlock {
  return (
    block.type === "tool_use" &&
    typeof (block as { id?: unknown }).id === "string" &&
    typeof (block as { name?: unknown }).name === "string"
  );
}

function isToolResultBlock(block: ClaudeContentBlock): block is ClaudeToolResultBlock {
  return (
    block.type === "tool_result" &&
    typeof (block as { tool_use_id?: unknown }).tool_use_id === "string"
  );
}

function extractAssistantText(content: ReadonlyArray<ClaudeContentBlock> | undefined): string {
  if (!content) return "";
  return content
    .filter(isTextBlock)
    .map((block) => block.text)
    .join("");
}

function toolResultSummary(content: unknown): string | undefined {
  if (typeof content === "string") {
    return trimToUndefined(content);
  }

  if (!Array.isArray(content)) {
    return trimToUndefined(stableStringify(content));
  }

  const parts = content
    .flatMap((entry) => {
      if (typeof entry === "string") return [entry];
      if (!entry || typeof entry !== "object") return [];
      const typed = entry as Record<string, unknown>;
      if (typed.type === "text") {
        return typeof typed.text === "string" ? [typed.text] : [];
      }
      if (typed.type === "tool_reference") {
        return typeof typed.tool_name === "string" ? [typed.tool_name] : [];
      }
      return stableStringify(entry) ? [stableStringify(entry)!] : [];
    })
    .map((part) => part.trim())
    .filter((part) => part.length > 0);

  return parts.length > 0 ? parts.join("\n") : undefined;
}

function itemTitle(itemType: "assistant_message" | "dynamic_tool_call"): string {
  return itemType === "assistant_message" ? "Assistant message" : "Tool call";
}

function resultStopReason(event: ClaudeResultEvent): string | undefined {
  return trimToUndefined(event.stop_reason ?? event.subtype);
}

function resultErrorMessage(event: ClaudeResultEvent): string | undefined {
  return trimToUndefined(event.error ?? event.result);
}

function isAbortedResult(event: ClaudeResultEvent): boolean {
  const subtype = event.subtype?.toLowerCase();
  const stopReason = event.stop_reason?.toLowerCase();
  return (
    subtype === "interrupted" ||
    subtype === "cancelled" ||
    stopReason === "interrupted" ||
    stopReason === "cancelled"
  );
}

export class ClaudeEventMapper {
  private readonly provider: ProviderKind;
  private readonly threadId: ThreadId;
  private readonly now: () => string;
  private readonly makeEventId: () => EventId;
  private readonly assistantStateByMessageId = new Map<string, AssistantSnapshotState>();
  private readonly toolStateById = new Map<string, ToolCallState>();
  private readonly requestTypeById = new Map<string, CanonicalRequestType>();

  constructor(options: ClaudeEventMapperOptions) {
    this.provider = options.provider;
    this.threadId = options.threadId;
    this.now = options.now ?? (() => new Date().toISOString());
    this.makeEventId = options.makeEventId ?? (() => crypto.randomUUID() as EventId);
  }

  map(input: ClaudeMapperInput): ReadonlyArray<ProviderRuntimeEvent> {
    switch (input.kind) {
      case "stream":
        return this.mapStreamEvent(input);
      case "request.opened":
        this.requestTypeById.set(input.requestId, input.requestType);
        return [
          this.runtimeEvent({
            createdAt: this.now(),
            turnId: input.turnId,
            requestId: input.requestId,
            providerRequestId: input.requestId,
            type: "request.opened",
            payload: {
              requestType: input.requestType,
              ...(trimToUndefined(input.detail) ? { detail: trimToUndefined(input.detail) } : {}),
              ...(input.args !== undefined ? { args: input.args } : {}),
            },
          }),
        ];
      case "request.resolved": {
        const requestType =
          input.requestType ?? this.requestTypeById.get(input.requestId) ?? "unknown";
        this.requestTypeById.delete(input.requestId);
        return [
          this.runtimeEvent({
            createdAt: this.now(),
            turnId: input.turnId,
            requestId: input.requestId,
            providerRequestId: input.requestId,
            type: "request.resolved",
            payload: {
              requestType,
              ...(trimToUndefined(input.decision)
                ? { decision: trimToUndefined(input.decision) }
                : {}),
              ...(input.resolution !== undefined ? { resolution: input.resolution } : {}),
            },
          }),
        ];
      }
      case "user-input.requested":
        return [
          this.runtimeEvent({
            createdAt: this.now(),
            turnId: input.turnId,
            requestId: input.requestId,
            providerRequestId: input.requestId,
            type: "user-input.requested",
            payload: {
              questions: [...input.questions],
            },
          }),
        ];
      case "user-input.resolved":
        return [
          this.runtimeEvent({
            createdAt: this.now(),
            turnId: input.turnId,
            requestId: input.requestId,
            providerRequestId: input.requestId,
            type: "user-input.resolved",
            payload: {
              answers: input.answers,
            },
          }),
        ];
      case "runtime.warning":
        return [
          this.runtimeEvent({
            createdAt: this.now(),
            turnId: input.turnId,
            type: "runtime.warning",
            payload: {
              message: input.message,
              ...(input.detail !== undefined ? { detail: input.detail } : {}),
            },
          }),
        ];
      case "runtime.error":
        return [
          this.runtimeEvent({
            createdAt: this.now(),
            turnId: input.turnId,
            type: "runtime.error",
            payload: {
              message: input.message,
              class: "provider_error",
              ...(input.detail !== undefined ? { detail: input.detail } : {}),
            },
          }),
        ];
    }
  }

  private mapStreamEvent(input: ClaudeMapperStreamInput): ReadonlyArray<ProviderRuntimeEvent> {
    switch (input.event.type) {
      case "system":
        return this.mapSystemEvent(input.event, input.turnId);
      case "assistant":
        return this.mapAssistantEvent(input.event, input.turnId);
      case "user":
        return this.mapUserEvent(input.event, input.turnId);
      case "result":
        return this.mapResultEvent(input.event, input.turnId);
    }
  }

  private mapSystemEvent(
    event: ClaudeSystemEvent,
    turnId: TurnId | undefined,
  ): ReadonlyArray<ProviderRuntimeEvent> {
    if (event.subtype !== "init") {
      return [];
    }

    const createdAt = this.now();
    const sessionId = trimToUndefined(event.session_id);
    const configured = {
      ...(trimToUndefined(event.cwd) ? { cwd: trimToUndefined(event.cwd) } : {}),
      ...(trimToUndefined(event.model) ? { model: trimToUndefined(event.model) } : {}),
      ...(trimToUndefined(event.permission_mode)
        ? { permissionMode: trimToUndefined(event.permission_mode) }
        : {}),
      ...(sessionId ? { sessionId } : {}),
      ...(event.tools !== undefined ? { tools: event.tools } : {}),
      ...(event.mcp_servers !== undefined ? { mcpServers: event.mcp_servers } : {}),
    };

    const events: ProviderRuntimeEvent[] = [
      this.runtimeEvent({
        createdAt,
        turnId,
        type: "session.started",
        payload: {
          message: "Claude session initialized",
          resume: event,
        },
      }),
    ];

    if (Object.keys(configured).length > 0) {
      events.push(
        this.runtimeEvent({
          createdAt,
          turnId,
          type: "session.configured",
          payload: {
            config: configured,
          },
        }),
      );
    }

    if (sessionId) {
      events.push(
        this.runtimeEvent({
          createdAt,
          turnId,
          type: "thread.started",
          payload: {
            providerThreadId: sessionId,
          },
        }),
      );
    }

    return events;
  }

  private mapAssistantEvent(
    event: ClaudeAssistantEvent,
    turnId: TurnId | undefined,
  ): ReadonlyArray<ProviderRuntimeEvent> {
    const createdAt = event.timestamp ?? this.now();
    const messageId = trimToUndefined(event.message.id);
    const content = event.message.content ?? [];
    const text = extractAssistantText(content);
    const events: ProviderRuntimeEvent[] = [];

    if (messageId && text.length > 0) {
      const currentState = this.assistantStateByMessageId.get(messageId) ?? {
        text: "",
        started: false,
        completed: false,
      };

      if (!currentState.started) {
        events.push(
          this.runtimeEvent({
            createdAt,
            turnId,
            itemId: messageId,
            providerItemId: messageId,
            type: "item.started",
            payload: {
              itemType: "assistant_message",
              status: "inProgress",
              title: itemTitle("assistant_message"),
              detail: text,
              data: {
                messageId,
                content,
              },
            },
          }),
        );
        currentState.started = true;
      }

      const delta = diffSuffix(currentState.text, text);
      if (delta.length > 0) {
        events.push(
          this.runtimeEvent({
            createdAt,
            turnId,
            itemId: messageId,
            providerItemId: messageId,
            type: "content.delta",
            payload: {
              streamKind: "assistant_text",
              delta,
            },
          }),
        );
      }

      currentState.text = text;

      if (event.message.stop_reason && !currentState.completed) {
        events.push(
          this.runtimeEvent({
            createdAt,
            turnId,
            itemId: messageId,
            providerItemId: messageId,
            type: "item.completed",
            payload: {
              itemType: "assistant_message",
              status: "completed",
              title: itemTitle("assistant_message"),
              detail: text,
              data: {
                messageId,
                content,
                stopReason: event.message.stop_reason,
              },
            },
          }),
        );
        currentState.completed = true;
      }

      this.assistantStateByMessageId.set(messageId, currentState);
    }

    for (const block of content) {
      if (!isToolUseBlock(block)) continue;
      const toolUseId = trimToUndefined(block.id);
      if (!toolUseId) continue;

      const inputHash = stableStringify(block.input);
      const previousState = this.toolStateById.get(toolUseId);
      const lifecycleType =
        previousState === undefined
          ? "item.started"
          : previousState.inputHash !== inputHash
            ? "item.updated"
            : undefined;

      if (!lifecycleType) continue;

      events.push(
        this.runtimeEvent({
          createdAt,
          turnId,
          itemId: toolUseId,
          providerItemId: toolUseId,
          type: lifecycleType,
          payload: {
            itemType: "dynamic_tool_call",
            ...(lifecycleType === "item.started" ? { status: "inProgress" } : {}),
            title: itemTitle("dynamic_tool_call"),
            ...(trimToUndefined(block.name) ? { detail: trimToUndefined(block.name) } : {}),
            data: {
              messageId,
              toolUseId,
              toolName: block.name,
              input: block.input,
            },
          },
        }),
      );

      this.toolStateById.set(toolUseId, {
        inputHash,
        toolName: trimToUndefined(block.name),
        completed: previousState?.completed ?? false,
      });
    }

    return events;
  }

  private mapUserEvent(
    event: ClaudeUserEvent,
    turnId: TurnId | undefined,
  ): ReadonlyArray<ProviderRuntimeEvent> {
    const content = Array.isArray(event.message.content) ? event.message.content : [];
    const createdAt = event.timestamp ?? this.now();
    const events: ProviderRuntimeEvent[] = [];

    for (const block of content) {
      if (!isToolResultBlock(block)) continue;
      const toolUseId = trimToUndefined(block.tool_use_id);
      if (!toolUseId) continue;
      const summary = toolResultSummary(block.content);
      const toolState = this.toolStateById.get(toolUseId);

      events.push(
        this.runtimeEvent({
          createdAt,
          turnId,
          type: "tool.progress",
          payload: {
            toolUseId,
            ...(toolState?.toolName ? { toolName: toolState.toolName } : {}),
            ...(summary ? { summary } : {}),
          },
        }),
      );

      events.push(
        this.runtimeEvent({
          createdAt,
          turnId,
          itemId: toolUseId,
          providerItemId: toolUseId,
          type: "item.completed",
          payload: {
            itemType: "dynamic_tool_call",
            status: "completed",
            title: itemTitle("dynamic_tool_call"),
            ...(summary ? { detail: summary } : {}),
            data: block,
          },
        }),
      );

      this.toolStateById.set(toolUseId, {
        inputHash: toolState?.inputHash,
        toolName: toolState?.toolName,
        completed: true,
      });
    }

    return events;
  }

  private mapResultEvent(
    event: ClaudeResultEvent,
    turnId: TurnId | undefined,
  ): ReadonlyArray<ProviderRuntimeEvent> {
    const createdAt = this.now();

    if (isAbortedResult(event)) {
      return [
        this.runtimeEvent({
          createdAt,
          turnId,
          type: "turn.aborted",
          payload: {
            reason: resultErrorMessage(event) ?? "Claude turn interrupted",
          },
        }),
      ];
    }

    return [
      this.runtimeEvent({
        createdAt,
        turnId,
        type: "turn.completed",
        payload: {
          state: event.is_error ? "failed" : "completed",
          ...(resultStopReason(event) ? { stopReason: resultStopReason(event) } : {}),
          ...(event.usage !== undefined ? { usage: event.usage } : {}),
          ...(typeof event.total_cost_usd === "number"
            ? { totalCostUsd: event.total_cost_usd }
            : {}),
          ...(event.is_error && resultErrorMessage(event)
            ? { errorMessage: resultErrorMessage(event) }
            : {}),
        },
      }),
    ];
  }

  private runtimeEvent<TType extends ProviderRuntimeEvent["type"]>(input: {
    readonly createdAt: string;
    readonly turnId?: TurnId | undefined;
    readonly itemId?: string | undefined;
    readonly requestId?: string | undefined;
    readonly providerItemId?: string | undefined;
    readonly providerRequestId?: string | undefined;
    readonly type: TType;
    readonly payload: Extract<ProviderRuntimeEvent, { type: TType }>["payload"];
  }): Extract<ProviderRuntimeEvent, { type: TType }> {
    const providerRefs = {
      ...(input.providerItemId ? { providerItemId: asProviderItemId(input.providerItemId) } : {}),
      ...(input.providerRequestId ? { providerRequestId: input.providerRequestId } : {}),
    };

    return {
      eventId: this.makeEventId(),
      provider: this.provider,
      threadId: this.threadId,
      createdAt: input.createdAt,
      ...(input.turnId ? { turnId: input.turnId } : {}),
      ...(input.itemId ? { itemId: asRuntimeItemId(input.itemId) } : {}),
      ...(input.requestId ? { requestId: asRuntimeRequestId(input.requestId) } : {}),
      ...(Object.keys(providerRefs).length > 0 ? { providerRefs } : {}),
      type: input.type,
      payload: input.payload,
    } as Extract<ProviderRuntimeEvent, { type: TType }>;
  }
}
