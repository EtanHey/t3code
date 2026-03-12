import { ThreadId, TurnId, type ProviderRuntimeEvent } from "@t3tools/contracts";
import { describe, expect, it } from "vitest";

import { ClaudeEventMapper } from "./ClaudeEventMapper.ts";

const asThreadId = (value: string): ThreadId => ThreadId.makeUnsafe(value);
const asTurnId = (value: string): TurnId => TurnId.makeUnsafe(value);

function createMapper() {
  let nextId = 0;
  return new ClaudeEventMapper({
    provider: "codex",
    threadId: asThreadId("thread-1"),
    makeEventId: () => `evt-${++nextId}` as ProviderRuntimeEvent["eventId"],
    now: () => "2026-03-12T09:40:00.000Z",
  });
}

describe("ClaudeEventMapper", () => {
  it("maps Claude init system events into canonical session lifecycle events", () => {
    const mapper = createMapper();

    const events = mapper.map({
      kind: "stream",
      event: {
        type: "system",
        subtype: "init",
        session_id: "claude-session-1",
        cwd: "/repo",
        model: "claude-sonnet-4-6",
        permission_mode: "default",
        tools: ["Bash", "Edit"],
      },
    });

    expect(events).toMatchObject([
      {
        type: "session.started",
        provider: "codex",
        threadId: "thread-1",
        payload: {
          message: "Claude session initialized",
        },
      },
      {
        type: "session.configured",
        payload: {
          config: {
            cwd: "/repo",
            model: "claude-sonnet-4-6",
            permissionMode: "default",
            sessionId: "claude-session-1",
            tools: ["Bash", "Edit"],
          },
        },
      },
      {
        type: "thread.started",
        payload: {
          providerThreadId: "claude-session-1",
        },
      },
    ]);
  });

  it("diffs assistant text snapshots into content deltas and finalizes the message", () => {
    const mapper = createMapper();
    const turnId = asTurnId("turn-1");

    const partial = mapper.map({
      kind: "stream",
      turnId,
      event: {
        type: "assistant",
        timestamp: "2026-03-12T09:40:01.000Z",
        message: {
          id: "msg-1",
          role: "assistant",
          content: [{ type: "text", text: "Hel" }],
          stop_reason: null,
        },
      },
    });

    expect(partial).toMatchObject([
      {
        type: "item.started",
        turnId: "turn-1",
        itemId: "msg-1",
        payload: {
          itemType: "assistant_message",
          status: "inProgress",
          title: "Assistant message",
          detail: "Hel",
        },
      },
      {
        type: "content.delta",
        turnId: "turn-1",
        itemId: "msg-1",
        payload: {
          streamKind: "assistant_text",
          delta: "Hel",
        },
      },
    ]);

    const next = mapper.map({
      kind: "stream",
      turnId,
      event: {
        type: "assistant",
        timestamp: "2026-03-12T09:40:02.000Z",
        message: {
          id: "msg-1",
          role: "assistant",
          content: [{ type: "text", text: "Hello" }],
          stop_reason: null,
        },
      },
    });

    expect(next).toMatchObject([
      {
        type: "content.delta",
        turnId: "turn-1",
        itemId: "msg-1",
        payload: {
          streamKind: "assistant_text",
          delta: "lo",
        },
      },
    ]);

    const completed = mapper.map({
      kind: "stream",
      turnId,
      event: {
        type: "assistant",
        timestamp: "2026-03-12T09:40:03.000Z",
        message: {
          id: "msg-1",
          role: "assistant",
          content: [{ type: "text", text: "Hello" }],
          stop_reason: "end_turn",
        },
      },
    });

    expect(completed).toMatchObject([
      {
        type: "item.completed",
        turnId: "turn-1",
        itemId: "msg-1",
        payload: {
          itemType: "assistant_message",
          status: "completed",
          detail: "Hello",
        },
      },
    ]);
  });

  it("maps tool_use and tool_result blocks into dynamic tool lifecycle events", () => {
    const mapper = createMapper();
    const turnId = asTurnId("turn-2");

    const started = mapper.map({
      kind: "stream",
      turnId,
      event: {
        type: "assistant",
        timestamp: "2026-03-12T09:40:04.000Z",
        message: {
          id: "msg-2",
          role: "assistant",
          content: [
            {
              type: "tool_use",
              id: "toolu_1",
              name: "Bash",
              input: { command: "ls" },
            },
          ],
          stop_reason: "tool_use",
        },
      },
    });

    expect(started).toMatchObject([
      {
        type: "item.started",
        turnId: "turn-2",
        itemId: "toolu_1",
        payload: {
          itemType: "dynamic_tool_call",
          status: "inProgress",
          title: "Tool call",
          detail: "Bash",
        },
      },
    ]);

    const updated = mapper.map({
      kind: "stream",
      turnId,
      event: {
        type: "assistant",
        timestamp: "2026-03-12T09:40:05.000Z",
        message: {
          id: "msg-2",
          role: "assistant",
          content: [
            {
              type: "tool_use",
              id: "toolu_1",
              name: "Bash",
              input: { command: "ls -la" },
            },
          ],
          stop_reason: null,
        },
      },
    });

    expect(updated).toMatchObject([
      {
        type: "item.updated",
        turnId: "turn-2",
        itemId: "toolu_1",
        payload: {
          itemType: "dynamic_tool_call",
          detail: "Bash",
        },
      },
    ]);

    const completed = mapper.map({
      kind: "stream",
      turnId,
      event: {
        type: "user",
        timestamp: "2026-03-12T09:40:06.000Z",
        message: {
          role: "user",
          content: [
            {
              type: "tool_result",
              tool_use_id: "toolu_1",
              content: "file-a\nfile-b",
              is_error: false,
            },
          ],
        },
      },
    });

    expect(completed).toMatchObject([
      {
        type: "tool.progress",
        turnId: "turn-2",
        payload: {
          toolUseId: "toolu_1",
          toolName: "Bash",
          summary: "file-a\nfile-b",
        },
      },
      {
        type: "item.completed",
        turnId: "turn-2",
        itemId: "toolu_1",
        payload: {
          itemType: "dynamic_tool_call",
          status: "completed",
          detail: "file-a\nfile-b",
        },
      },
    ]);
  });

  it("maps Claude result events into turn completion events", () => {
    const mapper = createMapper();
    const turnId = asTurnId("turn-3");

    const events = mapper.map({
      kind: "stream",
      turnId,
      event: {
        type: "result",
        subtype: "success",
        is_error: false,
        total_cost_usd: 0.42,
        usage: {
          input_tokens: 10,
          output_tokens: 5,
        },
        duration_ms: 1234,
        stop_reason: "end_turn",
      },
    });

    expect(events).toMatchObject([
      {
        type: "turn.completed",
        turnId: "turn-3",
        payload: {
          state: "completed",
          stopReason: "end_turn",
          totalCostUsd: 0.42,
          usage: {
            input_tokens: 10,
            output_tokens: 5,
          },
        },
      },
    ]);
  });

  it("maps approval and user-input flows into request and user-input runtime events", () => {
    const mapper = createMapper();
    const turnId = asTurnId("turn-4");

    const requestOpened = mapper.map({
      kind: "request.opened",
      turnId,
      requestId: "req-1",
      requestType: "command_execution_approval",
      detail: "Run Bash(ls -la)",
      args: { toolName: "Bash" },
    });
    expect(requestOpened).toMatchObject([
      {
        type: "request.opened",
        turnId: "turn-4",
        requestId: "req-1",
        payload: {
          requestType: "command_execution_approval",
          detail: "Run Bash(ls -la)",
          args: { toolName: "Bash" },
        },
      },
    ]);

    const requestResolved = mapper.map({
      kind: "request.resolved",
      turnId,
      requestId: "req-1",
      decision: "accept",
      resolution: { approved: true },
    });
    expect(requestResolved).toMatchObject([
      {
        type: "request.resolved",
        turnId: "turn-4",
        requestId: "req-1",
        payload: {
          requestType: "command_execution_approval",
          decision: "accept",
          resolution: { approved: true },
        },
      },
    ]);

    const userInputRequested = mapper.map({
      kind: "user-input.requested",
      turnId,
      requestId: "input-1",
      questions: [
        {
          id: "color",
          header: "Color",
          question: "Pick one",
          options: [{ label: "Blue", description: "Default theme" }],
        },
      ],
    });
    expect(userInputRequested).toMatchObject([
      {
        type: "user-input.requested",
        turnId: "turn-4",
        requestId: "input-1",
        payload: {
          questions: [
            {
              id: "color",
              header: "Color",
              question: "Pick one",
              options: [{ label: "Blue", description: "Default theme" }],
            },
          ],
        },
      },
    ]);

    const userInputResolved = mapper.map({
      kind: "user-input.resolved",
      turnId,
      requestId: "input-1",
      answers: { color: "Blue" },
    });
    expect(userInputResolved).toMatchObject([
      {
        type: "user-input.resolved",
        turnId: "turn-4",
        requestId: "input-1",
        payload: {
          answers: { color: "Blue" },
        },
      },
    ]);
  });
});
