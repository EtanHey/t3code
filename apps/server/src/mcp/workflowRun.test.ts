import { expect, it } from "@effect/vitest";
import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";

import {
  decodeWorkflowSpec,
  initialSnapshot,
  launchKey,
  resumeWorkflow,
  WorkflowSnapshot,
  type WorkflowAgentOutcome,
  type WorkflowAgentStart,
  type WorkflowPort,
  type WorkflowResult,
} from "./workflowRun.ts";

type Reply =
  | { readonly afterMs?: number; readonly outcome: WorkflowAgentOutcome }
  | { readonly hang: true; readonly ignoreCancel?: boolean };
type CrashMode = "after" | "instead";

interface FakeOptions {
  readonly reply: (start: WorkflowAgentStart) => Reply;
  readonly crash?: (snapshot: WorkflowSnapshot, call: number) => CrashMode | undefined;
  readonly onStart?: (start: WorkflowAgentStart) => Effect.Effect<void>;
}

interface FakeTask {
  readonly key: string;
  readonly stage: string;
  readonly round: number;
  readonly reply: Reply;
  readonly finishAt: number;
  readonly cancelled: Deferred.Deferred<void>;
  settled: WorkflowAgentOutcome | null;
}

const ok = (result: string, structuredResult?: unknown): WorkflowAgentOutcome => ({
  status: "completed",
  result,
  ...(structuredResult === undefined ? {} : { structuredResult }),
});
const failed: WorkflowAgentOutcome = { status: "failed", result: "boom" };
const CANCELLED: WorkflowAgentOutcome = { status: "cancelled", result: null };
const decodeSnapshot = Schema.decodeUnknownSync(WorkflowSnapshot);
const recordKey = (launch: WorkflowSnapshot["launches"][number]) =>
  launchKey(launch.stage, launch.round, launch.agent);

/**
 * A task port whose children live on, and keep finishing, across runner crashes. It records
 * TLA-style monitors: launch intent and fences at each start, stage barrier, monotone snapshots.
 */
const makeFake = (options: FakeOptions) => {
  const persisted: Array<{ readonly snapshot: WorkflowSnapshot; readonly now: number }> = [];
  const tasks = new Map<string, FakeTask>();
  const rendered = new Map<string, string>();
  const startCalls: Array<string> = [];
  const created: Array<string> = [];
  const cancelCalls: Array<string> = [];
  const violations: Array<string> = [];
  let calls = 0;
  let crashed = false;

  const settle = (task: FakeTask, outcome: WorkflowAgentOutcome) => {
    task.settled ??= outcome;
    return task.settled;
  };

  const port: WorkflowPort = {
    persist: (snapshot) =>
      Effect.gen(function* () {
        const mode = crashed ? undefined : options.crash?.(snapshot, calls);
        calls += 1;
        if (mode === "instead") {
          crashed = true;
          return yield* Effect.die("crash before persist");
        }
        const previous = persisted.at(-1)?.snapshot;
        if (previous !== undefined && previous.status !== "running") {
          if (snapshot.status !== previous.status) violations.push("terminal status changed");
        }
        for (const launch of previous?.launches ?? []) {
          const next = snapshot.launches.find((other) => recordKey(other) === recordKey(launch));
          if (
            launch.outcome !== null &&
            JSON.stringify(next?.outcome) !== JSON.stringify(launch.outcome)
          ) {
            violations.push(`${recordKey(launch)} outcome changed`);
          }
        }
        persisted.push({ snapshot, now: yield* Clock.currentTimeMillis });
        if (mode === "after") {
          crashed = true;
          return yield* Effect.die("crash after persist");
        }
      }),
    start: (start) =>
      Effect.gen(function* () {
        startCalls.push(start.key);
        if (options.onStart !== undefined) yield* options.onStart(start);
        const intent = persisted.find(({ snapshot }) =>
          snapshot.launches.some((launch) => recordKey(launch) === start.key),
        );
        if (intent === undefined) violations.push(`${start.key} started without intent`);
        else if (intent.snapshot.stop || intent.now >= intent.snapshot.deadline) {
          violations.push(`${start.key} launched behind a fence`);
        }
        if (
          persisted.some(({ snapshot }) =>
            snapshot.launches.some((l) => recordKey(l) === start.key && l.taskId !== null),
          )
        ) {
          violations.push(`${start.key} restarted after its task was recorded`);
        }
        for (const task of tasks.values()) {
          const samePhase = task.stage === start.stage && task.round === start.round;
          if (!samePhase && task.settled === null) {
            violations.push(`${start.key} started while ${task.key} was live`);
          }
        }
        const existing = tasks.get(`task:${start.key}`);
        if (existing !== undefined) return `task:${start.key}`;
        const reply = options.reply(start);
        const now = yield* Clock.currentTimeMillis;
        tasks.set(`task:${start.key}`, {
          key: start.key,
          stage: start.stage,
          round: start.round,
          reply,
          finishAt: now + ("afterMs" in reply ? (reply.afterMs ?? 0) : 0),
          cancelled: yield* Deferred.make<void>(),
          settled: null,
        });
        created.push(start.key);
        rendered.set(start.key, start.task);
        return `task:${start.key}`;
      }),
    await: (taskId) =>
      Effect.gen(function* () {
        const task = tasks.get(taskId);
        if (task === undefined) {
          return yield* new OrchestratorMcpFailure({ code: "task_not_found", message: taskId });
        }
        if (task.settled !== null) return task.settled;
        const cancelled = Deferred.await(task.cancelled).pipe(Effect.as(CANCELLED));
        if ("hang" in task.reply) return settle(task, yield* cancelled);
        const remaining = task.finishAt - (yield* Clock.currentTimeMillis);
        if (remaining <= 0) return settle(task, task.reply.outcome);
        const finished = Effect.sleep(remaining).pipe(Effect.as(task.reply.outcome));
        return settle(task, yield* Effect.race(finished, cancelled));
      }),
    cancel: (taskId) =>
      Effect.gen(function* () {
        const task = tasks.get(taskId);
        if (task === undefined) return;
        cancelCalls.push(task.key);
        if ("hang" in task.reply && task.reply.ignoreCancel) return;
        settle(task, CANCELLED);
        yield* Deferred.succeed(task.cancelled, undefined);
      }),
  };

  const terminalPersists = () =>
    persisted.filter(({ snapshot }) => snapshot.status !== "running").length;
  const unsettled = () => [...tasks.values()].filter((task) => task.settled === null);

  const finish = (key: string, outcome: WorkflowAgentOutcome) => {
    const task = tasks.get(`task:${key}`);
    if (task !== undefined) settle(task, outcome);
  };

  return {
    port,
    finish,
    persisted,
    rendered,
    startCalls,
    created,
    cancelCalls,
    violations,
    terminalPersists,
    unsettled,
    last: () => persisted.at(-1)?.snapshot,
  };
};

type Fake = ReturnType<typeof makeFake>;

const VERDICT = {
  type: "object",
  properties: { verdict: { type: "string", enum: ["accept", "reject"] } },
  required: ["verdict"],
};

const draft = {
  id: "draft",
  agents: [
    { id: "a", task: "Draft A." },
    { id: "b", task: "Draft B." },
  ],
};
const repeat = (max: number, onExhausted?: "fail" | "pass") => ({
  stages: [
    { id: "revise", agents: [{ id: "writer", task: "Revise {{draft}} with {{feedback}}." }] },
    {
      id: "review",
      agents: [{ id: "judge", task: "Judge {{revise.writer}}.", resultSchema: VERDICT }],
    },
  ],
  judge: "judge",
  until: { path: "verdict", equals: "accept" },
  max,
  ...(onExhausted === undefined ? {} : { onExhausted }),
});
const innerLoop = (max: number, onExhausted?: "fail" | "pass") => ({
  name: "inner-loop",
  stages: [
    draft,
    { id: "loop", repeat: repeat(max, onExhausted) },
    { id: "polish", agents: [{ id: "editor", task: "Polish {{revise.writer}}." }] },
  ],
});

/** Judge verdicts by round; every other agent answers with its key. */
const verdicts =
  (byRound: ReadonlyArray<"accept" | "reject">) =>
  (start: WorkflowAgentStart): Reply =>
    start.agent.id === "judge"
      ? { outcome: ok("verdict", { verdict: byRound[start.round - 1] ?? "reject" }) }
      : { outcome: ok(start.key) };

/** Fail-fast: a hanging sibling launches first, then its partner fails. */
const failFast = {
  name: "fail-fast",
  stages: [
    {
      id: "scan",
      agents: [
        { id: "slow", task: "Hang." },
        { id: "bad", task: "Fail." },
      ],
    },
    { id: "after", agents: [{ id: "never", task: "Unreachable." }] },
  ],
};
const failFastReply = (start: WorkflowAgentStart): Reply =>
  start.agent.id === "bad" ? { outcome: failed } : { hang: true };

/** Starts a run as WorkflowRuns does: the recorded initial snapshot, then the runner. */
const runWorkflow = (port: WorkflowPort, input: unknown) =>
  Effect.gen(function* () {
    const spec = yield* decodeWorkflowSpec(input);
    return yield* resumeWorkflow(port, spec, initialSnapshot(spec, yield* Clock.currentTimeMillis));
  });

const runOnce = (fake: Fake, input: unknown) =>
  Effect.gen(function* () {
    const spec = yield* decodeWorkflowSpec(input);
    return yield* runWorkflow(fake.port, input).pipe(
      Effect.catchDefect(() => {
        const snapshot = fake.last();
        return snapshot === undefined
          ? runWorkflow(fake.port, input)
          : resumeWorkflow(fake.port, spec, snapshot);
      }),
    );
  });

/** Crash at, and instead of, every persist of a clean run; resume each from the stored snapshot. */
const crashSweep = (
  input: unknown,
  reply: FakeOptions["reply"],
  check: (fake: Fake, result: WorkflowResult) => void,
) =>
  Effect.gen(function* () {
    const clean = makeFake({ reply });
    const baseline = yield* runWorkflow(clean.port, input);
    check(clean, baseline);
    for (let call = 0; call < clean.persisted.length; call += 1) {
      for (const mode of ["after", "instead"] as const) {
        const fake = makeFake({ reply, crash: (_, at) => (at === call ? mode : undefined) });
        const result = yield* runOnce(fake, input);
        expect({ call, mode, result }).toEqual({ call, mode, result: baseline });
        check(fake, result);
      }
    }
  });

it.effect("waits for every parallel child before starting the next stage or judge", () =>
  Effect.gen(function* () {
    const fake = makeFake({
      reply: (start) => ({
        afterMs: start.agent.id === "b" ? 10_000 : 1_000,
        outcome: start.agent.id === "judge" ? ok("v", { verdict: "accept" }) : ok(start.key),
      }),
    });

    const fiber = yield* Effect.forkChild(runWorkflow(fake.port, innerLoop(3)));
    yield* TestClock.adjust("1 minute");
    const result = yield* Fiber.join(fiber);

    expect(result.status).toBe("completed");
    expect(fake.violations).toEqual([]);
    expect(fake.startCalls).toEqual([
      "draft.a#1",
      "draft.b#1",
      "revise.writer#1",
      "review.judge#1",
      "polish.editor#1",
    ]);
  }),
);

it.effect("resumes without launching a run round agent twice", () =>
  Effect.gen(function* () {
    const fake = makeFake({
      reply: verdicts(["reject", "accept"]),
      crash: (snapshot) =>
        snapshot.launches.some(
          (launch) => recordKey(launch) === "revise.writer#2" && launch.taskId !== null,
        )
          ? "instead"
          : undefined,
    });

    const result = yield* runOnce(fake, innerLoop(3));

    expect(result.status).toBe("completed");
    expect(new Set(fake.created).size).toBe(fake.created.length);
    expect(fake.startCalls.filter((key) => key === "revise.writer#2")).toHaveLength(2);
    expect(fake.created.filter((key) => key === "revise.writer#2")).toHaveLength(1);
    expect(fake.violations).toEqual([]);

    const live = makeFake({
      reply: verdicts(["accept"]),
      crash: (snapshot) =>
        snapshot.launches.some((launch) => launch.taskId !== null && launch.outcome === null)
          ? "after"
          : undefined,
    });
    expect((yield* runOnce(live, innerLoop(3))).status).toBe("completed");
    expect(live.startCalls).toEqual(live.created);
    expect(live.violations).toEqual([]);
  }),
);

/** One live child, one instant sibling, then a stage a broken fence would launch. */
const fenced = {
  name: "fenced",
  stages: [
    {
      id: "scan",
      agents: [
        { id: "slow", task: "Hang." },
        { id: "quick", task: "Answer." },
      ],
    },
    { id: "after", agents: [{ id: "next", task: "Behind the fence." }] },
  ],
};
const fencedReply = (start: WorkflowAgentStart): Reply =>
  start.agent.id === "slow" ? { hang: true } : { outcome: ok(start.key) };

it.effect("preserves stop and deadline launch fences across restart", () =>
  Effect.gen(function* () {
    const spec = yield* decodeWorkflowSpec(fenced);
    const stopped = makeFake({
      reply: fencedReply,
      crash: (snapshot) =>
        snapshot.launches.some((l) => l.agent === "quick" && l.outcome !== null)
          ? "after"
          : undefined,
    });
    expect(Exit.isFailure(yield* Effect.exit(runWorkflow(stopped.port, fenced)))).toBe(true);
    // t3_workflow_stop writes the fence on the row, which the resumed run reads back.
    const fence = { ...stopped.last()!, stop: true };

    // The live child finishes while the runner is down, so a resume past the fence can advance.
    stopped.finish("scan.slow#1", ok("late"));
    const afterStop = yield* resumeWorkflow(stopped.port, spec, fence);

    expect(stopped.startCalls).toEqual(["scan.slow#1", "scan.quick#1"]);
    expect(afterStop.status).toBe("cancelled");
    expect(stopped.violations).toEqual([]);

    const timed = { ...fenced, timeoutMs: 5_000 };
    const timedSpec = yield* decodeWorkflowSpec(timed);
    const late = makeFake({
      reply: fencedReply,
      // The deadline passes mid-stage, and the runner crashes before deciding the sibling.
      onStart: (start) =>
        start.key === "scan.slow#1" ? TestClock.adjust("5 seconds") : Effect.void,
      crash: (snapshot) => (snapshot.launches.some((l) => l.taskId !== null) ? "after" : undefined),
    });
    expect(Exit.isFailure(yield* Effect.exit(runWorkflow(late.port, timed)))).toBe(true);
    const partial = late.last()!;
    expect(partial.launches.map(recordKey)).toEqual(["scan.slow#1"]);

    late.finish("scan.slow#1", ok("late"));
    const afterDeadline = yield* resumeWorkflow(late.port, timedSpec, partial);

    expect(late.startCalls).toEqual(["scan.slow#1"]);
    expect(afterDeadline.status).toBe("timed_out");
    expect(late.unsettled()).toEqual([]);
    expect(late.violations).toEqual([]);
  }),
);

it.effect("never launches or records a round beyond max", () =>
  Effect.gen(function* () {
    const fake = makeFake({ reply: verdicts(["reject", "reject", "reject"]) });

    const result = yield* runWorkflow(fake.port, innerLoop(2));

    expect(result.status).toBe("failed");
    expect(fake.created.filter((key) => key.startsWith("review.judge"))).toEqual([
      "review.judge#1",
      "review.judge#2",
    ]);
    expect(fake.persisted.every(({ snapshot }) => snapshot.round <= 2)).toBe(true);
    expect(result.stages[1]).toMatchObject({ id: "loop", exit: null });
    expect(result.stages[2]).toMatchObject({ agents: [{ id: "editor", status: "skipped" }] });
  }),
);

it.effect("repeat success requires acceptance or explicit exhausted pass", () =>
  Effect.gen(function* () {
    const accepted = makeFake({ reply: verdicts(["reject", "accept"]) });
    const early = yield* runWorkflow(accepted.port, innerLoop(3));

    expect(early.status).toBe("completed");
    expect(early.stages[1]).toMatchObject({ exit: "accepted" });
    expect(accepted.created).not.toContain("revise.writer#3");
    expect(accepted.rendered.get("revise.writer#1")).toBe(
      'Revise {"a":"draft.a#1","b":"draft.b#1"} with null.',
    );
    expect(accepted.rendered.get("revise.writer#2")).toBe(
      'Revise {"a":"draft.a#1","b":"draft.b#1"} with {"verdict":"reject"}.',
    );
    expect(accepted.rendered.get("polish.editor#1")).toBe("Polish revise.writer#2.");

    const passing = makeFake({ reply: verdicts([]) });
    const exhausted = yield* runWorkflow(passing.port, innerLoop(2, "pass"));
    expect(exhausted.status).toBe("completed");
    expect(exhausted.stages[1]).toMatchObject({ exit: "exhausted" });

    const unstructured = makeFake({
      reply: (start) => ({ outcome: ok(start.agent.id === "judge" ? "accept" : start.key) }),
    });
    expect((yield* runWorkflow(unstructured.port, innerLoop(1))).status).toBe("failed");
  }),
);

it.effect("a child whose JSON failed its resultSchema fails the run", () =>
  Effect.gen(function* () {
    const invalid: WorkflowAgentOutcome = {
      status: "invalid_output",
      result: "$.verdict: required",
    };
    const worker = makeFake({
      reply: (start) => ({ outcome: start.agent.id === "a" ? invalid : ok(start.key) }),
    });
    expect((yield* runWorkflow(worker.port, innerLoop(3))).status).toBe("failed");
    expect(worker.created).not.toContain("revise.writer#1");

    const judge = makeFake({
      reply: (start) => ({ outcome: start.agent.id === "judge" ? invalid : ok(start.key) }),
    });
    expect((yield* runWorkflow(judge.port, innerLoop(2, "pass"))).status).toBe("failed");
    expect(judge.created).not.toContain("polish.editor#1");
  }),
);

it.effect("late callbacks and restart cannot change terminal run or child states", () =>
  Effect.gen(function* () {
    const fake = makeFake({
      reply: (start) =>
        start.agent.id === "bad" ? { outcome: failed } : { hang: true, ignoreCancel: true },
    });
    const result = yield* runWorkflow(fake.port, failFast);
    const terminal = fake.last()!;

    fake.finish("scan.slow#1", ok("late"));
    const resumed = yield* resumeWorkflow(fake.port, yield* decodeWorkflowSpec(failFast), terminal);

    expect(result.status).toBe("failed");
    expect(resumed).toEqual(result);
    expect(result.stages[0]).toMatchObject({
      agents: [
        { id: "slow", status: "cancelled" },
        { id: "bad", status: "failed" },
      ],
    });
    expect(fake.terminalPersists()).toBe(1);
    expect(fake.violations).toEqual([]);
  }),
);

it.effect("terminal commit settles or requests cancellation of every live child", () =>
  Effect.gen(function* () {
    const fake = makeFake({
      reply: failFastReply,
      crash: (snapshot) => (snapshot.status === "failed" ? "after" : undefined),
    });

    const result = yield* runOnce(fake, failFast);

    const terminal = fake.persisted.find(({ snapshot }) => snapshot.status === "failed")!.snapshot;
    expect(terminal.launches.map((launch) => [launch.agent, launch.cancelRequested])).toEqual([
      ["slow", true],
      ["bad", false],
    ]);
    expect(terminal.launches.every((launch) => launch.outcome !== null)).toBe(true);
    expect(result.status).toBe("failed");
    expect(fake.cancelCalls).toEqual(["scan.slow#1"]);
    expect(fake.unsettled()).toEqual([]);
  }),
);

it.effect("retries and restart never deliver a second parent completion", () =>
  Effect.gen(function* () {
    const fake = makeFake({
      reply: verdicts(["accept"]),
      crash: (snapshot) => (snapshot.status === "completed" ? "after" : undefined),
    });
    const spec = yield* decodeWorkflowSpec(innerLoop(2));

    yield* runOnce(fake, innerLoop(2));
    yield* resumeWorkflow(fake.port, spec, fake.last()!);
    yield* resumeWorkflow(fake.port, spec, fake.last()!);

    expect(fake.terminalPersists()).toBe(1);
  }),
);

it.effect("eventually delivers one completion when the parent stays eligible", () =>
  crashSweep(innerLoop(3), verdicts(["reject", "accept"]), (fake) => {
    expect(fake.terminalPersists()).toBe(1);
  }),
);

it.effect("fair child completion and recovery eventually terminate the run", () =>
  Effect.gen(function* () {
    yield* crashSweep(innerLoop(2), verdicts([]), (fake, result) => {
      expect(result.status).toBe("failed");
      expect(fake.violations).toEqual([]);
      expect(new Set(fake.created).size).toBe(fake.created.length);
    });
    yield* crashSweep(innerLoop(3), verdicts(["reject", "reject", "accept"]), (fake, result) => {
      expect(result.status).toBe("completed");
      expect(fake.violations).toEqual([]);
      expect(new Set(fake.created).size).toBe(fake.created.length);
    });
  }),
);

it.effect("fair cancellation or completion eventually settles terminal run children", () =>
  crashSweep(failFast, failFastReply, (fake, result) => {
    expect(result.status).toBe("failed");
    expect(fake.unsettled()).toEqual([]);
    expect(fake.violations).toEqual([]);
  }),
);

it.effect("persisted workflow snapshots remain well formed after every transition", () =>
  crashSweep(innerLoop(3), verdicts(["reject", "accept"]), (fake) => {
    for (const { snapshot } of fake.persisted) {
      const roundTrip = decodeSnapshot(JSON.parse(JSON.stringify(snapshot)));
      expect(roundTrip).toEqual(snapshot);
      expect(snapshot.cursor).toBeGreaterThanOrEqual(0);
      expect(snapshot.cursor).toBeLessThanOrEqual(3);
      expect(snapshot.round).toBeGreaterThanOrEqual(1);
      expect(snapshot.round).toBeLessThanOrEqual(3);
      expect(snapshot.phase).toBeLessThanOrEqual(1);
    }
  }),
);

it.effect("feeds stage results forward and leaves unknown braces literal", () =>
  Effect.gen(function* () {
    const fake = makeFake({
      reply: (start) => ({ outcome: start.agent.id === "a" ? ok("A", { n: 1 }) : ok("B") }),
    });

    const result = yield* runWorkflow(fake.port, {
      name: "forward",
      stages: [
        {
          id: "scan",
          agents: [
            { id: "a", task: "A." },
            { id: "b", task: "B." },
          ],
        },
        { id: "merge", agents: [{ id: "m", task: "All {{scan}}, b {{scan.b}}, {{nope.x}}" }] },
      ],
    });

    expect(result.status).toBe("completed");
    expect(fake.rendered.get("merge.m#1")).toBe('All {"a":{"n":1},"b":"B"}, b B, {{nope.x}}');
    expect(result.stages[0]).toMatchObject({
      agents: [{ id: "a", status: "completed", structuredResult: { n: 1 } }, { id: "b" }],
    });
  }),
);

it.effect("rejects bad references and judges before any start", () =>
  Effect.gen(function* () {
    const one = (task: string) => ({ id: "x", task });
    const bad = [
      {
        name: "f",
        stages: [
          { id: "a", agents: [one("{{b.y}}")] },
          { id: "b", agents: [{ id: "y", task: "y" }] },
        ],
      },
      {
        name: "u",
        stages: [
          { id: "a", agents: [one("x")] },
          { id: "b", agents: [one("{{a.z}}")] },
        ],
      },
      { name: "s", stages: [{ id: "a", agents: [one("x"), one("{{a.x}}")] }] },
      {
        name: "d",
        stages: [
          { id: "a", agents: [one("x")] },
          { id: "a", agents: [one("y")] },
        ],
      },
      { name: "fb", stages: [{ id: "a", agents: [one("{{feedback}}")] }] },
      { name: "reserved-input", stages: [{ id: "input", agents: [one("x")] }] },
      {
        name: "template-forward",
        stages: [
          { id: "loop", repeat: { ...repeat(2), templates: { t: { task: "{{polish}}" } } } },
          { id: "polish", agents: [one("x")] },
        ],
      },
      {
        name: "template-repeat",
        stages: [{ id: "loop", repeat: { ...repeat(2), templates: { t: { task: "{{loop}}" } } } }],
      },
      { name: "dup-repeat", stages: [draft, { id: "draft", repeat: repeat(2) }] },
      { name: "judge", stages: [draft, { id: "loop", repeat: { ...repeat(2), judge: "writer" } }] },
      {
        name: "nested",
        stages: [
          {
            id: "outer",
            repeat: {
              stages: [{ id: "inner", repeat: repeat(2) }],
              judge: "judge",
              until: { path: "verdict", equals: "accept" },
              max: 2,
            },
          },
        ],
      },
    ];

    for (const input of bad) {
      const fake = makeFake({ reply: () => ({ outcome: ok("x") }) });
      const error = yield* runWorkflow(fake.port, input).pipe(Effect.flip);
      expect({ name: input.name, code: error.code }).toEqual({
        name: input.name,
        code: "invalid_request",
      });
      expect(fake.startCalls).toEqual([]);
      expect(fake.persisted).toEqual([]);
    }
  }),
);

/** Draft, probe, then a critic who judges and may grow the next round from two templates. */
const grow = (max: number, extra: Record<string, unknown> = {}) => ({
  name: "grow",
  stages: [
    {
      id: "loop",
      repeat: {
        stages: [
          { id: "draft", agents: [{ id: "poet", task: "Write {{feedback}}." }] },
          { id: "probe", agents: [{ id: "reader", task: "Read {{draft}}." }] },
          {
            id: "critique",
            agents: [{ id: "critic", task: "Judge {{probe}}.", resultSchema: VERDICT }],
          },
        ],
        judge: "critic",
        until: { path: "verdict", equals: "accept" },
        max,
        templates: {
          rewrite: { task: "Rewrite {{draft.poet}} for {{input.focus}}." },
          redteam: { task: "Attack {{draft}} on {{input.angle}}." },
        },
        ...extra,
      },
    },
    { id: "polish", agents: [{ id: "editor", task: "Polish {{draft}}." }] },
  ],
});
const rewrite = (key: string, focus: string) => ({ key, template: "rewrite", input: { focus } });
const redteam = (key: string, angle: string) => ({
  key,
  template: "redteam",
  input: { angle },
  stage: "probe",
});

/** The critic's structured result by round, rejecting unless told otherwise; others echo. */
const judged =
  (byRound: ReadonlyArray<Record<string, unknown>>) =>
  (start: WorkflowAgentStart): WorkflowAgentOutcome =>
    start.agent.id === "critic"
      ? ok("judged", { verdict: "reject", ...byRound[start.round - 1] })
      : ok(start.key);
const judging =
  (byRound: ReadonlyArray<Record<string, unknown>>) => (start: WorkflowAgentStart) => ({
    outcome: judged(byRound)(start),
  });

/** Round 1 grows both stages; round 2 repeats a key, adds one, reruns only probe; round 3 accepts. */
const GROWN = [
  { addAgents: [rewrite("rw-1", "rhythm"), redteam("rt-1", "cliche")] },
  { addAgents: [rewrite("rw-1", "again"), redteam("rt-2", "meter")], rerun: ["probe"] },
  { verdict: "accept" },
];

const roster = (result: WorkflowResult, round: number) => {
  const loop = result.stages[0];
  if (loop === undefined || !("rounds" in loop)) return [];
  return (loop.rounds[round - 1]?.stages ?? []).flatMap((stage) =>
    stage.agents.map((agent) => `${stage.id}.${agent.id}:${agent.status}`),
  );
};

it.effect(
  "grows the next round from the judge's addAgents, deduplicated by key across the run",
  () =>
    Effect.gen(function* () {
      const fake = makeFake({ reply: judging(GROWN) });

      const result = yield* runWorkflow(fake.port, grow(4));

      expect(result.status).toBe("completed");
      expect(result.stages[0]).toMatchObject({ exit: "accepted" });
      expect(fake.startCalls).toEqual([
        "draft.poet#1",
        "probe.reader#1",
        "critique.critic#1",
        "draft.poet#2",
        "draft.rw-1#2",
        "probe.reader#2",
        "probe.rt-1#2",
        "critique.critic#2",
        "probe.reader#3",
        "probe.rt-2#3",
        "critique.critic#3",
        "polish.editor#1",
      ]);
      expect(fake.rendered.get("probe.rt-1#2")).toBe(
        'Attack {"poet":"draft.poet#2","rw-1":"draft.rw-1#2"} on cliche.',
      );
      expect(roster(result, 3)).toEqual([
        "draft.poet:skipped",
        "probe.reader:completed",
        "probe.rt-2:completed",
        "critique.critic:completed",
      ]);
      // Grown rows are recorded with the judge's result, before the next round starts anything.
      const grown = fake.persisted.find(({ snapshot }) =>
        snapshot.launches.some((l) => recordKey(l) === "probe.rt-1#2"),
      )!.snapshot;
      expect(
        grown.launches.filter((l) => l.round === 2).map((l) => [recordKey(l), l.taskId]),
      ).toEqual([
        ["draft.rw-1#2", null],
        ["probe.rt-1#2", null],
      ]);
      expect(fake.violations).toEqual([]);
    }),
);

it.effect("exits after K consecutive dry votes, never on an invalid or missing judge result", () =>
  Effect.gen(function* () {
    const dry = makeFake({ reply: judging([{ addAgents: [rewrite("rw-1", "x")] }]) });
    const dried = yield* runWorkflow(dry.port, grow(4, { dry: 2 }));
    expect(dried.status).toBe("completed");
    expect(dried.stages[0]).toMatchObject({ exit: "dry" });
    expect(dry.created.filter((key) => key.startsWith("critique."))).toEqual([
      "critique.critic#1",
      "critique.critic#2",
      "critique.critic#3",
    ]);

    // A rerun request is unresolved work, so those rounds never vote dry.
    const busy = makeFake({ reply: judging([{ rerun: ["draft"] }, { rerun: ["draft"] }]) });
    expect((yield* runWorkflow(busy.port, grow(2, { dry: 1 }))).status).toBe("failed");

    for (const bad of [
      { addAgents: [{ key: "x", template: "nope" }] },
      { addAgents: [{ key: "x", template: "rewrite", stage: "polish" }] },
      { rerun: ["polish"] },
    ]) {
      const invalid = makeFake({ reply: judging([bad]) });
      expect((yield* runWorkflow(invalid.port, grow(3, { dry: 1 }))).status).toBe("failed");
      expect(invalid.created).not.toContain("draft.poet#2");
    }
    const missing = makeFake({ reply: (start) => ({ outcome: ok(start.key) }) });
    expect((yield* runWorkflow(missing.port, grow(3, { dry: 1 }))).status).toBe("failed");
    expect(missing.created).not.toContain("draft.poet#2");
  }),
);

it.effect("waits for grown agents before starting the next stage or judge", () =>
  Effect.gen(function* () {
    const fake = makeFake({
      reply: (start) => ({
        afterMs: start.agent.id === "rw-1" ? 10_000 : 1_000,
        outcome: judged(GROWN)(start),
      }),
    });

    const fiber = yield* Effect.forkChild(runWorkflow(fake.port, grow(4)));
    yield* TestClock.adjust("1 minute");
    const result = yield* Fiber.join(fiber);

    expect(result.status).toBe("completed");
    expect(fake.startCalls.indexOf("probe.reader#2")).toBeGreaterThan(
      fake.startCalls.indexOf("draft.rw-1#2"),
    );
    expect(fake.violations).toEqual([]);
  }),
);

it.effect("resumes a grown run without launching an instance twice", () =>
  Effect.gen(function* () {
    const fake = makeFake({
      reply: judging(GROWN),
      crash: (snapshot) =>
        snapshot.launches.some((l) => recordKey(l) === "draft.rw-1#2" && l.taskId !== null)
          ? "instead"
          : undefined,
    });

    const result = yield* runOnce(fake, grow(4));

    expect(result.status).toBe("completed");
    expect(fake.startCalls.filter((key) => key === "draft.rw-1#2")).toHaveLength(2);
    expect(fake.created.filter((key) => key === "draft.rw-1#2")).toHaveLength(1);
    expect(new Set(fake.created).size).toBe(fake.created.length);
    expect(fake.violations).toEqual([]);
  }),
);

it.effect("a stop after growth never starts a grown instance, across restart", () =>
  Effect.gen(function* () {
    const spec = yield* decodeWorkflowSpec(grow(4));
    const fake = makeFake({
      reply: judging(GROWN),
      crash: (snapshot) =>
        snapshot.launches.some((l) => recordKey(l) === "draft.rw-1#2") ? "after" : undefined,
    });
    expect(Exit.isFailure(yield* Effect.exit(runWorkflow(fake.port, grow(4))))).toBe(true);

    const result = yield* resumeWorkflow(fake.port, spec, { ...fake.last()!, stop: true });

    expect(result.status).toBe("cancelled");
    expect(fake.startCalls.filter((key) => key.endsWith("#2"))).toEqual([]);
    expect(
      fake
        .last()!
        .launches.filter((l) => l.round === 2)
        .map((l) => [recordKey(l), l.outcome?.status]),
    ).toEqual([["draft.rw-1#2", "cancelled"]]);
    expect(fake.violations).toEqual([]);
  }),
);

it.effect("never grows a round beyond max", () =>
  Effect.gen(function* () {
    const fake = makeFake({
      reply: (start) => ({
        outcome: judged([1, 2, 3].map((n) => ({ addAgents: [rewrite(`rw-${n}`, "x")] })))(start),
      }),
    });

    const result = yield* runWorkflow(fake.port, grow(2));

    expect(result.status).toBe("failed");
    expect(
      fake.persisted.every(({ snapshot }) => snapshot.launches.every((l) => l.round <= 2)),
    ).toBe(true);
    expect(fake.created.filter((key) => key.endsWith("#3"))).toEqual([]);
  }),
);

it.effect("a run that grows survives a crash at, and instead of, every persist", () =>
  Effect.gen(function* () {
    const check = (fake: Fake, result: WorkflowResult) => {
      expect(result.status).toBe("completed");
      expect(fake.violations).toEqual([]);
      expect(new Set(fake.created).size).toBe(fake.created.length);
      for (const { snapshot } of fake.persisted) {
        expect(decodeSnapshot(JSON.parse(JSON.stringify(snapshot)))).toEqual(snapshot);
      }
    };
    yield* crashSweep(grow(4), judging(GROWN), check);
    yield* crashSweep(
      grow(4, { dry: 2 }),
      judging([{ addAgents: [rewrite("rw-1", "x"), redteam("rt-1", "y")] }]),
      check,
    );
  }),
);
