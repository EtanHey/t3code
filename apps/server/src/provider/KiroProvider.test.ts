import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { KiroSettings } from "@t3tools/provider-kiro/settings";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import * as TestClock from "effect/testing/TestClock";

import {
  checkKiroProviderStatus,
  kiroAgentDescriptor,
  kiroDefaultModelId,
} from "@t3tools/provider-kiro/testing";

const encoder = new TextEncoder();
const settings = Schema.decodeSync(KiroSettings)({ enabled: true });

interface CliOutput {
  readonly stdout: string;
  readonly code: number;
}

function processHandle(output: CliOutput) {
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(900_000_002),
    exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(output.code)),
    isRunning: Effect.succeed(false),
    kill: () => Effect.void,
    unref: Effect.succeed(Effect.void),
    stdin: Sink.drain,
    stdout:
      output.stdout.length === 0 ? Stream.empty : Stream.succeed(encoder.encode(output.stdout)),
    stderr: Stream.empty,
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });
}

const KIRO_ACP_ARGS = "acp --agent-engine=v3 --auth-method=cli";
const decodeRequest = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({ id: Schema.Union([Schema.String, Schema.Number]), method: Schema.String }),
  ),
);

type AcpReply =
  | { readonly result: unknown; readonly resume?: Deferred.Deferred<void> }
  | { readonly error: { readonly code: number; readonly message: string } }
  /** Never answered; completes once the request arrives. */
  | { readonly received: Deferred.Deferred<void> };

/**
 * A `kiro-cli acp` that answers `initialize` and the scripted methods and
 * fails anything else, so a probe that opened a session would fail.
 */
const acpProcess = (replies: Readonly<Record<string, AcpReply>>, requested: Array<string>) =>
  Effect.gen(function* () {
    const output = yield* Queue.unbounded<Uint8Array>();
    const exited = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
    const terminate = Deferred.succeed(exited, ChildProcessSpawner.ExitCode(0)).pipe(
      Effect.andThen(Queue.shutdown(output)),
      Effect.asVoid,
    );
    const reply = (line: string) => {
      const request = decodeRequest(line);
      requested.push(request.method);
      const response =
        replies[request.method] ??
        (request.method === "initialize"
          ? { result: { protocolVersion: 1, agentCapabilities: {}, authMethods: [] } }
          : { error: { code: -32601, message: `${request.method} is not scripted` } });
      if ("received" in response) return Deferred.succeed(response.received, undefined);
      const send = Queue.offer(
        output,
        encoder.encode(
          `${JSON.stringify({ jsonrpc: "2.0", id: request.id, ...("result" in response ? { result: response.result } : response) })}\n`,
        ),
      );
      return "resume" in response && response.resume
        ? Deferred.await(response.resume).pipe(Effect.andThen(send))
        : send;
    };
    return ChildProcessSpawner.makeHandle({
      pid: ChildProcessSpawner.ProcessId(900_000_003),
      exitCode: Deferred.await(exited),
      isRunning: Deferred.isDone(exited).pipe(Effect.map((done) => !done)),
      kill: () => terminate,
      unref: Effect.succeed(Effect.void),
      stdin: Sink.forEach((bytes: Uint8Array) =>
        Effect.forEach(
          new TextDecoder()
            .decode(bytes)
            .split("\n")
            .filter((line) => line.trim().length > 0),
          reply,
        ),
      ),
      stdout: Stream.fromQueue(output),
      stderr: Stream.empty,
      all: Stream.empty,
      getInputFd: () => Sink.drain,
      getOutputFd: () => Stream.empty,
    });
  });

/** Answers `kiro-cli` invocations by their arguments and records which ran. */
function kiroCli(outputs: Record<string, CliOutput>, replies?: Readonly<Record<string, AcpReply>>) {
  const invoked: Array<string> = [];
  const requested: Array<string> = [];
  const spawner = ChildProcessSpawner.make((command) => {
    const args = ChildProcess.isStandardCommand(command) ? command.args.join(" ") : "";
    invoked.push(args);
    return args === KIRO_ACP_ARGS && replies !== undefined
      ? acpProcess(replies, requested)
      : Effect.succeed(processHandle(outputs[args] ?? { stdout: "", code: 127 }));
  });
  return { invoked, requested, spawner };
}

const check = (
  outputs: Record<string, CliOutput>,
  options: {
    readonly template?: unknown;
    readonly usage?: AcpReply;
    readonly initialize?: AcpReply;
    readonly settings?: KiroSettings;
  } = {},
) =>
  Effect.gen(function* () {
    const replies = {
      ...(options.initialize === undefined ? {} : { initialize: options.initialize }),
      ...(options.template === undefined
        ? {}
        : { "_kiro/config/template": { result: options.template } }),
      ...(options.usage === undefined ? {} : { "_kiro/account/getUsage": options.usage }),
    };
    const cli = kiroCli(outputs, Object.keys(replies).length === 0 ? undefined : replies);
    const snapshot = yield* checkKiroProviderStatus(options.settings ?? settings, {}).pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, cli.spawner),
    );
    return { snapshot, invoked: cli.invoked, requested: cli.requested };
  }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)));

describe("KiroProvider", () => {
  it.effect("reports a CLI whose version probe exits nonzero as failing to run", () =>
    Effect.gen(function* () {
      const { snapshot, invoked } = yield* check({
        "--version": { stdout: "kiro-cli 2.27.0\n", code: 1 },
      });
      assert.equal(snapshot.status, "error");
      assert.isTrue(snapshot.installed);
      assert.equal(snapshot.message, "Kiro CLI is installed but failed to run.");
      // A broken CLI is not asked for its sign-in.
      assert.deepEqual(invoked, ["--version"]);
    }),
  );

  // `whoami` outputs recorded from Kiro CLI 2.27.0.
  it.effect("asks for kiro-cli login when Kiro is signed out", () =>
    Effect.gen(function* () {
      const { snapshot } = yield* check({
        "--version": { stdout: "kiro-cli 2.27.0\n", code: 0 },
        "whoami --format json": { stdout: '{"account":null}\n', code: 1 },
      });
      assert.equal(snapshot.status, "error");
      assert.equal(snapshot.version, "2.27.0");
      assert.equal(snapshot.auth.status, "unauthenticated");
      assert.include(snapshot.message ?? "", "kiro-cli login");
    }),
  );

  it.effect("is ready once Kiro reports a signed-in account", () =>
    Effect.gen(function* () {
      const { snapshot } = yield* check({
        "--version": { stdout: "kiro-cli 2.27.0\n", code: 0 },
        "whoami --format json": { stdout: '{"accountType":"ApiKey","email":null}\n', code: 0 },
      });
      assert.equal(snapshot.status, "ready");
      assert.equal(snapshot.auth.status, "authenticated");
      // The version advisory is what the compatibility check and update UI read.
      assert.equal(snapshot.versionAdvisory?.currentVersion, "2.27.0");
      assert.isFalse(snapshot.versionAdvisory?.canUpdate);
    }),
  );

  // Shape of `kiro-cli chat --list-models --format json` on Kiro CLI 2.27.0, trimmed to three models.
  const KIRO_MODEL_LIST = JSON.stringify({
    models: [
      { model_name: "auto", model_id: "auto", description: "Models chosen by task" },
      { model_name: "claude-sonnet-4.5", model_id: "claude-sonnet-4.5" },
      { model_name: "claude-haiku-4.5", model_id: "claude-haiku-4.5" },
    ],
    default_model: "auto",
  });

  it.effect("lists the signed-in account's models with auto as the Kiro default", () =>
    Effect.gen(function* () {
      const { snapshot } = yield* check({
        "--version": { stdout: "kiro-cli 2.27.0\n", code: 0 },
        "whoami --format json": { stdout: '{"accountType":"SocialGitHub"}\n', code: 0 },
        "chat --list-models --format json": { stdout: `${KIRO_MODEL_LIST}\n`, code: 0 },
      });
      assert.deepEqual(
        snapshot.models.map((model) => [model.slug, model.name, model.isDefault === true]),
        [
          ["default", "auto", true],
          ["claude-sonnet-4.5", "claude-sonnet-4.5", false],
          ["claude-haiku-4.5", "claude-haiku-4.5", false],
        ],
      );
    }),
  );

  it.effect("names Kiro default after the model Kiro's list names as its default", () =>
    Effect.gen(function* () {
      const signedIn = {
        "--version": { stdout: "kiro-cli 2.27.0\n", code: 0 },
        "whoami --format json": { stdout: '{"accountType":"SocialGitHub"}\n', code: 0 },
      };
      const listed = yield* check({
        ...signedIn,
        "chat --list-models --format json": {
          stdout: KIRO_MODEL_LIST.replace(
            '"default_model":"auto"',
            '"default_model":"claude-sonnet-4.5"',
          ),
          code: 0,
        },
      });
      assert.deepEqual(
        listed.snapshot.models.map((model) => [model.slug, model.name]),
        [
          ["auto", "auto"],
          ["default", "claude-sonnet-4.5"],
          ["claude-haiku-4.5", "claude-haiku-4.5"],
        ],
      );
      assert.equal(kiroDefaultModelId(listed.snapshot.models), "claude-sonnet-4.5");
      // An unparsable list keeps the built-in entry, which falls back to `auto`.
      const unlisted = yield* check({
        ...signedIn,
        "chat --list-models --format json": { stdout: "not json\n", code: 0 },
      });
      assert.equal(unlisted.snapshot.models[0]?.name, "Kiro default");
      assert.equal(kiroDefaultModelId(unlisted.snapshot.models), "auto");
    }),
  );

  it.effect("lists Kiro default even when Kiro's model list leaves its default out", () =>
    Effect.gen(function* () {
      const { snapshot } = yield* check({
        "--version": { stdout: "kiro-cli 2.27.0\n", code: 0 },
        "whoami --format json": { stdout: '{"accountType":"SocialGitHub"}\n', code: 0 },
        "chat --list-models --format json": {
          stdout: JSON.stringify({ models: [{ model_id: "claude-sonnet-4.5" }] }),
          code: 0,
        },
      });
      assert.deepEqual(
        snapshot.models.map((model) => [model.slug, model.name, model.isDefault === true]),
        [
          ["default", "Kiro default", true],
          ["claude-sonnet-4.5", "claude-sonnet-4.5", false],
        ],
      );
      assert.equal(kiroDefaultModelId(snapshot.models), "auto");
    }),
  );

  // `--list-models` names no effort levels, so the picker's come from Kiro's ids.
  const KIRO_EFFORT_MODEL_LIST = JSON.stringify({
    models: [
      "auto",
      "claude-fable-5.1",
      "claude-opus-5.5",
      "claude-opus-4.6",
      "kimi-k3",
      "claude-sonnet-4.5",
    ].map((id) => ({
      model_name: id,
      model_id: id,
    })),
    default_model: "auto",
  });

  it.effect("offers each model only the reasoning levels Kiro takes for it", () =>
    Effect.gen(function* () {
      const { snapshot } = yield* check({
        "--version": { stdout: "kiro-cli 2.27.0\n", code: 0 },
        "whoami --format json": { stdout: '{"accountType":"SocialGitHub"}\n', code: 0 },
        "chat --list-models --format json": { stdout: KIRO_EFFORT_MODEL_LIST, code: 0 },
      });
      const reasoning = snapshot.models.map((model) => {
        const descriptors = model.capabilities?.optionDescriptors ?? [];
        const select = descriptors.find((descriptor) => descriptor.id === "reasoningEffort");
        return select?.type === "select"
          ? {
              slug: model.slug,
              label: select.label,
              levels: select.options.map((choice) => choice.id),
              defaults: select.options.filter((choice) => choice.isDefault).map((c) => c.id),
              currentValue: select.currentValue,
            }
          : { slug: model.slug, descriptors: descriptors.length };
      });
      assert.deepEqual(reasoning, [
        // Kiro default and models without effort get no Reasoning select.
        { slug: "default", descriptors: 0 },
        {
          slug: "claude-fable-5.1",
          label: "Reasoning",
          levels: ["low", "medium", "high", "xhigh", "max"],
          defaults: ["high"],
          currentValue: "high",
        },
        {
          slug: "claude-opus-5.5",
          label: "Reasoning",
          levels: ["low", "medium", "high", "xhigh", "max"],
          defaults: ["medium"],
          currentValue: "medium",
        },
        // Opus 4.6 has no xhigh, and Kiro runs it at high by default.
        {
          slug: "claude-opus-4.6",
          label: "Reasoning",
          levels: ["low", "medium", "high", "max"],
          defaults: ["high"],
          currentValue: "high",
        },
        // Kimi K3 skips medium and runs at max by default.
        {
          slug: "kimi-k3",
          label: "Reasoning",
          levels: ["low", "high", "max"],
          defaults: ["max"],
          currentValue: "max",
        },
        { slug: "claude-sonnet-4.5", descriptors: 0 },
      ]);
    }),
  );

  it.effect("gives every model, custom ones too, the agent picker Kiro's template lists", () =>
    Effect.gen(function* () {
      const { snapshot } = yield* check(
        {
          "--version": { stdout: "kiro-cli 2.27.0\n", code: 0 },
          "whoami --format json": { stdout: '{"accountType":"SocialGitHub"}\n', code: 0 },
          "chat --list-models --format json": { stdout: KIRO_MODEL_LIST, code: 0 },
        },
        {
          template: {
            configOptions: [
              {
                type: "select",
                id: "mode",
                name: "Mode",
                currentValue: "vibe",
                options: [
                  { value: "vibe", name: "Default" },
                  { value: "reviewer", name: "reviewer" },
                ],
              },
            ],
          },
          settings: { ...settings, customModels: ["my-model"] },
        },
      );
      assert.deepEqual(
        snapshot.models.map((model) => [
          model.slug,
          model.capabilities?.optionDescriptors?.flatMap((descriptor) =>
            descriptor.type === "select" && descriptor.id === "mode"
              ? descriptor.options.map((choice) => choice.id)
              : [],
          ),
        ]),
        [
          ["default", ["vibe", "reviewer"]],
          ["claude-sonnet-4.5", ["vibe", "reviewer"]],
          ["claude-haiku-4.5", ["vibe", "reviewer"]],
          ["my-model", ["vibe", "reviewer"]],
        ],
      );
    }),
  );

  it("offers Kiro's agents from its config template, Default first", () => {
    // `_kiro/config/template` from kiro-cli 2.27.0 with two user agents in
    // `~/.kiro/agents`, trimmed to three bundled agents and the `mode` option.
    const template = {
      modes: { currentModeId: "vibe", availableModes: [] },
      configOptions: [
        {
          type: "select",
          id: "mode",
          name: "Mode",
          category: "mode",
          currentValue: "vibe",
          options: [
            { value: "vibe", name: "Default", description: "General coding assistance" },
            { value: "spec", name: "Spec", description: "Structured feature development" },
            { value: "plan", name: "Plan", description: "Plan-only mode" },
            { value: "global-probe", name: "global-probe", description: "Global probe agent" },
            { value: "planner", name: "planner", description: null },
          ],
        },
        {
          type: "select",
          id: "autopilot",
          name: "Autopilot",
          currentValue: "on",
          options: [{ value: "on", name: "Autopilot" }],
        },
      ],
    };
    const descriptor = kiroAgentDescriptor(template);
    assert.deepEqual(descriptor, {
      id: "mode",
      label: "Agent",
      type: "select",
      currentValue: "vibe",
      options: [
        { id: "vibe", label: "Default", description: "General coding assistance", isDefault: true },
        { id: "spec", label: "Spec", description: "Structured feature development" },
        { id: "plan", label: "Plan", description: "Plan-only mode" },
        { id: "global-probe", label: "global-probe", description: "Global probe agent" },
        { id: "planner", label: "planner" },
      ],
    });
    // The default follows the agent Kiro starts sessions on.
    const onSpec = kiroAgentDescriptor({
      configOptions: [{ ...template.configOptions[0], currentValue: "spec" }],
    });
    assert.equal(onSpec?.type === "select" ? onSpec.currentValue : undefined, "spec");
    assert.deepEqual(
      onSpec?.type === "select" ? onSpec.options.filter((choice) => choice.isDefault) : [],
      [
        {
          id: "spec",
          label: "Spec",
          description: "Structured feature development",
          isDefault: true,
        },
      ],
    );
    // A template without agents to choose between gives no picker.
    assert.isUndefined(kiroAgentDescriptor({ configOptions: [] }));
    assert.isUndefined(kiroAgentDescriptor("not a template"));
  });

  it.effect("never lists models while signed out, where listing would start a browser login", () =>
    Effect.gen(function* () {
      const { snapshot, invoked } = yield* check({
        "--version": { stdout: "kiro-cli 2.27.0\n", code: 0 },
        "whoami --format json": { stdout: '{"account":null}\n', code: 1 },
        "chat --list-models --format json": { stdout: `${KIRO_MODEL_LIST}\n`, code: 0 },
      });
      assert.notInclude(invoked, "chat --list-models --format json");
      assert.deepEqual(
        snapshot.models.map((model) => model.slug),
        ["default"],
      );
    }),
  );

  const SIGNED_IN = {
    "--version": { stdout: "kiro-cli 2.35.2\n", code: 0 },
    "whoami --format json": {
      stdout: '{"accountType":"IdC","email":"dev@example.com"}\n',
      code: 0,
    },
  };
  // `_kiro/account/getUsage` from Kiro CLI 2.35.2 on a plan past its allowance with overages on.
  const KIRO_ACCOUNT_USAGE = {
    success: true,
    message: "Plan: KIRO POWER | 1 usage breakdowns",
    data: {
      planName: "KIRO POWER",
      billingCycleReset: "2026-11-01",
      overagesEnabled: true,
      isEnterprise: true,
      usageBreakdowns: [
        {
          resourceType: "CREDIT",
          displayName: "Credits",
          used: 15034.06,
          limit: 10000,
          percentage: 150,
          currentOverages: 5034.06,
          overageRate: 0.04,
          overageCharges: 201.362463126896,
          currency: "USD",
          hasLimit: true,
        },
      ],
      bonusCredits: [],
      addOnCredits: [],
      overageCapable: true,
    },
  };

  // Sanitized session-free `_kiro/account/getUsage` response from Kiro CLI 2.37.1.
  const KIRO_FREE_USAGE = {
    success: true,
    data: {
      planName: "KIRO FREE",
      billingCycleReset: "2026-12-01",
      overagesEnabled: false,
      isEnterprise: false,
      usageBreakdowns: [
        {
          resourceType: "CREDIT",
          displayName: "Credits",
          used: 0,
          limit: 50,
          percentage: 0,
          currentOverages: 0,
          overageRate: 0.04,
          overageCharges: 0,
          currency: "USD",
          hasLimit: true,
        },
      ],
      bonusCredits: [],
      addOnCredits: [],
      overageCapable: false,
    },
  };

  it.effect("reports paid and FREE plan credits from Kiro's account usage, without a session", () =>
    Effect.gen(function* () {
      const freeCredits = KIRO_FREE_USAGE.data.usageBreakdowns[0];
      const cases = [
        { usage: KIRO_ACCOUNT_USAGE, usedPercent: 100, label: "Credits · overage" },
        ...(
          [
            [0, 0],
            [25, 50],
            [50, 100],
            [75, 100],
          ] as const
        ).map(([used, usedPercent]) => ({
          usage: {
            ...KIRO_FREE_USAGE,
            data: {
              ...KIRO_FREE_USAGE.data,
              usageBreakdowns: [{ ...freeCredits, used }],
            },
          },
          usedPercent,
          label: "Credits",
        })),
      ];
      for (const { usage, usedPercent, label } of cases) {
        const { snapshot, requested } = yield* check(SIGNED_IN, {
          usage: { result: usage },
        });
        assert.deepEqual(snapshot.usageLimits, {
          checkedAt: snapshot.checkedAt,
          windows: [
            {
              id: "credit",
              kind: "monthly",
              label,
              usedPercent,
              ...(usage.data.overagesEnabled ? { overagesEnabled: true } : {}),
              resetsAt: `${usage.data.billingCycleReset}T00:00:00.000Z`,
            },
          ],
        });
        assert.sameMembers(requested, [
          "initialize",
          "_kiro/config/template",
          "_kiro/account/getUsage",
        ]);
      }
    }),
  );

  it.effect("shows each limited allowance and skips unlimited or unreadable ones", () =>
    Effect.gen(function* () {
      const credits = KIRO_ACCOUNT_USAGE.data.usageBreakdowns[0];
      const { snapshot } = yield* check(SIGNED_IN, {
        usage: {
          result: {
            success: true,
            data: {
              ...KIRO_ACCOUNT_USAGE.data,
              billingCycleReset: "Unknown",
              overagesEnabled: false,
              usageBreakdowns: [
                { ...credits, used: 2500, limit: 10000 },
                // A resource Kiro may add later keeps its own bar under Kiro's name.
                { resourceType: "AGENT_TASK", displayName: "Agent tasks", used: 3, limit: 4 },
                {
                  resourceType: "SPEC",
                  displayName: "Specs",
                  used: 1,
                  limit: 999999,
                  hasLimit: false,
                },
                { resourceType: "BROKEN", used: "many", limit: 10 },
                { ...credits, used: 9000 },
              ],
            },
          },
        },
      });
      assert.deepEqual(snapshot.usageLimits?.windows, [
        { id: "agent_task", kind: "monthly", label: "Agent tasks", usedPercent: 75 },
        { id: "credit", kind: "monthly", label: "Credits", usedPercent: 25 },
      ]);
      assert.isUndefined(snapshot.usageLimits?.unavailable);
    }),
  );

  it.effect("reports why Kiro's credit usage is missing, and keeps the agent picker", () =>
    Effect.gen(function* () {
      const template = {
        configOptions: [
          {
            type: "select",
            id: "mode",
            currentValue: "vibe",
            options: [
              { value: "vibe", name: "Default" },
              { value: "reviewer", name: "reviewer" },
            ],
          },
        ],
      };
      const cases: ReadonlyArray<readonly [AcpReply, "probeFailed" | "unsupported"]> = [
        [{ error: { code: -32603, message: "Internal error" } }, "probeFailed"],
        [
          { result: { success: false, message: "Failed to retrieve usage information" } },
          "probeFailed",
        ],
        [{ result: "not usage" }, "probeFailed"],
        [{ result: { success: true, data: { usageBreakdowns: [{ used: 1 }] } } }, "probeFailed"],
        // Kiro's answer for a plan its organization manages.
        [{ result: { success: true, message: "Your plan is managed by admin" } }, "unsupported"],
        [
          {
            result: {
              success: true,
              data: {
                usageBreakdowns: [
                  { resourceType: "CREDIT", used: 5, limit: 999999, hasLimit: false },
                ],
              },
            },
          },
          "unsupported",
        ],
      ];
      for (const [usage, reason] of cases) {
        const { snapshot } = yield* check(SIGNED_IN, { usage, template });
        assert.deepEqual(snapshot.usageLimits?.windows, []);
        assert.equal(snapshot.usageLimits?.unavailable?.reason, reason);
        assert.equal(snapshot.status, "ready");
        assert.isTrue(
          snapshot.models.every((model) =>
            model.capabilities?.optionDescriptors?.some((descriptor) => descriptor.id === "mode"),
          ),
        );
      }
    }),
  );

  it.effect("keeps the agent picker when Kiro never answers the usage request", () =>
    Effect.gen(function* () {
      for (const initializationSeconds of [0, 6]) {
        const resume = yield* Deferred.make<void>();
        const received = yield* Deferred.make<void>();
        const probe = yield* check(SIGNED_IN, {
          initialize: {
            result: { protocolVersion: 1, agentCapabilities: {}, authMethods: [] },
            resume,
          },
          usage: { received },
          template: {
            configOptions: [
              {
                type: "select",
                id: "mode",
                currentValue: "vibe",
                options: [
                  { value: "vibe", name: "Default" },
                  { value: "reviewer", name: "reviewer" },
                ],
              },
            ],
          },
        }).pipe(Effect.forkChild);
        yield* TestClock.adjust(`${initializationSeconds} seconds`);
        yield* Deferred.succeed(resume, undefined);
        yield* Deferred.await(received);
        yield* TestClock.adjust("5 seconds");
        const { snapshot, requested } = yield* Fiber.join(probe);
        assert.equal(snapshot.usageLimits?.unavailable?.reason, "probeFailed");
        assert.isTrue(
          snapshot.models.every((model) =>
            model.capabilities?.optionDescriptors?.some((descriptor) => descriptor.id === "mode"),
          ),
        );
        assert.sameMembers(requested, [
          "initialize",
          "_kiro/config/template",
          "_kiro/account/getUsage",
        ]);
      }
    }),
  );

  it.effect("never asks for account usage while signed out", () =>
    Effect.gen(function* () {
      const { snapshot, invoked, requested } = yield* check(
        {
          "--version": { stdout: "kiro-cli 2.35.2\n", code: 0 },
          "whoami --format json": { stdout: '{"account":null}\n', code: 1 },
        },
        { usage: { result: KIRO_ACCOUNT_USAGE } },
      );
      assert.notInclude(invoked, KIRO_ACP_ARGS);
      assert.deepEqual(requested, []);
      assert.isUndefined(snapshot.usageLimits);
    }),
  );
});
