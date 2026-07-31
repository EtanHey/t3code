import { describe, expect, it } from "@effect/vitest";

import { deriveThreadLifecycle } from "./lifecycle.ts";

const baseInput = {
  isEvidenceComplete: true,
  sessionStatus: null,
  latestTurnState: null,
  hasPendingApprovals: false,
  hasPendingUserInput: false,
} as const;

describe("deriveThreadLifecycle", () => {
  it("returns unknown before using incomplete structural evidence", () => {
    expect(
      deriveThreadLifecycle({
        ...baseInput,
        isEvidenceComplete: false,
        sessionStatus: "error",
        latestTurnState: "error",
        hasPendingApprovals: true,
        hasPendingUserInput: true,
      }),
    ).toBe("unknown");
  });

  it("gives terminal session states highest precedence", () => {
    for (const status of ["error", "interrupted", "stopped"] as const) {
      expect(
        deriveThreadLifecycle({
          ...baseInput,
          sessionStatus: status,
          latestTurnState: "running",
          hasPendingApprovals: true,
          hasPendingUserInput: true,
        }),
      ).toBe(status);
    }
  });

  it("derives awaiting-input from either typed pending-request flag", () => {
    expect(
      deriveThreadLifecycle({
        ...baseInput,
        sessionStatus: "running",
        latestTurnState: "running",
        hasPendingApprovals: true,
      }),
    ).toBe("awaiting-input");
    expect(
      deriveThreadLifecycle({
        ...baseInput,
        sessionStatus: "running",
        latestTurnState: "running",
        hasPendingUserInput: true,
      }),
    ).toBe("awaiting-input");
  });

  it("derives starting and running only without terminal turn contradictions", () => {
    expect(
      deriveThreadLifecycle({
        ...baseInput,
        sessionStatus: "starting",
        latestTurnState: "running",
      }),
    ).toBe("starting");
    expect(
      deriveThreadLifecycle({
        ...baseInput,
        sessionStatus: "running",
        latestTurnState: "running",
      }),
    ).toBe("running");

    for (const sessionStatus of ["starting", "running"] as const) {
      for (const latestTurnState of ["completed", "interrupted", "error"] as const) {
        expect(
          deriveThreadLifecycle({
            ...baseInput,
            sessionStatus,
            latestTurnState,
          }),
        ).toBe("unknown");
      }
    }
  });

  it("derives terminal lifecycle from the latest turn when the session is not active", () => {
    for (const sessionStatus of ["ready", "idle", null] as const) {
      for (const latestTurnState of ["completed", "interrupted", "error"] as const) {
        expect(
          deriveThreadLifecycle({
            ...baseInput,
            sessionStatus,
            latestTurnState,
          }),
        ).toBe(latestTurnState);
      }
    }
  });

  it("derives ready only from an explicit non-contradictory ready session", () => {
    expect(
      deriveThreadLifecycle({
        ...baseInput,
        sessionStatus: "ready",
      }),
    ).toBe("ready");

    for (const sessionStatus of ["idle", null] as const) {
      expect(
        deriveThreadLifecycle({
          ...baseInput,
          sessionStatus,
        }),
      ).toBe("unknown");
    }
    expect(
      deriveThreadLifecycle({
        ...baseInput,
        sessionStatus: "ready",
        latestTurnState: "running",
      }),
    ).toBe("unknown");
  });

  it("returns unknown when structural evidence is missing or contradictory", () => {
    expect(deriveThreadLifecycle(baseInput)).toBe("unknown");
    expect(
      deriveThreadLifecycle({
        ...baseInput,
        latestTurnState: "running",
      }),
    ).toBe("unknown");
    expect(
      deriveThreadLifecycle({
        ...baseInput,
        sessionStatus: "idle",
        latestTurnState: "running",
      }),
    ).toBe("unknown");
  });
});
