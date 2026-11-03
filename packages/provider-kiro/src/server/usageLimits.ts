import type { ServerProviderUsageLimits, ServerProviderUsageWindow } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import {
  clampPercent,
  makeUnavailableUsageLimits,
  makeUsageLimits,
} from "@t3tools/provider-core/server/usageLimits";

/**
 * Kiro V3's account usage request. It needs no session, so no MCP server
 * starts. Kiro 2.33 to 2.35 answer it but leave it out of `extensionMethods`.
 */
export const KIRO_ACCOUNT_USAGE_METHOD = "_kiro/account/getUsage";

const KiroAccountUsage = Schema.Struct({
  success: Schema.Boolean,
  data: Schema.optional(
    Schema.NullOr(
      Schema.Struct({
        billingCycleReset: Schema.optional(Schema.String),
        overagesEnabled: Schema.optional(Schema.Boolean),
        usageBreakdowns: Schema.optional(Schema.Array(Schema.Unknown)),
        bonusCredits: Schema.optional(Schema.Array(Schema.Unknown)),
        addOnCredits: Schema.optional(Schema.Array(Schema.Unknown)),
      }),
    ),
  ),
});
const decodeKiroAccountUsage = Schema.decodeUnknownOption(KiroAccountUsage);
const KiroUsageBreakdown = Schema.Struct({
  resourceType: Schema.String,
  displayName: Schema.optional(Schema.String),
  used: Schema.Finite,
  limit: Schema.Finite,
  hasLimit: Schema.optional(Schema.Boolean),
});
const decodeKiroUsageBreakdown = Schema.decodeUnknownOption(KiroUsageBreakdown);

const KIRO_RESET_DAY = /^\d{4}-\d{2}-\d{2}$/;

/** Kiro sends only the UTC day the billing cycle resets ("Unknown" when unset), so this is its start. */
function kiroResetsAt(day: string | undefined): string | undefined {
  if (day === undefined || !KIRO_RESET_DAY.test(day)) return undefined;
  const parsed = DateTime.make(`${day}T00:00:00.000Z`);
  return Option.isSome(parsed) ? DateTime.formatIso(parsed.value) : undefined;
}

const probeFailed = (checkedAt: string) =>
  makeUnavailableUsageLimits({
    checkedAt,
    reason: "probeFailed",
    message: "Could not read Kiro credit usage. Refresh provider status to retry.",
  });

/**
 * Maps Kiro's account usage to one bar per limited allowance, by percent of the
 * plan used. Kiro also reports overage charges in currency; they are not shown,
 * since a bar measures the plan's credits, not spend.
 */
export function kiroUsageLimits(result: unknown, checkedAt: string): ServerProviderUsageLimits {
  const usage = Option.getOrUndefined(decodeKiroAccountUsage(result));
  if (usage === undefined || !usage.success) return probeFailed(checkedAt);
  // Kiro answers an organization-managed plan without usage data.
  if (usage.data == null) {
    return makeUnavailableUsageLimits({
      checkedAt,
      reason: "unsupported",
      message: "Kiro does not report credit usage for this account.",
    });
  }
  const { data } = usage;
  if (data.bonusCredits?.length || data.addOnCredits?.length) {
    return makeUnavailableUsageLimits({
      checkedAt,
      reason: "unsupported",
      message: "Kiro bonus and add-on credit balances are not supported yet.",
    });
  }
  const resetsAt = kiroResetsAt(data.billingCycleReset);
  const windows = new Map<string, ServerProviderUsageWindow>();
  let unlimited = false;
  for (const entry of data.usageBreakdowns ?? []) {
    const breakdown = Option.getOrUndefined(decodeKiroUsageBreakdown(entry));
    const id = breakdown?.resourceType.trim().toLowerCase();
    if (breakdown === undefined || !id || breakdown.used < 0) continue;
    // Kiro clears `hasLimit` on an unlimited allowance (a limit of 999999 or more).
    if (breakdown.hasLimit === false || breakdown.limit <= 0) {
      unlimited = true;
      continue;
    }
    if (windows.has(id)) continue;
    const label = breakdown.displayName?.trim() || breakdown.resourceType.trim();
    const overage =
      id === "credit" && data.overagesEnabled === true && breakdown.used > breakdown.limit;
    windows.set(id, {
      id,
      // Kiro's allowances renew with the billing cycle.
      kind: "monthly",
      label: overage ? `${label} · overage` : label,
      usedPercent: clampPercent((breakdown.used / breakdown.limit) * 100),
      ...(id === "credit" && data.overagesEnabled === true ? { overagesEnabled: true } : {}),
      ...(resetsAt === undefined ? {} : { resetsAt }),
    });
  }
  if (windows.size > 0) return makeUsageLimits({ checkedAt, windows: windows.values() });
  return unlimited
    ? makeUnavailableUsageLimits({
        checkedAt,
        reason: "unsupported",
        message: "Kiro reports no credit limit for this plan.",
      })
    : probeFailed(checkedAt);
}
