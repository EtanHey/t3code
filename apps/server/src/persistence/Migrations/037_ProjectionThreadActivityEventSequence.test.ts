import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("037_ProjectionThreadActivityEventSequence", (it) => {
  it.effect("backfills authoritative append sequence without changing provider sequence", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 36 });

      yield* sql`
        INSERT INTO orchestration_events (
          event_id,
          aggregate_kind,
          stream_id,
          stream_version,
          event_type,
          occurred_at,
          command_id,
          causation_event_id,
          correlation_id,
          actor_kind,
          payload_json,
          metadata_json
        )
        VALUES
          (
            'event-activity-first',
            'thread',
            'thread-event-sequence',
            1,
            'thread.activity-appended',
            '2026-07-31T00:00:01.000Z',
            'cmd-activity-first',
            NULL,
            'cmd-activity-first',
            'system',
            '{"threadId":"thread-event-sequence","activity":{"id":"activity-matched"}}',
            '{}'
          ),
          (
            'event-activity-latest',
            'thread',
            'thread-event-sequence',
            2,
            'thread.activity-appended',
            '2026-07-31T00:00:02.000Z',
            'cmd-activity-latest',
            NULL,
            'cmd-activity-latest',
            'system',
            '{"threadId":"thread-event-sequence","activity":{"id":"activity-matched"}}',
            '{}'
          ),
          (
            'event-approval-requested',
            'thread',
            'thread-event-sequence',
            3,
            'thread.activity-appended',
            '2026-07-31T00:00:03.000Z',
            'cmd-approval-requested',
            NULL,
            'cmd-approval-requested',
            'system',
            '{"threadId":"thread-event-sequence","activity":{"id":"activity-approval-requested"}}',
            '{}'
          ),
          (
            'event-approval-requested-resolved',
            'thread',
            'thread-event-sequence',
            4,
            'thread.activity-appended',
            '2026-07-31T00:00:04.000Z',
            'cmd-approval-requested-resolved',
            NULL,
            'cmd-approval-requested-resolved',
            'system',
            '{"threadId":"thread-event-sequence","activity":{"id":"activity-approval-requested-resolved"}}',
            '{}'
          ),
          (
            'event-approval-response',
            'thread',
            'thread-event-sequence',
            5,
            'thread.approval-response-requested',
            '2026-07-31T00:00:05.000Z',
            'cmd-approval-response',
            NULL,
            'cmd-approval-response',
            'user',
            '{"threadId":"thread-event-sequence","requestId":"approval-resolved","decision":"accept","createdAt":"2026-07-31T00:00:05.000Z"}',
            '{}'
          )
      `;

      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          branch,
          worktree_path,
          latest_turn_id,
          created_at,
          updated_at,
          archived_at,
          latest_user_message_at,
          pending_approval_count,
          pending_user_input_count,
          has_actionable_proposed_plan,
          deleted_at
        )
        VALUES (
          'thread-event-sequence',
          'project-event-sequence',
          'Event sequence migration',
          '{"provider":"codex","model":"gpt-5-codex"}',
          'approval-required',
          'default',
          NULL,
          NULL,
          NULL,
          '2026-07-31T00:00:00.000Z',
          '2026-07-31T00:00:03.000Z',
          NULL,
          NULL,
          0,
          1,
          0,
          NULL
        )
      `;

      yield* sql`
        INSERT INTO projection_thread_activities (
          activity_id,
          thread_id,
          turn_id,
          tone,
          kind,
          summary,
          payload_json,
          sequence,
          created_at
        )
        VALUES
          (
            'activity-matched',
            'thread-event-sequence',
            NULL,
            'info',
            'user-input.resolved',
            'Resolved',
            '{"requestId":"request-matched"}',
            77,
            '2026-07-31T00:00:00.000Z'
          ),
          (
            'activity-unmatched',
            'thread-event-sequence',
            NULL,
            'info',
            'user-input.requested',
            'Requested',
            '{"requestId":"request-unmatched"}',
            NULL,
            '2026-07-31T00:00:03.000Z'
          ),
          (
            'activity-approval-requested',
            'thread-event-sequence',
            NULL,
            'approval',
            'approval.requested',
            'Approval requested',
            '{"requestId":"approval-matched","requestKind":"command"}',
            NULL,
            '2026-07-31T00:00:04.000Z'
          ),
          (
            'activity-approval-requested-resolved',
            'thread-event-sequence',
            NULL,
            'approval',
            'approval.requested',
            'Approval requested then resolved',
            '{"requestId":"approval-resolved","requestKind":"command"}',
            NULL,
            '2026-07-31T00:00:06.000Z'
          )
      `;

      yield* sql`
        INSERT INTO projection_pending_approvals (
          request_id,
          thread_id,
          turn_id,
          status,
          decision,
          created_at,
          resolved_at
        )
        VALUES
          (
            'approval-matched',
            'thread-event-sequence',
            NULL,
            'resolved',
            NULL,
            '2026-07-31T00:00:04.000Z',
            '2026-07-31T00:00:05.000Z'
          ),
          (
            'approval-resolved',
            'thread-event-sequence',
            NULL,
            'pending',
            NULL,
            '2026-07-31T00:00:06.000Z',
            NULL
          )
      `;

      const executed = yield* runMigrations({ toMigrationInclusive: 37 });
      assert.deepStrictEqual(
        executed.map(([id]) => id),
        [37],
      );

      const rows = yield* sql<{
        readonly activityId: string;
        readonly providerSequence: number | null;
        readonly eventSequence: number | null;
      }>`
        SELECT
          activity_id AS "activityId",
          sequence AS "providerSequence",
          event_sequence AS "eventSequence"
        FROM projection_thread_activities
        ORDER BY activity_id
      `;
      assert.deepStrictEqual(rows, [
        {
          activityId: "activity-approval-requested",
          providerSequence: null,
          eventSequence: 3,
        },
        {
          activityId: "activity-approval-requested-resolved",
          providerSequence: null,
          eventSequence: 4,
        },
        {
          activityId: "activity-matched",
          providerSequence: 77,
          eventSequence: 2,
        },
        {
          activityId: "activity-unmatched",
          providerSequence: null,
          eventSequence: null,
        },
      ]);

      const threadRows = yield* sql<{
        readonly pendingApprovalCount: number;
        readonly pendingUserInputCount: number;
      }>`
        SELECT
          pending_approval_count AS "pendingApprovalCount",
          pending_user_input_count AS "pendingUserInputCount"
        FROM projection_threads
        WHERE thread_id = 'thread-event-sequence'
      `;
      assert.deepStrictEqual(threadRows, [
        {
          pendingApprovalCount: 2,
          pendingUserInputCount: 0,
        },
      ]);

      const approvalRows = yield* sql<{
        readonly decision: string | null;
        readonly resolvedAt: string | null;
        readonly status: string;
      }>`
        SELECT
          status,
          decision,
          resolved_at AS "resolvedAt"
        FROM projection_pending_approvals
        WHERE request_id IN ('approval-matched', 'approval-resolved')
        ORDER BY request_id
      `;
      assert.deepStrictEqual(approvalRows, [
        {
          status: "pending",
          decision: null,
          resolvedAt: null,
        },
        {
          status: "pending",
          decision: null,
          resolvedAt: null,
        },
      ]);
    }),
  );
});
