import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const columns = yield* sql<{ readonly name: string }>`
    PRAGMA table_info(projection_thread_activities)
  `;

  if (!columns.some((column) => column.name === "event_sequence")) {
    yield* sql`
      ALTER TABLE projection_thread_activities
      ADD COLUMN event_sequence INTEGER
    `;
  }

  yield* sql`
    UPDATE projection_thread_activities AS activity
    SET event_sequence = (
      SELECT event.sequence
      FROM orchestration_events AS event
      WHERE event.event_type = 'thread.activity-appended'
        AND event.stream_id = activity.thread_id
        AND json_extract(event.payload_json, '$.activity.id') = activity.activity_id
      ORDER BY event.sequence DESC
      LIMIT 1
    )
    WHERE activity.event_sequence IS NULL
  `;

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_projection_thread_activities_thread_event_sequence
    ON projection_thread_activities(thread_id, event_sequence)
  `;

  yield* sql`
    WITH structural_approval_states AS (
      SELECT
        activity.thread_id,
        json_extract(activity.payload_json, '$.requestId') AS request_id,
        CASE
          WHEN activity.kind = 'approval.requested' THEN 'pending'
          ELSE 'resolved'
        END AS status,
        CASE
          WHEN activity.kind = 'approval.resolved'
            AND json_extract(activity.payload_json, '$.decision') IN (
              'accept',
              'acceptForSession',
              'decline',
              'cancel'
            )
          THEN json_extract(activity.payload_json, '$.decision')
          ELSE NULL
        END AS decision,
        CASE
          WHEN activity.kind = 'approval.resolved' THEN activity.created_at
          ELSE NULL
        END AS resolved_at,
        activity.event_sequence
      FROM projection_thread_activities AS activity
      WHERE activity.event_sequence IS NOT NULL
        AND activity.kind IN ('approval.requested', 'approval.resolved')
        AND json_extract(activity.payload_json, '$.requestId') IS NOT NULL
    ),
    latest_structural_approval_states AS (
      SELECT
        ranked.thread_id,
        ranked.request_id,
        ranked.status,
        ranked.decision,
        ranked.resolved_at
      FROM (
        SELECT
          structural_approval_states.*,
          ROW_NUMBER() OVER (
            PARTITION BY thread_id, request_id
            ORDER BY event_sequence DESC
          ) AS row_number
        FROM structural_approval_states
      ) AS ranked
      WHERE ranked.row_number = 1
    )
    UPDATE projection_pending_approvals
    SET
      status = (
        SELECT latest.status
        FROM latest_structural_approval_states AS latest
        WHERE latest.thread_id = projection_pending_approvals.thread_id
          AND latest.request_id = projection_pending_approvals.request_id
      ),
      decision = (
        SELECT latest.decision
        FROM latest_structural_approval_states AS latest
        WHERE latest.thread_id = projection_pending_approvals.thread_id
          AND latest.request_id = projection_pending_approvals.request_id
      ),
      resolved_at = (
        SELECT latest.resolved_at
        FROM latest_structural_approval_states AS latest
        WHERE latest.thread_id = projection_pending_approvals.thread_id
          AND latest.request_id = projection_pending_approvals.request_id
      )
    WHERE EXISTS (
      SELECT 1
      FROM latest_structural_approval_states AS latest
      WHERE latest.thread_id = projection_pending_approvals.thread_id
        AND latest.request_id = projection_pending_approvals.request_id
    )
  `;

  yield* sql`
    UPDATE projection_threads
    SET
      pending_approval_count = COALESCE((
        SELECT COUNT(*)
        FROM projection_pending_approvals AS approval
        WHERE approval.thread_id = projection_threads.thread_id
          AND approval.status = 'pending'
      ), 0),
      pending_user_input_count = COALESCE((
        WITH latest_structural_user_input_states AS (
          SELECT
            ranked.kind
          FROM (
            SELECT
              activity.kind,
              ROW_NUMBER() OVER (
                PARTITION BY json_extract(activity.payload_json, '$.requestId')
                ORDER BY activity.event_sequence DESC
              ) AS row_number
            FROM projection_thread_activities AS activity
            WHERE activity.thread_id = projection_threads.thread_id
              AND activity.event_sequence IS NOT NULL
              AND activity.kind IN (
                'user-input.requested',
                'user-input.resolved'
              )
              AND json_extract(activity.payload_json, '$.requestId') IS NOT NULL
          ) AS ranked
          WHERE ranked.row_number = 1
        )
        SELECT COUNT(*)
        FROM latest_structural_user_input_states
        WHERE kind = 'user-input.requested'
      ), 0)
  `;
});
