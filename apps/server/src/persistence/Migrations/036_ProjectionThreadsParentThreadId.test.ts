import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("036_ProjectionThreadsParentThreadId", (it) => {
  it.effect("adds nullable parent ids and the active hierarchy lookup index", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 35 });
      yield* runMigrations({ toMigrationInclusive: 36 });

      const columns = yield* sql<{
        readonly name: string;
        readonly notnull: number;
      }>`
        PRAGMA table_info(projection_threads)
      `;
      const parentThreadId = columns.find((column) => column.name === "parent_thread_id");
      assert.ok(parentThreadId);
      assert.equal(parentThreadId.notnull, 0);

      const indexes = yield* sql<{ readonly name: string }>`
        PRAGMA index_list(projection_threads)
      `;
      assert.ok(
        indexes.some((index) => index.name === "idx_projection_threads_parent_shell_active"),
      );

      const indexColumns = yield* sql<{ readonly name: string }>`
        PRAGMA index_info('idx_projection_threads_parent_shell_active')
      `;
      assert.deepStrictEqual(
        indexColumns.map((column) => column.name),
        ["parent_thread_id", "deleted_at", "archived_at", "project_id", "created_at", "thread_id"],
      );
    }),
  );
});
