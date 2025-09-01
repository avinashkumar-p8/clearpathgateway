import { Spanner } from '@google-cloud/spanner';

export function spannerClient(projectId: string, instanceId: string, databaseId: string) {
  const spanner = new Spanner({ projectId });
  const instance = spanner.instance(instanceId);
  const database = instance.database(databaseId);
  return {
    query: async (sql: string, params?: Record<string, unknown>) => {
      const [rows] = await database.run({ sql, params });
      return rows.map(r => r.toJSON());
    },
    close: async () => {
      try { await database.close(); } finally { await spanner.close(); }
    }
  };
}
