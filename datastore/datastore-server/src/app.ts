import {
  addTextMessage,
  addTextMessages,
  getMetrics,
  getStats,
  listTextMessages,
  type AddTextMessageInput,
} from "datastore-core";
import { Elysia, t } from "elysia";

export interface BuildAppInput {
  apiKey: string;
}

const MAX_LIMIT = 500;

const parseLimit = (value: string | undefined): number => {
  if (!value) {
    return 50;
  }

  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error("limit must be a positive number");
  }

  return Math.min(Math.floor(parsed), MAX_LIMIT);
};

const requireWriteApiKey = (request: Request, expectedApiKey: string): boolean => {
  const providedApiKey = request.headers.get("x-api-key");
  return providedApiKey === expectedApiKey;
};

const writeMessageBody = t.Object({
  user: t.String({ minLength: 1 }),
  thread: t.String({ minLength: 1 }),
  platform_uuid: t.String({ minLength: 1 }),
  platform: t.String({ minLength: 1 }),
  message: t.String({ minLength: 1 }),
});

const toApiMessage = (row: {
  id: number;
  user: string;
  thread: string;
  platformUuid: string;
  platform: string;
  message: string;
  createdAt: Date;
}) => ({
  id: row.id,
  user: row.user,
  thread: row.thread,
  platform_uuid: row.platformUuid,
  platform: row.platform,
  message: row.message,
  createdAt: row.createdAt,
});

export const buildApp = ({ apiKey }: BuildAppInput): Elysia => {
  return new Elysia()
    .get("/health", () => ({ ok: true }))
    .get("/stats", async ({ set }) => {
      try {
        const stats = await getStats();
        return { ok: true, data: stats };
      } catch (error) {
        set.status = 500;
        return {
          ok: false,
          error: {
            code: "GET_STATS_FAILED",
            message: error instanceof Error ? error.message : String(error),
          },
        };
      }
    })
    .get("/metrics", async ({ set }) => {
      try {
        const metrics = await getMetrics();
        return { ok: true, data: metrics };
      } catch (error) {
        set.status = 500;
        return {
          ok: false,
          error: {
            code: "GET_METRICS_FAILED",
            message: error instanceof Error ? error.message : String(error),
          },
        };
      }
    })
    .get(
      "/messages",
      async ({ query, set }) => {
        try {
          const rows = await listTextMessages(
            parseLimit(query.limit),
            query.platform,
            query.user,
            query.thread,
            query.platform_uuid,
          );
          return { ok: true, data: rows.map(toApiMessage) };
        } catch (error) {
          set.status = 500;
          return {
            ok: false,
            error: {
              code: "LIST_MESSAGES_FAILED",
              message: error instanceof Error ? error.message : String(error),
            },
          };
        }
      },
      {
        query: t.Object({
          limit: t.Optional(t.String()),
          platform: t.Optional(t.String()),
          user: t.Optional(t.String()),
          thread: t.Optional(t.String()),
          platform_uuid: t.Optional(t.String()),
        }),
      },
    )
    .post(
      "/messages",
      async ({ body, request, set }) => {
        if (!requireWriteApiKey(request, apiKey)) {
          set.status = 401;
          return {
            ok: false,
            error: {
              code: "UNAUTHORIZED",
              message: "invalid api key",
            },
          };
        }

        try {
          const row = await addTextMessage({
            user: body.user,
            thread: body.thread,
            platformUuid: body.platform_uuid,
            platform: body.platform,
            message: body.message,
          });
          return { ok: true, data: toApiMessage(row) };
        } catch (error) {
          set.status = 500;
          return {
            ok: false,
            error: {
              code: "CREATE_MESSAGE_FAILED",
              message: error instanceof Error ? error.message : String(error),
            },
          };
        }
      },
      {
        body: writeMessageBody,
      },
    )
    .post(
      "/messages/bulk",
      async ({ body, request, set }) => {
        if (!requireWriteApiKey(request, apiKey)) {
          set.status = 401;
          return {
            ok: false,
            error: {
              code: "UNAUTHORIZED",
              message: "invalid api key",
            },
          };
        }

        const messages = body.messages.map((row) => ({
          user: row.user,
          thread: row.thread,
          platformUuid: row.platform_uuid,
          platform: row.platform,
          message: row.message,
        })) as AddTextMessageInput[];
        try {
          const result = await addTextMessages(messages);
          return {
            ok: true,
            data: {
              inserted: result.count,
            },
          };
        } catch (error) {
          set.status = 500;
          return {
            ok: false,
            error: {
              code: "CREATE_MESSAGES_BULK_FAILED",
              message: error instanceof Error ? error.message : String(error),
            },
          };
        }
      },
      {
        body: t.Object({
          messages: t.Array(writeMessageBody, { minItems: 1, maxItems: 500 }),
        }),
      },
    );
};
