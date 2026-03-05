import adze from "adze";

import { prisma } from "./db";

export interface AddTextMessageInput {
  user: string;
  thread: string;
  platformUuid: string;
  platform: string;
  message: string;
}

export async function addTextMessage(input: AddTextMessageInput) {
  adze.debug("Creating text message", {
    user: input.user,
    thread: input.thread,
    platform: input.platform,
    platformUuid: input.platformUuid,
    messageLength: input.message.length,
  });

  try {
    const row = await prisma.textMessage.create({
      data: {
        user: input.user,
        thread: input.thread,
        platformUuid: input.platformUuid,
        platform: input.platform,
        message: input.message,
      },
    });
    adze.info("Created text message", { id: row.id, platformUuid: row.platformUuid });
    return row;
  } catch (error) {
    adze.error("Failed to create text message", {
      platformUuid: input.platformUuid,
      error: error instanceof Error ? error.message : String(error),
    });
    throw error;
  }
}

export async function addTextMessages(input: AddTextMessageInput[]) {
  if (input.length === 0) {
    adze.warn("Skipping bulk insert because batch is empty");
    return { count: 0 };
  }

  adze.info("Creating text messages in bulk", { batchSize: input.length });

  try {
    const result = await prisma.textMessage.createMany({
      data: input.map((row) => ({
        user: row.user,
        thread: row.thread,
        platformUuid: row.platformUuid,
        platform: row.platform,
        message: row.message,
      })),
    });
    adze.info("Bulk insert complete", { inserted: result.count });
    return result;
  } catch (error) {
    adze.error("Bulk insert failed", {
      batchSize: input.length,
      error: error instanceof Error ? error.message : String(error),
    });
    throw error;
  }
}

export async function listTextMessages(
  limit = 50,
  platform?: string,
  user?: string,
  thread?: string,
  platformUuid?: string,
) {
  adze.debug("Listing text messages", { limit, platform, user, thread, platformUuid });

  try {
    const rows = await prisma.textMessage.findMany({
      where: {
        platform,
        user,
        thread,
        platformUuid,
      },
      orderBy: { createdAt: "desc" },
      take: limit,
    });
    adze.debug("Listed text messages", { count: rows.length });
    return rows;
  } catch (error) {
    adze.error("Failed to list text messages", {
      limit,
      platform,
      user,
      thread,
      platformUuid,
      error: error instanceof Error ? error.message : String(error),
    });
    throw error;
  }
}

export async function getStats() {
  adze.debug("Computing datastore stats");
  const textMessageCount = await prisma.textMessage.count();
  adze.info("Computed datastore stats", { textMessageCount });

  return { textMessageCount };
}

interface SqliteTableNameRow {
  name: string;
}

interface SqliteTableCountRow {
  count: number | bigint;
}

export async function getMetrics() {
  adze.debug("Computing datastore metrics");

  const tables = await prisma.$queryRaw<SqliteTableNameRow[]>`
    SELECT name
    FROM sqlite_master
    WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
    ORDER BY name ASC
  `;

  const tableRowCountEntries = await Promise.all(
    tables.map(async ({ name }) => {
      const escapedTableName = name.replace(/"/g, "\"\"");
      const countResult = await prisma.$queryRawUnsafe<SqliteTableCountRow[]>(
        `SELECT COUNT(*) as count FROM "${escapedTableName}"`,
      );

      const rawCount = countResult[0]?.count ?? 0;
      return [name, Number(rawCount)] as const;
    }),
  );

  const tableRowCounts = Object.fromEntries(tableRowCountEntries);
  const totalRows = tableRowCountEntries.reduce((sum, [, count]) => sum + count, 0);
  const metrics = {
    tableCount: tables.length,
    totalRows,
    tableRowCounts,
  };

  adze.info("Computed datastore metrics", metrics);
  return metrics;
}

export async function closeDatastore() {
  adze.info("Closing datastore Prisma client");
  await prisma.$disconnect();
}
