import {
  addTextMessage,
  closeDatastore,
  getMetrics,
  getStats,
  listTextMessages,
} from "datastore-core";

function printHelp() {
  console.log(`datastore-cli

Usage:
  bun run src/index.ts seed
  bun run src/index.ts add-message <user> <thread> <platform_uuid> "Message content" [platform]
  bun run src/index.ts list-messages [limit] [platform] [user] [thread] [platform_uuid]
  bun run src/index.ts stats
  bun run src/index.ts metrics
`);
}

const [, , command, ...args] = Bun.argv;

try {
  switch (command) {
    case "seed": {
      const row = await addTextMessage({
        user: "alice",
        thread: "channel:seed",
        platformUuid: "seed-1",
        platform: "DISCORD",
        message: "Please extract key tasks from this conversation.",
      });
      const stats = await getStats();
      console.log(`Seeded message #${row.id}.`);
      console.log(`Text messages: ${stats.textMessageCount}`);
      break;
    }

    case "add-message": {
      const [user, thread, platformUuid, message, platform] = args;
      if (!user || !thread || !platformUuid || !message) {
        throw new Error(
          "Usage: add-message <user> <thread> <platform_uuid> \"message\" [platform]",
        );
      }
      const row = await addTextMessage({
        user,
        thread,
        platformUuid,
        platform: platform || "DISCORD",
        message,
      });
      console.log(`Created text message #${row.id}`);
      break;
    }

    case "list-messages": {
      const limit = args[0] ? Number(args[0]) : 20;
      const platform = args[1] || undefined;
      const user = args[2] || undefined;
      const thread = args[3] || undefined;
      const platformUuid = args[4] || undefined;
      const rows = await listTextMessages(limit, platform, user, thread, platformUuid);
      for (const row of rows) {
        console.log(
          `#${row.id} [${row.platform}] (${row.thread}) [${row.platformUuid}] ${row.user}: ${row.message}`,
        );
      }
      break;
    }

    case "stats": {
      const stats = await getStats();
      console.log(`Text messages: ${stats.textMessageCount}`);
      break;
    }

    case "metrics": {
      const metrics = await getMetrics();
      console.log(`Tables: ${metrics.tableCount}`);
      console.log(`Total rows: ${metrics.totalRows}`);
      for (const [tableName, rowCount] of Object.entries(metrics.tableRowCounts)) {
        console.log(`${tableName}: ${rowCount}`);
      }
      break;
    }

    default:
      printHelp();
      break;
  }
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Error: ${message}`);
  process.exitCode = 1;
} finally {
  await closeDatastore();
}
