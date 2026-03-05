import { closeDatastore } from "datastore-core";

import { buildApp } from "./app";

const host = process.env.DATASTORE_SERVER_HOST || "127.0.0.1";
const rawPort = process.env.DATASTORE_SERVER_PORT || "4040";
const apiKey = process.env.DATASTORE_API_KEY;

if (!apiKey) {
  throw new Error("DATASTORE_API_KEY must be set");
}

const port = Number(rawPort);
if (!Number.isFinite(port) || port <= 0) {
  throw new Error(`DATASTORE_SERVER_PORT must be a positive number (received: ${rawPort})`);
}

const app = buildApp({ apiKey });
const server = app.listen({
  hostname: host,
  port,
});

console.log(`datastore-server listening on http://${host}:${port}`);

let shuttingDown = false;

const shutdown = async (): Promise<void> => {
  if (shuttingDown) {
    return;
  }

  shuttingDown = true;
  server.stop();
  await closeDatastore();
  process.exit(0);
};

process.on("SIGINT", () => {
  void shutdown();
});

process.on("SIGTERM", () => {
  void shutdown();
});
