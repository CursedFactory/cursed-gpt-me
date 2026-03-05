import { PrismaLibSQL } from "@prisma/adapter-libsql";
import adze from "adze";
import { join } from "node:path";
import { PrismaClient } from "../prisma/generated/client";

const defaultDatabaseUrl = `file:${join(import.meta.dir, "..", "..", "..", "data", "datastore.db")}`;
const configuredDatabaseUrl = process.env.DATABASE_URL || defaultDatabaseUrl;
const usingEnvironmentDatabaseUrl = Boolean(process.env.DATABASE_URL);

adze.info("Initializing datastore Prisma client", {
  usingEnvironmentDatabaseUrl,
  databaseUrl: configuredDatabaseUrl,
});

const adapter = new PrismaLibSQL({
  url: configuredDatabaseUrl,
});

export const prisma = new PrismaClient({ adapter });
