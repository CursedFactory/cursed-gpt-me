/*
  Warnings:

  - Added the required column `thread` to the `text_messages` table without a default value. This is not possible if the table is not empty.

*/
-- RedefineTables
PRAGMA defer_foreign_keys=ON;
PRAGMA foreign_keys=OFF;
CREATE TABLE "new_text_messages" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "user" TEXT NOT NULL,
    "thread" TEXT NOT NULL,
    "platform" TEXT NOT NULL,
    "message" TEXT NOT NULL,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO "new_text_messages" ("createdAt", "id", "message", "platform", "user") SELECT "createdAt", "id", "message", "platform", "user" FROM "text_messages";
DROP TABLE "text_messages";
ALTER TABLE "new_text_messages" RENAME TO "text_messages";
PRAGMA foreign_keys=ON;
PRAGMA defer_foreign_keys=OFF;
