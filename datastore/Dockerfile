FROM oven/bun:1

WORKDIR /app

COPY package.json bun.lock ./
COPY datastore-core/package.json datastore-core/package.json
COPY datastore-cli/package.json datastore-cli/package.json
COPY datastore-server/package.json datastore-server/package.json

RUN bun install --frozen-lockfile

COPY . .

CMD ["sh", "-c", "bunx --bun prisma@6.19.2 migrate deploy --schema datastore-core/prisma/schema.prisma && bun run server"]
