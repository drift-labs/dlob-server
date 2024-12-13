FROM node:20.18.1 AS builder

WORKDIR /app

# Copy package files first to leverage cache
COPY package.json yarn.lock ./
COPY drift-common/protocol/sdk/package.json ./drift-common/protocol/sdk/
COPY drift-common/common-ts/package.json ./drift-common/common-ts/

RUN npm install -g bun typescript @vercel/ncc

ENV NODE_ENV=production

WORKDIR /app/drift-common/protocol/sdk
COPY drift-common/protocol/sdk/ .
RUN bun install && bun run build

WORKDIR /app/drift-common/common-ts
COPY drift-common/common-ts/ .
RUN bun install && bun run build

WORKDIR /app
COPY . .
RUN bun install
# This builds the .wasm deps for gRPC package
# maybe there's a simpler way
RUN ncc build src/index.ts --out lib 
RUN bun esbuild.config.js # no clean

FROM node:20.18.1-alpine
COPY --from=builder /app/lib/ ./lib/

ENV NODE_ENV=production
EXPOSE 9464

CMD ["node", "./lib/index.js"]