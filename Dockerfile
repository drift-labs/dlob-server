FROM node:20.18.1 AS builder

WORKDIR /app

# Copy package files first to leverage cache
COPY package.json yarn.lock ./
COPY drift-common/protocol/sdk/package.json ./drift-common/protocol/sdk/
COPY drift-common/common-ts/package.json ./drift-common/common-ts/

RUN npm install -g bun typescript husky

WORKDIR /app/drift-common/protocol/sdk
COPY drift-common/protocol/sdk/ .
RUN bun install && bun run build

WORKDIR /app/drift-common/common-ts
COPY drift-common/common-ts/ .
RUN bun install && bun run build

WORKDIR /app
COPY . .
# no '--production' for esbuild
RUN bun install && bun esbuild.config.js

FROM node:20.18.1-alpine
# 'bigint-buffer' native lib for performance
# @triton-one/yellowstone-grpc so .wasm lib included
RUN apk add python3 make g++ --virtual .build &&\
    npm install -C /lib bigint-buffer @triton-one/yellowstone-grpc &&\
    apk del .build
COPY --from=builder /app/lib/ ./lib/

ENV NODE_ENV=production
EXPOSE 9464

CMD ["node", "./lib/index.js"]