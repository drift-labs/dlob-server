FROM public.ecr.aws/docker/library/node:20 AS builder

RUN npm install -g typescript bun

WORKDIR /app
COPY drift-common /app/drift-common
COPY . .
WORKDIR /app/drift-common/protocol/sdk
RUN bun install
RUN yarn build
WORKDIR /app/drift-common/common-ts
RUN bun install
RUN yarn build
WORKDIR /app
RUN bun install
RUN node esbuild.config.js --minify-whitespace

FROM public.ecr.aws/docker/library/node:20 AS modules
WORKDIR /app
# 'bigint-buffer' native lib for performance
# @triton-one/yellowstone-grpc so .wasm lib included
RUN apt update && apt install -y build-essential python3
RUN npm install -C lib bigint-buffer @triton-one/yellowstone-grpc

FROM gcr.io/distroless/nodejs20-debian12
COPY --from=builder /app/lib/ ./lib/
COPY --from=modules /app/lib/node_modules/ ./lib/node_modules/

ENV NODE_ENV=production
EXPOSE 9464
