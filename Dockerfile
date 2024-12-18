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

FROM public.ecr.aws/docker/library/node:20-alpine
# 'bigint-buffer' native lib for performance
# @triton-one/yellowstone-grpc so .wasm lib included
RUN apk add --virtual .build python3 g++ make &&\
    npm install -C /lib bigint-buffer @triton-one/yellowstone-grpc &&\
    apk del .build &&\
    rm -rf ./root/.cache/
COPY --from=builder /app/lib/ ./lib/

ENV NODE_ENV=production
EXPOSE 9464
