FROM public.ecr.aws/docker/library/node:20 AS builder

RUN npm install -g typescript bun husky
WORKDIR /app

COPY . .
RUN bun install
WORKDIR /app/drift-common/protocol/sdk
RUN yarn build
WORKDIR /app/drift-common/common-ts
RUN yarn build
WORKDIR /app
RUN bun esbuild.config.js

FROM public.ecr.aws/docker/library/node:20-alpine
COPY --from=builder /app/lib/ ./lib/
RUN apk add --virtual .build python3 g++ make &&\
    npm install -C lib bigint-buffer @triton-one/yellowstone-grpc@1.3.0 &&\
    apk del .build &&\
    rm -rf /root/.cache/ /root/.npm /usr/local/lib/node_modules

ENV NODE_ENV=production
EXPOSE 9464
