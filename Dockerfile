FROM public.ecr.aws/docker/library/node:22 AS builder
RUN npm install -g typescript@5.4.5

WORKDIR /app

COPY . .

WORKDIR /app/drift-common/protocol/sdk
RUN yarn && yarn build

WORKDIR /app/drift-common/common-ts
RUN yarn && yarn build

WORKDIR /app
RUN yarn && yarn build

FROM public.ecr.aws/docker/library/node:22-slim
RUN apt-get update && \
    apt-get install -y python3 make g++ && \
    npm install -C /lib \
        bigint-buffer \
        @triton-one/yellowstone-grpc@5.0.1 \
        helius-laserstream@0.1.8 \
        rpc-websockets@7.5.1
            
COPY --from=builder /app/lib/ ./lib/

ENV NODE_ENV=production
EXPOSE 9464
