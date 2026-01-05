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

FROM public.ecr.aws/docker/library/node:22-alpine
RUN apk add python3 make g++ --virtual .build &&\
    npm install -C /lib bigint-buffer @triton-one/yellowstone-grpc@1.3.0 helius-laserstream@0.1.8 rpc-websockets@7.5.1 &&\
    apk del .build
    
COPY --from=builder /app/lib/ ./lib/

ENV NODE_ENV=production
EXPOSE 9464
