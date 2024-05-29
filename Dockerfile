FROM public.ecr.aws/bitnami/node:18
RUN apt-get install git
ENV NODE_ENV=production
RUN npm install -g yarn
RUN npm install -g typescript

WORKDIR /app
COPY drift-common /app/drift-common
COPY . .
WORKDIR /app/drift-common/protocol/sdk
RUN yarn
RUN yarn build
WORKDIR /app/drift-common/common-ts
RUN yarn
RUN yarn build
WORKDIR /app
RUN yarn
RUN yarn build

EXPOSE 9464