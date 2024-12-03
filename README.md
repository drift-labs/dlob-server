<div align="center">
  <img height="120x" src="https://uploads-ssl.webflow.com/611580035ad59b20437eb024/616f97a42f5637c4517d0193_Logo%20(1)%20(1).png" />

  <h1 style="margin-top:20px;">DLOB Server for Drift Protocol v2</h1>

  <p>
    <a href="https://docs.drift.trade/tutorial-keeper-bots"><img alt="Docs" src="https://img.shields.io/badge/docs-tutorials-blueviolet" /></a>
    <a href="https://discord.com/channels/849494028176588802/878700556904980500"><img alt="Discord Chat" src="https://img.shields.io/discord/889577356681945098?color=blueviolet" /></a>
    <a href="https://opensource.org/licenses/Apache-2.0"><img alt="License" src="https://img.shields.io/github/license/project-serum/anchor?color=blueviolet" /></a>
  </p>
</div>

# DLOB Server

This is the backend server that provides a REST API for the drift [DLOB](https://docs.drift.trade/about-v2/decentralized-orderbook).

# Run the server

## Setup

The build dependencies
```
git submodule update --init
bash build_all.sh
```

First set the necessary environment variables:
```
cp .env.example .env
```

## Environment Configuration

To properly configure the DLOB server, set the following environment variables in your `.env` file:

| Variable                  | Description                                               | Example Value                                                      |
|---------------------------|-----------------------------------------------------------|--------------------------------------------------------------------|
| `ENDPOINT`                | The Solana RPC node http endpoint.                        | `https://your-private-rpc-node.com`                                |
| `WS_ENDPOINT`             | The Solana RPC node websocket endpoint.                   | `wss://your-private-rpc-node.com`                                  |
| `USE_WEBSOCKET`           | Flag to enable WebSocket connection.                      | `true`                                                             |
| `USE_ORDER_SUBSCRIBER`    | Flag to enable order subscriber DLOB source.              | `true`                                                             |
| `DISABLE_GPA_REFRESH`     | Flag to disable periodic refresh using `getProgramAccounts`. | `true`                                                          |
| `ENV`                     | The network environment the server is connecting to.      | `mainnet-beta`                                                     |
| `PORT`                    | The port number the HTTP server listens on.               | `6969`                                                             |
| `METRICS_PORT`            | The port number for Prometheus metrics.                   | `9465`                                                             |
| `PRIVATE_KEY`             | Path to the Solana private key file.                      | `/path/to/keypair.json`                                            |
| `RATE_LIMIT_CALLS_PER_SECOND` | Maximum number of API calls per second.               | `100`                                                              |
| `PERP_MARKETS_TO_LOAD`    | Number of perpetual markets to load at startup.           | `0`                                                                |
| `SPOT_MARKETS_TO_LOAD`    | Number of spot markets to load at startup.                | `5`                                                                |
| `ELASTICACHE_HOST`        | (for websocket server) Redis host endpoint.               | `localhost`                                                        |
| `ELASTICACHE_PORT`        | (for websocket server) Redis port.                        | `6379`                                                             |
| `REDIS_CLIENT`            | (for websocket server) Redis client type (DLOB/DLOB_HELIUS).| `DLOB`                                                           |
| `WS_PORT`                 | (for websocket server) The port to run the websocket server on.|  `3000`                                                       |


Note: multiple Redis hosts can be provided by providing a comma separated string.


## HTTP mode

The HTTP server as documented [here](https://drift-labs.github.io/v2-teacher/?python#orderbook-trades-dlob-server) can be run with, and by default accessible on `http://127.0.0.1:6969`:
```
yarn run dev
```

## Websocket mode

The websocket server has 2 components, the `dlob-publisher` that takes frequent snapshots of the DLOB and publishes them to Redis, and `ws-manager` listens for new connections and sends the latest DLOB to ws clients, the two components communicate through Redis pub-sub.

To run the websocket server, a Redis cache is required, and the following environment variables must be set:
* `REDIS_HOSTS`
* `REDIS_PASSWORDS`
* `REDIS_PORTS`

In the first terminal, start the redis cluster:
```
bash redisCluster.sh start
bash redisCluster.sh create
```

In second terminal, run:
```
yarn run dlob-publisher
```

In a third terminal, run:
```
yarn run ws-manager
```

Then connect to the ws server at ws://127.0.0.1:3000

When you're done, stop the redis cluster:
```
bash redisCluster.sh stop
```

# Run the example client

Documentation for connecting to the dlob server are available [here](https://drift-labs.github.io/v2-teacher/?python#orderbook-trades-dlob-server)

TODO: complete client examples.