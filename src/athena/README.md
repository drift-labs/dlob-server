# Athena Integration

This module provides connectivity to AWS Athena for querying historical Drift protocol data.

## Setup

### Prerequisites

1. AWS credentials configured with access to Athena
2. Environment variables:
   - `AWS_REGION` (optional, defaults to `us-east-1`)
   - `AWS_ACCESS_KEY_ID` (required)
   - `AWS_SECRET_ACCESS_KEY` (required)
   - `ATHENA_DATABASE` (optional, defaults to `staging-archive`)
   - `ATHENA_OUTPUT_BUCKET` (optional, defaults to `staging-data-ingestion-bucket`)

### Installation

The required AWS SDK packages are already included in `package.json`:
```bash
yarn install
```

## Usage

### Basic Query Example

```typescript
import { Athena } from './athena/client';

const { query } = Athena();

const results = await query(`
  SELECT * FROM eventtype_traderecord 
  WHERE markettype = 'perp' 
  LIMIT 10
`);

console.log(results);
```

### Using the Fill Quality Analytics Repository

```typescript
import { FillQualityAnalyticsRepository } from './athena/repositories/fillQualityAnalytics';

const repository = FillQualityAnalyticsRepository();

// Get taker fill vs oracle basis points for ALL perp markets
// Time range: last 7 days
const nowMs = Date.now();
const weekAgoMs = nowMs - 7 * 24 * 60 * 60 * 1000;

const results = await repository.getTakerFillVsOracleBps(
  weekAgoMs,    // from (unix timestamp in milliseconds)
  nowMs,        // to (unix timestamp in milliseconds)
  9,            // baseDecimals (default: 9)
  60            // smoothingMinutes (default: 60)
);

// Results contain time series data with columns:
// - Time: timestamp
// - MarketIndex: the perp market index
// - TakerBuyBpsFromOracle_1e0: average for buy orders < $1000
// - TakerBuyBpsFromOracle_1e3: average for buy orders $1k-$10k
// - TakerBuyBpsFromOracle_1e4: average for buy orders $10k-$100k
// - TakerBuyBpsFromOracle_1e5: average for buy orders $100k-$1M
// - TakerBuyBpsFromOracle_1e6: average for buy orders > $1M
// - (Similar columns for sell orders)
// - Zero: baseline (always 0)

// Data is partitioned by MarketIndex, so you can filter/group by market
const solPerpData = results.filter(r => r.MarketIndex === '0');
console.log(solPerpData);
```

### Batch Queries

```typescript
import { Athena } from './athena/client';

const { batchQuery } = Athena();

const { results, errors } = await batchQuery({
  queries: [
    { query: 'SELECT COUNT(*) FROM eventtype_traderecord' },
    { query: 'SELECT COUNT(*) FROM eventtype_orderrecord' },
  ]
});

console.log('Results:', results);
console.log('Errors:', errors);
```

### Custom Database/Bucket

```typescript
import { Athena } from './athena/client';

const { query } = Athena({
  overrideDatabaseName: 'my_custom_db',
  overrideBucketName: 'my-custom-bucket'
});
```

## Architecture

### Client (`client.ts`)
- Handles Athena query execution
- Manages query lifecycle (start, poll, retrieve results)
- Implements rate limiting via Bottleneck
- Provides batch query support

### Utilities (`utils.ts`)
- `getTimePartition`: Generate time partition filters for queries
- `getTimeRangeAndPartitions`: Generate time range and partition filters

### Repositories
- **Fill Quality Analytics** (`repositories/fillQualityAnalytics.ts`): Analyzes taker fill quality vs oracle prices, bucketed by order size cohorts

## Query Best Practices

1. **Always use partition filters** to minimize data scanned:
   ```sql
   WHERE year = '2024' AND month = '01' AND day = '15'
   ```

2. **Use the utility functions** for generating time-based filters:
   ```typescript
   import { getTimeRangeAndPartitions } from './athena/utils';
   
   const query = `
     ${getTimeRangeAndPartitions(from, to)}
     SELECT * FROM eventtype_traderecord
     JOIN valid_partitions vp ON ...
   `;
   ```

3. **Monitor costs**: Each query logs bytes scanned. Athena charges per TB scanned.

4. **Use batch queries** when running multiple independent queries to improve throughput.

## Error Handling

The client automatically retries failed queries up to 3 times with exponential backoff. Queries timeout after 5 minutes (300s) by default.

```typescript
try {
  const results = await query(queryString);
} catch (error) {
  console.error('Query failed:', error.message);
  // Handle error appropriately
}
```

## Adding New Repositories

To add a new analytics repository:

1. Create a new file in `repositories/` (e.g., `myAnalytics.ts`)
2. Define TypeScript interfaces for your results
3. Create a repository function that uses the Athena client
4. Export from `index.ts`

Example:

```typescript
// repositories/myAnalytics.ts
import { Athena } from '../client';

export interface MyResult {
  field1: string;
  field2: string;
}

export const MyAnalyticsRepository = () => {
  const { query } = Athena();

  const getMyData = async (from: number, to: number): Promise<MyResult[]> => {
    const queryString = `
      SELECT field1, field2 
      FROM my_table 
      WHERE ts BETWEEN ${from} AND ${to}
    `;
    
    const results = await query(queryString);
    
    return results.map((r) => ({
      field1: r.field1 || '',
      field2: r.field2 || '',
    }));
  };

  return { getMyData };
};
```

```typescript
// index.ts
export * from './repositories/myAnalytics';
```

## Integration with DLOBPublisher

The DLOBPublisher (`src/publishers/dlobPublisher.ts`) includes automatic fetching and caching of fill quality analytics data to Redis.

### Configuration

Enable fill quality analytics via environment variables:

```bash
# Enable the feature (default: false)
ENABLE_FILL_QUALITY_ANALYTICS=true

# How often to fetch and update analytics (default: 300000 = 5 minutes)
FILL_QUALITY_ANALYTICS_INTERVAL=300000

# Historical data lookback window (default: 86400000 = 24 hours)
FILL_QUALITY_ANALYTICS_LOOKBACK_MS=86400000

# Rolling average smoothing window in minutes (default: 60)
FILL_QUALITY_ANALYTICS_SMOOTHING_MINUTES=60
```

### How it Works

1. **On Startup**: Immediately fetches fill quality analytics for all perp markets
2. **Periodic Updates**: Re-fetches data every `FILL_QUALITY_ANALYTICS_INTERVAL` milliseconds
3. **Redis Storage**: Stores results in Redis with the following keys:
   - `taker_fill_vs_oracle_bps:market:{marketIndex}` - Individual market data
   - `taker_fill_vs_oracle_bps:summary` - Summary with metadata

### Redis Data Format

Per-market keys contain:
```json
{
  "time": "2024-01-15 10:30:00.000",
  "marketIndex": "0",
  "takerBuyBpsFromOracle": {
    "all": "5.23",
    "1e0": "8.45",
    "1e3": "6.12",
    "1e4": "4.89",
    "1e5": "3.56",
    "1e6": "2.34"
  },
  "takerSellBpsFromOracle": {
    "all": "-4.87",
    "1e0": "-7.23",
    "1e3": "-5.45",
    "1e4": "-4.12",
    "1e5": "-3.01",
    "1e6": "-2.15"
  },
  "updatedAt": "2024-01-15T10:35:00.000Z"
}
```

Summary key contains:
```json
{
  "markets": ["0", "1", "2"],
  "lastUpdated": "2024-01-15T10:35:00.000Z",
  "lookbackMs": 86400000,
  "smoothingMinutes": 60
}
```

### Accessing the Data

Downstream services can read the cached analytics from Redis without querying Athena directly:

```typescript
import { RedisClient } from '@drift/common/clients';

const redis = new RedisClient();
await redis.connect();

// Get summary
const summary = await redis.get('taker_fill_vs_oracle_bps:summary');
console.log(JSON.parse(summary));

// Get data for SOL-PERP (market index 0)
const solData = await redis.get('taker_fill_vs_oracle_bps:market:0');
console.log(JSON.parse(solData));
```

This architecture allows multiple services to access expensive analytics queries without hitting Athena repeatedly.

## Testing

### Test Redis Polling

To verify that fill quality analytics data is being written to and updated in Redis:

```bash
# Start the DLOBPublisher with fill quality analytics enabled
ENABLE_FILL_QUALITY_ANALYTICS=true yarn dlob-publish

# In another terminal, run the Redis polling test
yarn test:fill-quality-redis
```

The polling test script will:
- Connect to Redis
- Poll every 10 seconds for markets 0, 1, 2 (SOL, BTC, ETH)
- Display the summary and per-market data
- Track when data is updated (shows 🆕 NEW UPDATE when timestamps change)
- Validate data structure integrity
- Show data age and warn if stale (>10 minutes old)

**Example output:**
```
========== Polling iteration 1 ==========

📋 Summary:
   Markets available: 0, 1, 2
   Last updated: 2024-01-15T10:35:00.000Z
   Lookback: 86400000ms (24h)
   Smoothing: 60 minutes

📊 Market Data:
   🆕 NEW UPDATE - SOL-PERP (Market 0)
      Data timestamp: 2024-01-15 10:30:00.000
      Updated at: 2024-01-15T10:35:00.000Z
      Age: 15s ago
      Taker Buy (all): 5.23 bps
      Taker Sell (all): -4.87 bps
      Large orders (>$1M): Buy=2.34bps, Sell=-2.15bps

   🆕 NEW UPDATE - BTC-PERP (Market 1)
      Data timestamp: 2024-01-15 10:30:00.000
      Updated at: 2024-01-15T10:35:00.000Z
      Age: 15s ago
      Taker Buy (all): 4.12 bps
      Taker Sell (all): -3.89 bps
      Large orders (>$1M): Buy=1.89bps, Sell=-1.76bps

📈 Update Tracking:
   ✅ All 3 markets have data

🔍 Validation:
   ✅ All data structures are valid
```

The script will continue polling until you press `Ctrl+C`.

