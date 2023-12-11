import { ObservableResult } from '@opentelemetry/api';
import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import { logger } from '../utils/logger';
import {
	ExplicitBucketHistogramAggregation,
	InstrumentType,
	MeterProvider,
	View,
} from '@opentelemetry/sdk-metrics-base';
import {
	ORDERBOOK_UPDATE_INTERVAL,
	commitHash,
	driftEnv,
	endpoint,
	wsEndpoint,
} from '..';
import { SlotSource } from '@drift-labs/sdk';

/**
 * Creates {count} buckets of size {increment} starting from {start}. Each bucket stores the count of values within its "size".
 * @param start
 * @param increment
 * @param count
 * @returns
 */
const createHistogramBuckets = (
	start: number,
	increment: number,
	count: number
) => {
	return new ExplicitBucketHistogramAggregation(
		Array.from(new Array(count), (_, i) => start + i * increment)
	);
};

enum METRIC_TYPES {
	runtime_specs = 'runtime_specs',
	endpoint_response_times_histogram = 'endpoint_response_times_histogram',
	endpoint_response_status = 'endpoint_response_status',
	gpa_fetch_duration = 'gpa_fetch_duration',
	last_ws_message_received_ts = 'last_ws_message_received_ts',
	account_updates_count = 'account_updates_count',
	cache_hit_count = 'cache_hit_count',
	cache_miss_count = 'cache_miss_count',
	current_system_ts = 'current_system_ts',
	health_status = 'health_status',
}

export enum HEALTH_STATUS {
	Ok = 0,
	StaleBulkAccountLoader,
	UnhealthySlotSubscriber,
	LivenessTesting,
}

const metricsPort =
	parseInt(process.env.METRICS_PORT) || PrometheusExporter.DEFAULT_OPTIONS.port;
const { endpoint: defaultEndpoint } = PrometheusExporter.DEFAULT_OPTIONS;
const exporter = new PrometheusExporter(
	{
		port: metricsPort,
		endpoint: defaultEndpoint,
	},
	() => {
		logger.info(
			`prometheus scrape endpoint started: http://localhost:${metricsPort}${defaultEndpoint}`
		);
	}
);
const meterName = 'dlob-meter';
const meterProvider = new MeterProvider({
	views: [
		new View({
			instrumentName: METRIC_TYPES.endpoint_response_times_histogram,
			instrumentType: InstrumentType.HISTOGRAM,
			meterName,
			aggregation: createHistogramBuckets(0, 20, 30),
		}),
		new View({
			instrumentName: METRIC_TYPES.gpa_fetch_duration,
			instrumentType: InstrumentType.HISTOGRAM,
			meterName,
			aggregation: createHistogramBuckets(0, 500, 20),
		}),
	],
});
meterProvider.addMetricReader(exporter);
const meter = meterProvider.getMeter(meterName);

const runtimeSpecsGauge = meter.createObservableGauge(
	METRIC_TYPES.runtime_specs,
	{
		description: 'Runtime sepcification of this program',
	}
);
const bootTimeMs = Date.now();
runtimeSpecsGauge.addCallback((obs) => {
	obs.observe(bootTimeMs, {
		commit: commitHash,
		driftEnv,
		rpcEndpoint: endpoint,
		wsEndpoint: wsEndpoint,
	});
});

let healthStatus: HEALTH_STATUS = HEALTH_STATUS.Ok;
const healthStatusGauge = meter.createObservableGauge(
	METRIC_TYPES.health_status,
	{
		description: 'Health status of this program',
	}
);
healthStatusGauge.addCallback((obs: ObservableResult) => {
	obs.observe(healthStatus, {});
});

let lastWsMsgReceivedTs = 0;
const setLastReceivedWsMsgTs = (ts: number) => {
	lastWsMsgReceivedTs = ts;
};
const lastWsReceivedTsGauge = meter.createObservableGauge(
	METRIC_TYPES.last_ws_message_received_ts,
	{
		description: 'Timestamp of last received websocket message',
	}
);
lastWsReceivedTsGauge.addCallback((obs: ObservableResult) => {
	obs.observe(lastWsMsgReceivedTs, {});
});

const cacheHitCounter = meter.createCounter(METRIC_TYPES.cache_hit_count, {
	description: 'Total redis cache hits',
});

const accountUpdatesCounter = meter.createCounter(
	METRIC_TYPES.account_updates_count,
	{
		description: 'Total accounts update',
	}
);

const currentSystemTsGauge = meter.createObservableGauge(
	METRIC_TYPES.current_system_ts,
	{
		description: 'Timestamp of system at time of metric collection',
	}
);
currentSystemTsGauge.addCallback((obs: ObservableResult) => {
	obs.observe(Date.now(), {});
});

const endpointResponseTimeHistogram = meter.createHistogram(
	METRIC_TYPES.endpoint_response_times_histogram,
	{
		description: 'Duration of endpoint responses',
		unit: 'ms',
	}
);
const gpaFetchDurationHistogram = meter.createHistogram(
	METRIC_TYPES.gpa_fetch_duration,
	{
		description: 'Duration of GPA fetches',
		unit: 'ms',
	}
);

const responseStatusCounter = meter.createCounter(
	METRIC_TYPES.endpoint_response_status,
	{
		description: 'Count of endpoint responses by status code',
	}
);

const healthCheckInterval = 2 * (ORDERBOOK_UPDATE_INTERVAL ?? 1000); // ORDERBOOK_UPDATE_INTERVAL is NaN here for some reason ... hardcode to 1000.
let lastHealthCheckSlot = -1;
let lastHealthCheckState = true; // true = healthy, false = unhealthy
let lastHealthCheckPerformed = Date.now() - healthCheckInterval;
let lastTimeHealthy = Date.now() - healthCheckInterval;

/**
 * Middleware that checks if we are in general healthy by checking that the bulk account loader slot
 * has changed recently.
 *
 * We may be hit by multiple sources performing health checks on us, so this middleware will latch
 * to its health state and only update every `healthCheckInterval`.
 *
 * A grace period is also used to only report unhealthy if we have been unhealthy for a certain
 * amount of time. This prevents reporting unhealthy even if we are just in the middle of a
 * bulk account load.
 */
const handleHealthCheck = (
	healthCheckGracePeriod: number,
	slotSource: SlotSource
) => {
	return async (_req, res, _next) => {
		if (Date.now() < lastHealthCheckPerformed + healthCheckInterval) {
			if (lastHealthCheckState) {
				res.writeHead(200);
				res.end('OK');
				lastHealthCheckPerformed = Date.now();
				return;
			}
			// always check if last check was unhealthy (give it another chance to recover)
		}

		// healthy if slot has advanced since the last check
		const lastSlotReceived = slotSource.getSlot();
		const inGracePeriod =
			Date.now() - lastTimeHealthy <= healthCheckGracePeriod;
		lastHealthCheckState = lastSlotReceived > lastHealthCheckSlot;
		if (!lastHealthCheckState) {
			logger.error(
				`Unhealthy: lastSlot: ${lastSlotReceived}, lastHealthCheckSlot: ${lastHealthCheckSlot}, timeSinceLastCheck: ${
					Date.now() - lastHealthCheckPerformed
				} ms, sinceLastTimeHealthy: ${
					Date.now() - lastTimeHealthy
				} ms, inGracePeriod: ${inGracePeriod}`
			);
		} else {
			lastTimeHealthy = Date.now();
		}

		lastHealthCheckSlot = lastSlotReceived;
		lastHealthCheckPerformed = Date.now();

		if (!lastHealthCheckState && !inGracePeriod) {
			healthStatus = HEALTH_STATUS.UnhealthySlotSubscriber;

			res.writeHead(500);
			res.end(`NOK`);
			return;
		}

		healthStatus = HEALTH_STATUS.Ok;
		res.writeHead(200);
		res.end('OK');
	};
};

export {
	endpointResponseTimeHistogram,
	gpaFetchDurationHistogram,
	responseStatusCounter,
	handleHealthCheck,
	setLastReceivedWsMsgTs,
	accountUpdatesCounter,
	cacheHitCounter,
};
