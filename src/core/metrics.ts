import { ObservableResult } from '@opentelemetry/api';
import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import { logger } from '../utils/logger';
import {
	ExplicitBucketHistogramAggregation,
	InstrumentType,
	MeterProvider,
	View,
} from '@opentelemetry/sdk-metrics-base';
import { SlotSource } from '@drift-labs/sdk';
require('dotenv').config();

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
	incoming_requests_count = 'incoming_requests_count',
	kinesis_records_sent = 'kinesis_records_sent',
}

export enum HEALTH_STATUS {
	Ok = 0,
	StaleBulkAccountLoader,
	UnhealthySlotSubscriber,
	LivenessTesting,
	Restart,
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

const incomingRequestsCounter = meter.createCounter(
	METRIC_TYPES.incoming_requests_count,
	{
		description: 'Total incoming requests',
	}
);

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

const kinesisRecordsSentGauge = meter.createObservableGauge(
	METRIC_TYPES.kinesis_records_sent,
	{
		description: 'Number of records successfully sent to Kinesis stream',
	}
);

let kinesisRecordsSent: KinesisMetric = {
	stream: '',
	count: 0,
};

const setKinesisRecordsSent = (count: number, stream: string): void => {
	kinesisRecordsSent = {
		stream,
		count,
	};
};

kinesisRecordsSentGauge.addCallback((obs: ObservableResult) => {
	obs.observe(kinesisRecordsSent.count, { stream: kinesisRecordsSent.stream });
});

/**
 * Health check configuration
 */
const HEALTH_CHECK_CONFIG = {
	CHECK_INTERVAL_MS: 2000,
	// Maximum time allowed between slot updates
	MAX_SLOT_STALENESS_MS: 5000,
	// Minimum expected slot advancement rate (slots per second)
	MIN_SLOT_RATE: 1,
} as const;

/**
 * Tracks the health state of the slot subscriber
 */
type HealthState = {
	lastSlot: number;
	lastSlotTimestamp: number;
};

const globalHealthState: HealthState = {
	lastSlot: -1,
	lastSlotTimestamp: Date.now(),
};

/**
 * Evaluates if the current state is healthy based on slot progression
 */
function evaluateHealth(currentSlot: number): {
	isHealthy: boolean;
	reason?: string;
} {
	const now = Date.now();

	// First health check
	if (globalHealthState.lastSlot === -1) {
		globalHealthState.lastSlot = currentSlot;
		globalHealthState.lastSlotTimestamp = now;
		return { isHealthy: true };
	}

	const timeDelta = now - globalHealthState.lastSlotTimestamp;
	const slotDelta = currentSlot - globalHealthState.lastSlot;

	// Update state if slot has progressed
	if (currentSlot > globalHealthState.lastSlot) {
		globalHealthState.lastSlot = currentSlot;
		globalHealthState.lastSlotTimestamp = now;
	}

	// Check if slots are too stale
	if (timeDelta > HEALTH_CHECK_CONFIG.MAX_SLOT_STALENESS_MS) {
		return {
			isHealthy: false,
			reason: `No slot updates in ${timeDelta}ms (max ${HEALTH_CHECK_CONFIG.MAX_SLOT_STALENESS_MS}ms)`,
		};
	}

	// Check if slot update rate is too low
	const slotRate = (slotDelta / timeDelta) * 1000; // Convert to per second
	if (slotRate < HEALTH_CHECK_CONFIG.MIN_SLOT_RATE) {
		return {
			isHealthy: false,
			reason: `Slot update rate ${slotRate.toFixed(
				2
			)} slots/sec below minimum ${HEALTH_CHECK_CONFIG.MIN_SLOT_RATE}`,
		};
	}

	return { isHealthy: true };
}

/**
 * Middleware that checks the health of the slot subscriber.
 * Health is determined by:
 * 1. Regular slot updates
 * 2. Minimum slot update rate
 */
const handleHealthCheck = (slotSource: SlotSource) => {
	return async (_req, res, _next) => {
		const currentSlot = slotSource.getSlot();
		const { isHealthy, reason } = evaluateHealth(currentSlot);

		if (!isHealthy) {
			logger.error(
				`Unhealthy: ${reason}, ` +
					`Details: currentSlot=${currentSlot}, ` +
					`lastSlot=${globalHealthState.lastSlot}, ` +
					`timeSinceLastSlot=${
						Date.now() - globalHealthState.lastSlotTimestamp
					}ms`
			);
			setHealthStatus(HEALTH_STATUS.UnhealthySlotSubscriber);
			res.writeHead(500);
			res.end('NOK');
			return;
		}

		setHealthStatus(HEALTH_STATUS.Ok);
		res.writeHead(200);
		res.end('OK');
	};
};

export const setHealthStatus = (status: HEALTH_STATUS): void => {
	healthStatus = status;
};

interface KinesisMetric {
	stream: string;
	count: number;
}

export {
	endpointResponseTimeHistogram,
	gpaFetchDurationHistogram,
	responseStatusCounter,
	incomingRequestsCounter,
	handleHealthCheck,
	setLastReceivedWsMsgTs,
	accountUpdatesCounter,
	cacheHitCounter,
	runtimeSpecsGauge,
	setKinesisRecordsSent,
};
