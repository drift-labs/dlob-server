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
	commitHash,
	driftEnv,
	endpoint,
	getSlotHealthCheckInfo,
	wsEndpoint,
} from '..';

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

const endpointResponseTimeHistogram = meter.createHistogram(
	METRIC_TYPES.endpoint_response_times_histogram,
	{
		description: 'Duration of endpoint responses',
		unit: 'ms',
	}
);

const responseStatusCounter = meter.createCounter(
	METRIC_TYPES.endpoint_response_status,
	{
		description: 'Count of endpoint responses by status code',
	}
);

const healthCheckInterval = 2000;
let lastHealthCheckSlot = -1;
let lastHealthCheckSlotUpdated = Date.now();
const handleHealthCheck = async (req, res, next) => {
	const { lastSlotReceived, lastSlotReceivedMutex } = getSlotHealthCheckInfo();

	try {
		if (req.url === '/health' || req.url === '/') {
			// check if a slot was received recently
			let healthySlotSubscriber = false;
			await lastSlotReceivedMutex.runExclusive(async () => {
				const slotChanged = lastSlotReceived > lastHealthCheckSlot;
				const slotChangedRecently =
					Date.now() - lastHealthCheckSlotUpdated < healthCheckInterval;
				healthySlotSubscriber = slotChanged || slotChangedRecently;
				logger.debug(
					`Slotsubscriber health check: lastSlotReceived: ${lastSlotReceived}, lastHealthCheckSlot: ${lastHealthCheckSlot}, slotChanged: ${slotChanged}, slotChangedRecently: ${slotChangedRecently}`
				);
				if (slotChanged) {
					lastHealthCheckSlot = lastSlotReceived;
					lastHealthCheckSlotUpdated = Date.now();
				}
			});
			if (!healthySlotSubscriber) {
				healthStatus = HEALTH_STATUS.UnhealthySlotSubscriber;
				logger.error(`SlotSubscriber is not healthy`);

				res.writeHead(500);
				res.end(`SlotSubscriber is not healthy`);
				return;
			}

			// liveness check passed
			healthStatus = HEALTH_STATUS.Ok;
			res.writeHead(200);
			res.end('OK');
		} else {
			res.writeHead(404);
			res.end('Not found');
		}
	} catch (e) {
		next(e);
	}
};

export {
	endpointResponseTimeHistogram,
	responseStatusCounter,
	handleHealthCheck,
};
