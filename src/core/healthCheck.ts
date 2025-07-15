export enum HEALTH_STATUS {
	Ok = 0,
	StaleBulkAccountLoader,
	UnhealthySlotSubscriber,
	LivenessTesting,
	Restart,
}

/**
 * Health check configuration
 */
const HEALTH_CHECK_CONFIG = {
	CHECK_INTERVAL_MS: 2000,
	// Maximum time allowed between slot updates
	MAX_SLOT_STALENESS_MS: 5000,
	// Minimum expected slot advancement rate (slots slower than 1 per second is problematic)
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

	// If slot has progressed, we are healthy, check rate and update state.
	if (currentSlot > globalHealthState.lastSlot) {
		// Update state
		globalHealthState.lastSlot = currentSlot;
		globalHealthState.lastSlotTimestamp = now;

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

	// If slot has NOT progressed, check for staleness.
	if (timeDelta > HEALTH_CHECK_CONFIG.MAX_SLOT_STALENESS_MS) {
		return {
			isHealthy: false,
			reason: `No slot updates in ${timeDelta}ms (max ${HEALTH_CHECK_CONFIG.MAX_SLOT_STALENESS_MS}ms)`,
		};
	}

	// Slot has not progressed, but not stale yet. Still healthy.
	return { isHealthy: true };
}

let healthStatus: HEALTH_STATUS = HEALTH_STATUS.Ok;
const setHealthStatus = (status: HEALTH_STATUS): void => {
	healthStatus = status;
};

const getHealthStatus = (): HEALTH_STATUS => {
	return healthStatus;
};

export {
	evaluateHealth,
	globalHealthState,
	HEALTH_CHECK_CONFIG,
	setHealthStatus,
	getHealthStatus,
};
