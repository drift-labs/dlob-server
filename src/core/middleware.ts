import { Request, Response } from 'express';
import responseTime = require('response-time');
import {
	endpointResponseTimeHistogram,
	responseStatusCounter,
} from './metrics';
import { MEASURED_ENDPOINTS } from '../utils/constants';

export const handleResponseTime = (
	measuredEndpoints: string[] = MEASURED_ENDPOINTS
) => {
	return responseTime((req: Request, res: Response, time: number) => {
		const endpoint = req.path;

		if (!measuredEndpoints.includes(endpoint)) {
			return;
		}
		//if (endpoint === '/health' || req.url === '/') {
		//  return;
		//}

		responseStatusCounter.add(1, {
			endpoint,
			status: res.statusCode,
		});

		const responseTimeMs = time;
		endpointResponseTimeHistogram.record(responseTimeMs, {
			endpoint,
		});
	});
};
