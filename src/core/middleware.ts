import { Request, Response } from 'express';
import responseTime = require('response-time');
import {
	endpointResponseTimeHistogram,
	responseStatusCounter,
} from './metrics';

export const handleResponseTime = responseTime(
	(req: Request, res: Response, time: number) => {
		const endpoint = req.path;

		if (endpoint === '/health' || req.url === '/') {
			return;
		}

		responseStatusCounter.add(1, {
			endpoint,
			status: res.statusCode,
		});

		const responseTimeMs = time;
		endpointResponseTimeHistogram.record(responseTimeMs, {
			endpoint,
		});
	}
);
