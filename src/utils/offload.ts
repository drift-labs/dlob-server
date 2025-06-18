import {
	KinesisClient,
	PutRecordsCommand,
	PutRecordsRequestEntry,
} from '@aws-sdk/client-kinesis';
import { ConfiguredRetryStrategy } from '@aws-sdk/util-retry';
import { logger } from '../utils/logger';

type EventType = 'DLOBSnapshot' | 'DLOBL3Snapshot';

interface QueueMessage {
	data: any;
	eventType: EventType;
}

const kinesis = new KinesisClient({
	retryStrategy: new ConfiguredRetryStrategy(
		5,
		(attempt: number) => 100 + attempt * 500
	),
});

const kinesisStream = process.env.KINESIS_STREAM_NAME;
const batchInterval = process.env.KINESIS_BATCH_INTERVAL
	? parseInt(process.env.KINESIS_BATCH_INTERVAL)
	: 10000;

export const OffloadQueue = () => {
	const queue: QueueMessage[] = [];
	let isProcessing = false;

	setInterval(async () => {
		if (!isProcessing && queue.length > 0) {
			await processQueue();
		}
	}, batchInterval);

	const processQueue = async (): Promise<void> => {
		if (isProcessing || queue.length === 0) {
			return;
		}

		isProcessing = true;

		try {
			const batch = queue.splice(0, 500); // Kinesis limit

			const records: PutRecordsRequestEntry[] = batch.map((msg) => ({
				Data: Buffer.from(
					JSON.stringify({
						...msg.data,
						source: 'dlob',
					})
				),
				PartitionKey: Date.now().toString(),
			}));

			const command = new PutRecordsCommand({
				StreamName: kinesisStream,
				Records: records,
			});

			const response = await kinesis.send(command);

			if (response.FailedRecordCount && response.FailedRecordCount > 0) {
				logger.warn(`${response.FailedRecordCount} records failed to send`);

				const failedMessages: QueueMessage[] = [];
				response.Records?.forEach((record, index) => {
					if (record.ErrorCode) {
						failedMessages.push(batch[index]);
						logger.error(
							`Record ${index} failed: ${record.ErrorCode} - ${record.ErrorMessage}`
						);
					}
				});

				queue.unshift(...failedMessages);
			}

			const successCount = batch.length - (response.FailedRecordCount || 0);
			logger.info(
				`Successfully sent ${successCount} records to Kinesis stream: ${kinesisStream}`
			);
		} catch (error) {
			logger.error('Error processing queue:', error);
		} finally {
			isProcessing = false;
		}
	};

	const addMessage = (data: any, eventType: EventType = 'DLOBSnapshot') => {
		queue.push({
			data,
			eventType,
		});
	};

	return {
		addMessage,
	};
};
