import { L2OrderBook } from '@drift-labs/sdk';

export const l2WithBNToStrings = (l2: L2OrderBook): any => {
	for (const key of Object.keys(l2)) {
		for (const idx in l2[key]) {
			const level = l2[key][idx];
			const sources = level['sources'];
			for (const sourceKey of Object.keys(sources)) {
				sources[sourceKey] = sources[sourceKey].toString();
			}
			l2[key][idx] = {
				price: level.price.toString(),
				size: level.size.toString(),
				sources,
			};
		}
	}
	return l2;
};

export function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}
