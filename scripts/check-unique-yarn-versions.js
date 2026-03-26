#!/usr/bin/env node
/**
 * Ensures Yarn v1 lockfile lists at most one resolved semver for each watched package.
 * Duplicates mean multiple copies can be installed (e.g. SDK vs app drift).
 *
 * Usage: node scripts/check-unique-yarn-versions.js
 * Override lock path: YARN_LOCK_PATH=path/to/yarn.lock node scripts/check-unique-yarn-versions.js
 */

const fs = require("fs");
const path = require("path");

const WATCHED = [
	"@triton-one/yellowstone-grpc",
	"rpc-websockets",
];

function collectResolvedVersions(lockContent, packageName) {
	const versions = new Set();
	const lines = lockContent.split(/\r?\n/);
	const isScoped = packageName.startsWith("@");

	for (let i = 0; i < lines.length; i++) {
		const line = lines[i];
		let matched = false;

		if (isScoped) {
			const escaped = packageName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
			matched = new RegExp(`^"${escaped}@[^"]+":\\s*$`).test(line);
		} else {
			const escaped = packageName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
			matched = new RegExp(`^${escaped}@[^:]+:\\s*$`).test(line);
		}

		if (!matched) {
			continue;
		}

		for (let j = i + 1; j < Math.min(i + 30, lines.length); j++) {
			const vm = lines[j].match(/^\s+version "([^"]+)"/);
			if (vm) {
				versions.add(vm[1]);
				break;
			}
			// Next top-level key — malformed block
			if (/^[^ \t#]/.test(lines[j]) && lines[j].trim() !== "") {
				break;
			}
		}
	}

	return versions;
}

function main() {
	const lockPath = path.resolve(
		process.env.YARN_LOCK_PATH || path.join(process.cwd(), "yarn.lock")
	);

	if (!fs.existsSync(lockPath)) {
		console.error(`check-unique-yarn-versions: missing lockfile: ${lockPath}`);
		process.exit(1);
	}

	const lockContent = fs.readFileSync(lockPath, "utf8");
	let failed = false;

	for (const pkg of WATCHED) {
		const versions = collectResolvedVersions(lockContent, pkg);
		if (versions.size === 0) {
			console.error(
				`check-unique-yarn-versions: no "${pkg}" entries found in ${lockPath}`
			);
			failed = true;
			continue;
		}
		if (versions.size > 1) {
			console.error(
				`check-unique-yarn-versions: "${pkg}" has multiple resolved versions in the lockfile (${[
					...versions,
				].sort().join(", ")}). Align dependencies or use resolutions.`
			);
			failed = true;
		} else {
			const [v] = [...versions];
			console.log(`check-unique-yarn-versions: OK ${pkg}@${v}`);
		}
	}

	process.exit(failed ? 1 : 0);
}

main();
