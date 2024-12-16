const esbuild = require('esbuild');
const glob = require('tiny-glob');

const commonConfig = {
    bundle: true,
    platform: 'node',
    target: 'node20',
    sourcemap: false,
    // minify: true, makes messy debug/error output
    treeShaking: true,
    legalComments: 'none',
    mainFields: ['module', 'main'],
    metafile: true,
    format: 'cjs',
    external: [
        'bigint-buffer',
        '@triton-one/yellowstone-grpc'
    ]
};

(async () => {
    let entryPoints = await glob("./src/*.ts", { filesOnly: true });
    await esbuild.build({
        ...commonConfig,
        entryPoints,
        outdir: 'lib',
    });
})().catch(() => process.exit(1));