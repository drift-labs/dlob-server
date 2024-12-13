const esbuild = require('esbuild');

const commonConfig = {
    bundle: true,
    platform: 'node',
    target: 'node20',
    sourcemap: true,
    // minify: true, makes messy debug/error output
    treeShaking: true,
    legalComments: 'none',
    mainFields: ['module', 'main'],
    metafile: true,
    format: 'cjs',
    external: [
        '@triton-one/yellowstone-grpc'
    ]
};

esbuild.build({
    ...commonConfig,
    entryPoints: ['src/index.ts'],
    outdir: 'lib',
}).catch(() => process.exit(1));