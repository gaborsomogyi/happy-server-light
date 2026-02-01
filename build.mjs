import { build } from 'esbuild';

await build({
  entryPoints: ['sources/main.ts'],
  bundle: true,
  platform: 'node',
  target: 'node22',
  outfile: 'dist/main.js',
  format: 'esm',
  sourcemap: true,
  external: [
    'better-sqlite3',
    'sharp',
    'pino',
    'pino-pretty',
    'pino/*',
  ],
  banner: {
    js: "import { createRequire } from 'module'; import { fileURLToPath as __fileURLToPath } from 'url'; import { dirname as __pathDirname } from 'path'; const require = createRequire(import.meta.url); const __filename = __fileURLToPath(import.meta.url); const __dirname = __pathDirname(__filename);",
  },
});

console.log('Build complete: dist/main.js');
