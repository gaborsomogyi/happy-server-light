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
  ],
  banner: {
    js: "import { createRequire } from 'module'; const require = createRequire(import.meta.url);",
  },
});

console.log('Build complete: dist/main.js');
