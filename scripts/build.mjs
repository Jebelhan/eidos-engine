/**
 * 构建脚本：把 src/ 复制到 lib/，并把运行时资源（prompts/）放进 lib/。
 * 不做打包/压缩：逐文件复制，产物与源码一一对应，便于排查。
 */

import { cpSync, existsSync, mkdirSync, readdirSync, rmSync, statSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const srcDir = join(root, 'src');
const libDir = join(root, 'lib');
const promptsDir = join(root, 'prompts');

rmSync(libDir, { recursive: true, force: true });
mkdirSync(libDir, { recursive: true });
cpSync(srcDir, libDir, { recursive: true });
if (existsSync(promptsDir)) {
  cpSync(promptsDir, join(libDir, 'prompts'), { recursive: true });
}

function walk(dir, base = '') {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const rel = base ? `${base}/${entry.name}` : entry.name;
    const abs = join(dir, entry.name);
    return entry.isDirectory() ? walk(abs, rel) : [rel];
  });
}

const files = [...walk(srcDir), ...(existsSync(promptsDir) ? walk(promptsDir).map((f) => `prompts/${f}`) : [])];
if (!files.includes('index.js')) {
  throw new Error('build failed: src/index.js missing');
}
console.log(`built ${files.length} file(s): ${files.map((f) => `lib/${f}`).join(', ')}`);
