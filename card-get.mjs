#!/usr/bin/env node

import { addClassPath, loadFile } from 'nbb';
import { fileURLToPath } from 'url';
import { dirname, resolve } from 'path';

const __dirname = fileURLToPath(dirname(import.meta.url));

addClassPath(resolve(__dirname, 'src'));
const { handler } = await loadFile(resolve(__dirname, 'src/card_get.cljs'));

console.log("HANDLER", handler);
export { handler }
