import { addClassPath, loadFile } from 'nbb';

addClassPath("src")

const { handler } = await loadFile('./src/card_get.cljs');

export { handler }