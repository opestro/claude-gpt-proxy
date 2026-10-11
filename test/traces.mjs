// Traces (src/traces.ts): a request and its streamed response land in today's
// SQLite file with credentials redacted, and the first touch after local
// midnight in the trace zone starts a new file and deletes the old one.

import { TraceStore, traceDay } from '../dist/traces.js';
import { createServer } from 'node:http';
import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

let pass = 0, fail = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✅ ${label}`); pass++; }
  else { console.log(`  ❌ ${label}`); fail++; }
}

// Africa/Algiers is UTC+1 all year: 23:00Z is already the next day there.
check('22:59Z is still the 11th in Algiers', traceDay('Africa/Algiers', new Date('2026-10-11T22:59:00Z')) === '2026-10-11');
check('23:00Z is the 12th in Algiers', traceDay('Africa/Algiers', new Date('2026-10-11T23:00:00Z')) === '2026-10-12');

const dir = mkdtempSync(join(tmpdir(), 'dario-traces-'));
let now = new Date('2026-10-11T12:00:00Z');
const store = await TraceStore.open(dir, 'Africa/Algiers', () => now);

const server = createServer((req, res) => {
  const chunks = [];
  req.on('data', (c) => chunks.push(c));
  req.on('end', () => {
    store.begin(req, res, Date.now(), 'alice').requestBody = Buffer.concat(chunks);
    res.writeHead(200, { 'Content-Type': 'text/event-stream' });
    res.write('event: a\ndata: {"x":1}\n\n');
    res.end('event: b\ndata: {"x":2}\n\n');
  });
});
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const url = `http://127.0.0.1:${server.address().port}/v1/messages`;
await (await fetch(url, { method: 'POST', headers: { 'x-api-key': 'sekrit', 'content-type': 'application/json' }, body: JSON.stringify({ model: 'claude-x', stream: true }) })).text();
await new Promise((r) => setTimeout(r, 50)); // the row is written on 'close'

const list = store.list();
check('one row listed', list.length === 1);
check('model and consumer in the summary', list[0]?.model === 'claude-x' && list[0]?.consumer === 'alice' && list[0]?.status === 200);
const row = store.get(list[0].id);
check('request body parsed back as JSON', row.request_body.model === 'claude-x');
check('whole stream kept', row.response_body === 'event: a\ndata: {"x":1}\n\nevent: b\ndata: {"x":2}\n\n');
check('writeHead headers kept', row.response_headers['Content-Type'] === 'text/event-stream');
check('api key redacted', row.request_headers['x-api-key'] === '[redacted]');
check('today\'s file exists', existsSync(join(dir, '2026-10-11.db')));

now = new Date('2026-10-11T23:00:00Z');
check('new day starts empty', store.list().length === 0 && store.info().day === '2026-10-12');
check('yesterday\'s file deleted', !existsSync(join(dir, '2026-10-11.db')));

server.close();
store.close();
rmSync(dir, { recursive: true, force: true });
console.log(`\n  Results: ${pass} passed, ${fail} failed\n`);
if (fail > 0) process.exit(1);
