/**
 * Traces — every /v1/ request and response, verbatim, in SQLite.
 *
 * The log file says that a request happened; a trace says what it was: the
 * client's headers and body, the status, the response headers and every byte
 * written back (a stream is kept as the SSE text the client read). It is what
 * the client saw, after dario's rewrites on the way back and before them on
 * the way in, so a Responses client's row holds the translated stream.
 *
 * One database per calendar day in DARIO_TRACES_TZ (default Africa/Algiers):
 * `~/.dario/traces/YYYY-MM-DD.db`. The first write or read after local
 * midnight opens the new day's file and deletes every other one, so the
 * directory never holds more than today. Credentials in headers are
 * redacted; bodies are not — they are the point — which is why traces are
 * off by default and the directory is 0700.
 *
 * Read through GET /traces and /traces/<id> on the proxy (root key only).
 * `node:sqlite` is built into Node >= 22.13 and Bun; on an older Node,
 * --traces fails at startup instead of silently recording nothing.
 */
import { mkdirSync, readdirSync, rmSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import type { IncomingMessage, ServerResponse } from 'node:http';
import type { DatabaseSync } from 'node:sqlite';

export const DEFAULT_TRACES_TZ = 'Africa/Algiers';
export const TRACES_LIST_MAX = 500;
const REDACTED_HEADERS = new Set(['authorization', 'x-api-key', 'x-dario-admin-token', 'cookie', 'proxy-authorization']);

export function tracesDir(env: NodeJS.ProcessEnv = process.env): string {
  return env['DARIO_TRACES_DIR'] || join(homedir(), '.dario', 'traces');
}

/** YYYY-MM-DD in `tz`. en-CA formats dates as ISO. */
export function traceDay(tz: string, now: Date = new Date()): string {
  return new Intl.DateTimeFormat('en-CA', { timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit' }).format(now);
}

export function redactHeaders(h: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(h)) out[k] = REDACTED_HEADERS.has(k.toLowerCase()) ? '[redacted]' : v;
  return out;
}

/** A body as JSON when it is JSON, else the text. */
function bodyOut(s: string | null): unknown {
  if (s === null || s === '') return s;
  try { return JSON.parse(s); } catch { return s; }
}

export interface TraceListQuery { limit?: number; before?: number; path?: string; status?: number }

export class TraceStore {
  private db: DatabaseSync | null = null;
  private day = '';

  private constructor(
    private readonly Database: typeof DatabaseSync,
    readonly dir: string,
    readonly tz: string,
    private readonly now: () => Date,
  ) {}

  static async open(dir: string = tracesDir(), tz: string = process.env['DARIO_TRACES_TZ'] || DEFAULT_TRACES_TZ, now: () => Date = () => new Date()): Promise<TraceStore> {
    traceDay(tz); // throws RangeError on an unknown zone, at startup rather than on the first request
    const { DatabaseSync } = await import('node:sqlite').catch(() => {
      throw new Error(`--traces needs node:sqlite (Node >= 22.13, or Bun); this is Node ${process.version}. Upgrade Node or drop --traces.`);
    });
    mkdirSync(dir, { recursive: true, mode: 0o700 });
    const store = new TraceStore(DatabaseSync, dir, tz, now);
    store.current();
    return store;
  }

  /** Today's database, rolling over (and deleting the old days) when the day has changed. */
  private current(): DatabaseSync {
    const day = traceDay(this.tz, this.now());
    if (this.db && day === this.day) return this.db;
    this.db?.close();
    for (const f of readdirSync(this.dir)) {
      if (/^\d{4}-\d{2}-\d{2}\.db(-wal|-shm|-journal)?$/.test(f) && !f.startsWith(day + '.db')) rmSync(join(this.dir, f), { force: true });
    }
    const db = new this.Database(join(this.dir, `${day}.db`));
    db.exec(`PRAGMA journal_mode = WAL;
      CREATE TABLE IF NOT EXISTS traces (
        id INTEGER PRIMARY KEY,
        ts TEXT NOT NULL,
        duration_ms INTEGER NOT NULL,
        method TEXT NOT NULL,
        path TEXT NOT NULL,
        status INTEGER NOT NULL,
        consumer TEXT,
        remote TEXT,
        request_headers TEXT NOT NULL,
        request_body TEXT,
        response_headers TEXT NOT NULL,
        response_body TEXT NOT NULL
      )`);
    this.db = db;
    this.day = day;
    return db;
  }

  /**
   * Start tracing one request: taps `res` so every byte written back is kept,
   * and records the row when the response closes (finished or aborted). The
   * caller sets `requestBody` once it has read it.
   */
  begin(req: IncomingMessage, res: ServerResponse, arrivedAt: number, consumer: string | undefined): { requestBody: Buffer | null } {
    const handle = { requestBody: null as Buffer | null };
    const chunks: Buffer[] = [];
    const headers: Record<string, unknown> = {};
    const keep = (chunk: unknown, enc: unknown) => {
      if (typeof chunk === 'string') chunks.push(Buffer.from(chunk, typeof enc === 'string' ? enc as BufferEncoding : 'utf8'));
      else if (chunk instanceof Uint8Array) chunks.push(Buffer.from(chunk));
    };
    const writeHead = res.writeHead;
    res.writeHead = function (this: ServerResponse, ...a: unknown[]) {
      // Headers handed to writeHead never reach getHeaders(), so they are kept here.
      const h = a.slice(1).find((x) => x !== null && typeof x === 'object' && !Array.isArray(x));
      if (h) Object.assign(headers, h);
      return (writeHead as (...x: unknown[]) => ServerResponse).apply(this, a);
    } as typeof res.writeHead;
    const write = res.write;
    res.write = function (this: ServerResponse, chunk: unknown, ...a: unknown[]) {
      keep(chunk, a[0]);
      return (write as (...x: unknown[]) => boolean).call(this, chunk, ...a);
    } as typeof res.write;
    const end = res.end;
    res.end = function (this: ServerResponse, chunk?: unknown, ...a: unknown[]) {
      if (typeof chunk !== 'function') keep(chunk, a[0]);
      return (end as (...x: unknown[]) => ServerResponse).call(this, chunk, ...a);
    } as typeof res.end;
    res.once('close', () => {
      try {
        this.current().prepare(`INSERT INTO traces (ts, duration_ms, method, path, status, consumer, remote, request_headers, request_body, response_headers, response_body)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(
          new Date(arrivedAt).toISOString(),
          Date.now() - arrivedAt,
          req.method ?? '',
          req.url ?? '',
          res.statusCode,
          consumer ?? null,
          req.socket?.remoteAddress ?? null,
          JSON.stringify(redactHeaders(req.headers)),
          handle.requestBody ? handle.requestBody.toString('utf8') : null,
          JSON.stringify(redactHeaders({ ...res.getHeaders(), ...headers })),
          Buffer.concat(chunks).toString('utf8'),
        );
      } catch (err) {
        console.error(`[dario] traces: could not record ${req.method} ${req.url}: ${err instanceof Error ? err.message : err}`);
      }
    });
    return handle;
  }

  /** Newest first, without bodies. `before` pages: pass the last id seen. */
  list(q: TraceListQuery = {}): unknown[] {
    const where: string[] = [];
    const params: (string | number)[] = [];
    if (q.before !== undefined) { where.push('id < ?'); params.push(q.before); }
    if (q.path) { where.push('path LIKE ?'); params.push(q.path + '%'); }
    if (q.status !== undefined) { where.push('status = ?'); params.push(q.status); }
    const limit = Math.min(Math.max(1, q.limit ?? 50), TRACES_LIST_MAX);
    return this.current().prepare(`SELECT id, ts, duration_ms, method, path, status, consumer, remote,
        json_extract(CASE WHEN json_valid(request_body) THEN request_body END, '$.model') AS model,
        json_extract(CASE WHEN json_valid(request_body) THEN request_body END, '$.stream') AS stream,
        length(request_body) AS request_bytes, length(response_body) AS response_bytes
      FROM traces ${where.length ? 'WHERE ' + where.join(' AND ') : ''} ORDER BY id DESC LIMIT ${limit}`).all(...params);
  }

  get(id: number): unknown | null {
    const row = this.current().prepare('SELECT * FROM traces WHERE id = ?').get(id) as Record<string, unknown> | undefined;
    if (!row) return null;
    return {
      ...row,
      request_headers: JSON.parse(row['request_headers'] as string),
      request_body: bodyOut(row['request_body'] as string | null),
      response_headers: JSON.parse(row['response_headers'] as string),
      response_body: bodyOut(row['response_body'] as string),
    };
  }

  /** Today's file and row count, for the list response. */
  info(): { day: string; tz: string; file: string; count: number } {
    const db = this.current();
    const { n } = db.prepare('SELECT count(*) AS n FROM traces').get() as { n: number };
    return { day: this.day, tz: this.tz, file: join(this.dir, `${this.day}.db`), count: n };
  }

  close(): void {
    this.db?.close();
    this.db = null;
  }
}
