import { setTimeout } from "node:timers/promises";

const TIMEOUT = 30_000;
const MAX_RETRIES = 3;

type Message = {
  id: string;
  body: Buffer;
  received: number;
  type: string;
};

/**
 *
 * We don't use the type from `better-sqlite3` here to not have a dependency to it.
 *
 * https://github.com/JoshuaWise/better-sqlite3/blob/master/docs/api.md#new-databasepath-options
 */
export interface SqliteDatabase {
  close(): void;
  exec(sql: string): void;
  prepare(sql: string): SqliteStatement;
}

export interface SqliteStatement<Result = unknown> {
  run(...parameters: ReadonlyArray<unknown>): {
    changes: number | bigint;
    lastInsertRowid: number | bigint;
  };
  get(...params: ReadonlyArray<unknown>): Result | null;
}

const toPrecise = (date: Date): string => {
  // RFC3339
  const pad = (num: number): string => num.toString().padStart(2, "0");
  const year = date.getUTCFullYear();
  const month = pad(date.getUTCMonth() + 1);
  const day = pad(date.getUTCDate());
  const hours = pad(date.getUTCHours());
  const minutes = pad(date.getUTCMinutes());
  const seconds = pad(date.getUTCSeconds());
  const milliseconds = date.getUTCMilliseconds().toString().padStart(3, "0");
  return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}.${milliseconds}Z`;
};

const sql = String.raw;
const schema = sql`
    create table if not exists sqslite (
      id text primary key default ('m_' || lower(hex(randomblob(16)))),
      created text not null default (strftime('%Y-%m-%dT%H:%M:%fZ')),
      updated text not null default (strftime('%Y-%m-%dT%H:%M:%fZ')),
      queue text not null,
      body blob not null,
      type text not null,
      timeout text not null default (strftime('%Y-%m-%dT%H:%M:%fZ')),
      received integer not null default 0
    ) strict;
    create trigger if not exists sqslite_updated_timestamp after update on sqslite begin
      update sqslite set updated = strftime('%Y-%m-%dT%H:%M:%fZ') where id = old.id;
    end;
    create index if not exists sqslite_queue_created_idx on sqslite (queue, created);
  `;

const setup = (
  c: SqliteDatabase,
  opts: {
    cacheSize?: number;
  },
) => {
  try {
    const cacheSize = opts.cacheSize || 10_000;
    // enable WAL mode
    c.exec("PRAGMA journal_mode = WAL;");
    // enable synchronous mode
    c.exec("PRAGMA synchronous = NORMAL;");
    // enable memory mode
    c.exec("PRAGMA temp_store = MEMORY;");
    // set cache size
    c.exec(`PRAGMA cache_size = ${cacheSize};`);
    c.exec(schema);
  } catch (e) {
    console.error("sqslite:Failed to setup schema");
    console.error(e);
    throw e;
  }
};

const sendMessage = (
  c: SqliteDatabase,
  queue: string,
  type: string,
  message: any,
) => {
  const insertQuery = c.prepare(sql`
        insert into
         sqslite (queue,type, body)
         values (@queue,@type,@body)
         returning id
     `);
  const result = insertQuery.get({
    queue,
    type: type,
    body: Buffer.from(JSON.stringify(message)),
  });
  return result as string;
};

const receiveMessage = (
  c: SqliteDatabase,
  params: {
    name: string;
    types: string[];
    opts?: {
      maxRetries?: number;
      timeout?: number;
    };
  },
) => {
  const { name, types, opts } = params;
  const timeout = opts?.timeout || TIMEOUT;
  const maxRetries = opts?.maxRetries || MAX_RETRIES;
  const query = c.prepare(sql`
        update sqslite
        set
          timeout = @timeout,
          received = received + 1
        where id = (
          select id from sqslite
          where
          type IN (select value from json_each(@types)) and
            queue = @queue and
            @now >= timeout and
            received < @maxRetries
          order by created
          limit 1
        )
        returning id,body,received,type
    `);

  const result = query.get({
    timeout: toPrecise(new Date(Date.now() + timeout)),
    now: toPrecise(new Date()),
    maxRetries,
    queue: name,
    types: JSON.stringify(types),
  });
  if (result) {
    return {
      ...result,
      body: Buffer.from((result as any).body),
    } as Message;
  }
  return result as null;
};

const deleteMessage = (c: SqliteDatabase, queue: string, id: string) => {
  const query = c.prepare(sql`
        delete from sqslite where queue = @queue and id = @id returning id
        `);
  const result = query.get({ queue, id });
  return result as string;
};

async function* poller(
  c: SqliteDatabase,
  params: {
    name: string;
    types: string[];
    interval: number;
    opts: {
      maxRetries?: number;
      timeout?: number;
    };
  },
) {
  while (true) {
    const messageOrNull = receiveMessage(c, params);
    if (messageOrNull) {
      yield messageOrNull;
    }
    await setTimeout(params.interval);
  }
}

const extendTimeout = async (
  c: SqliteDatabase,
  queue: string,
  id: string,
  timeout: string,
) => {
  const query = c.prepare(sql`
      update sqslite set timeout = @timeout where queue = @queue and id = @id returning id
    `);
  const result = query.get({ timeout, queue, id });
  return result;
};

export const sqslite = {
  setup,
  sendMessage,
  receiveMessage,
  deleteMessage,
  poller,
  extendTimeout,
};
