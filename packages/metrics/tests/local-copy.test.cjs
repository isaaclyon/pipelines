const { test } = require('node:test');
const assert = require('node:assert/strict');
const { mkdtemp, mkdir, rm, readFile, stat } = require('node:fs/promises');
const { join } = require('node:path');
const { tmpdir } = require('node:os');
const { DuckDBDriver } = require('@cubejs-backend/duckdb-driver');
const { execFile, spawn } = require('node:child_process');
const { promisify } = require('node:util');
const { once } = require('node:events');
const { copyLocalDatabase } = require('../local-copy.cjs');

test('clean copies leave the source untouched on success and failure and keep branches independent', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'local-copy-'));
  const source = join(directory, 'source.duckdb');
  const a = join(directory, 'branch-a');
  const b = join(directory, 'branch-b');
  try {
    await mkdir(a); await mkdir(b);
    await promisify(execFile)(process.execPath, ['-e',
      "require('./tests/orders-fixture.cjs').publish(process.argv[1]).catch(e => { console.error(e); process.exit(1); })", source], { timeout: 10000 });
    const original = await readFile(source);
    const originalStat = await stat(source);
    const first = await copyLocalDatabase(source, a);
    const second = await copyLocalDatabase(source, b);
    assert.notEqual(first, second);
    await assert.rejects(copyLocalDatabase(source, a), /exist/i);
    const before = await readFile(second);
    const writer = new DuckDBDriver({ databasePath: first });
    try { await writer.query('DELETE FROM ci_marts.fct_orders'); }
    finally { await writer.release(); }
    assert.deepEqual(await readFile(second), before);
    assert.deepEqual(await readFile(source), original);
    assert.equal((await stat(source)).mtimeMs, originalStat.mtimeMs);
    assert.equal((await stat(source)).ino, originalStat.ino);
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('a stopped source with recovery state is refused without repair or mutation', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'local-copy-wal-'));
  const source = join(directory, 'source.duckdb');
  const destination = join(directory, 'copy');
  try {
    await mkdir(destination);
    const held = spawn(process.execPath, ['-e',
      "const { DuckDBDriver } = require('@cubejs-backend/duckdb-driver'); const db = new DuckDBDriver({ databasePath: process.argv[1] }); db.query('CREATE TABLE recovery_probe AS SELECT 1 AS id').then(() => console.log('ready')); setTimeout(() => process.exit(0), 10000);", source]);
    await once(held.stdout, 'data');
    held.kill('SIGKILL');
    await once(held, 'exit');
    const original = await readFile(source);
    const recovery = await readFile(source + '.wal');
    const originalStat = await stat(source);
    await assert.rejects(copyLocalDatabase(source, destination), /WAL.*owner|owner.*WAL/i);
    assert.deepEqual(await readFile(source), original);
    assert.deepEqual(await readFile(source + '.wal'), recovery);
    assert.equal((await stat(source)).mtimeMs, originalStat.mtimeMs);
    await assert.rejects(stat(join(destination, 'working.duckdb')), /ENOENT/);
  } finally { await rm(directory, { recursive: true, force: true }); }
});
