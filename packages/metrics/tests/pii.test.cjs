const { test } = require('node:test');
const assert = require('node:assert/strict');
const { checkCubePii } = require('../exposures.cjs');
const { copyModel } = require('./model-directory.cjs');

test('Cube requires explicit boolean PII declarations even on hidden members', () => {
  for (const pii of [undefined, null, 'false', 0, 1]) {
    assert.throws(() => checkCubePii([{
      name: 'customers', public: false,
      dimensions: { email: { public: false, meta: { pii } } },
    }]), /customers.email.*boolean/);
  }
});

test('PII cannot enter a Cube model, including hidden and joined members', () => {
  assert.throws(() => checkCubePii([{
    name: 'contacts', public: false,
    dimensions: { email: { public: false, meta: { pii: true } } },
  }]), /contacts.email.*restricted PII/);
});

test('derived measures and segments require their own decision', () => {
  assert.doesNotThrow(() => checkCubePii([{
    name: 'customers', measures: { count: { type: 'count', meta: { pii: false } } },
  }]));
  assert.throws(() => checkCubePii([{
    name: 'customers', segments: { selected: { sql: 'something' } },
  }]), /customers.selected.*boolean/);
});

test('actual Cube compilation rejects a classified PII fixture', { timeout: 60000 }, async () => {
  const { mkdtemp, readFile, writeFile, rm } = require('node:fs/promises');
  const { tmpdir } = require('node:os');
  const { join } = require('node:path');
  const { compileExposures } = require('../exposures.cjs');
  // Customer dimensions take their classification from the dbt snapshot. Each
  // case gets its own directory because Node caches the loaded snapshot.
  for (const [pii, rejection] of /** @type {const} */ ([
    [true, /shopify_customers.locale: restricted PII/],
    [undefined, /shopify_customers.locale: declare boolean meta.pii/],
  ])) {
    const directory = await mkdtemp(join(tmpdir(), 'cube-pii-'));
    try {
      await copyModel(directory);
      const snapshot = join(directory, 'mart-contracts.json');
      const marts = JSON.parse(await readFile(snapshot, 'utf8'));
      await writeFile(snapshot, JSON.stringify(marts.map((/** @type {any} */ mart) => mart.relation !== 'dim_customers' ? mart
        : { ...mart, columns: mart.columns.map((/** @type {any} */ column) => column.name === 'locale' ? { ...column, pii } : column) })));
      await assert.rejects(compileExposures(true, join(directory, 'model')), rejection);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  }
});

test('hidden direct aliases reach the dbt classification check even when views omit them', { timeout: 60000 }, async () => {
  const { mkdtemp, readFile, writeFile, rm } = require('node:fs/promises');
  const { tmpdir } = require('node:os');
  const { join } = require('node:path');
  const { compileExposures } = require('../exposures.cjs');
  const directory = await mkdtemp(join(tmpdir(), 'cube-hidden-pii-'));
  try {
    const model = join(directory, 'model');
    await copyModel(directory);
    const path = join(model, 'orders.js');
    const original = await readFile(path, 'utf8');
    const position = original.indexOf("cube('shopify_order_geography', {");
    const modified = original.slice(0, position) + original.slice(position).replace(
      '  dimensions: {',
      "  dimensions: {\n    contact_probe: { sql: 'order_email', type: 'string', public: false, meta: { pii: false } },");
    await writeFile(path, modified);
    for (const enabled of [false, true]) {
      const exposures = await compileExposures(enabled, model);
      const inventory = exposures.find(exposure => exposure.view === 'model_classifications');
      assert.ok(inventory, 'all cubes need an independent classification inventory');
      const field = inventory.fields.find(field => field.field === 'shopify_order_geography.contact_probe');
      assert.ok(field, 'hidden alias must not disappear from cross-boundary validation');
      assert.equal(field.directColumn, `${enabled ? 'dim_order_geography' : 'dim_order_contacts'}.order_email`);
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
