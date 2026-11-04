const { test } = require('node:test');
const assert = require('node:assert/strict');
const { readdir, readFile } = require('node:fs/promises');
const { join } = require('node:path');

const MODEL = join(__dirname, '..', 'model');

/**
 * Until Cube has per-user authorization, no Cube member may hold PII.
 * Check hidden members too: hiding is not a substitute for the policy.
 * Author declarations are decisions, not inferred from SQL or lineage.
 * @param {Array<{name: string, dimensions?: Record<string, any>, measures?: Record<string, any>, segments?: Record<string, any>}>} cubes
 */
function checkCubePii(cubes) {
  for (const cube of cubes) {
    for (const group of [cube.dimensions, cube.measures, cube.segments]) {
      for (const [name, member] of Object.entries(group ?? {})) {
        if (typeof member.meta?.pii !== 'boolean') {
          throw new Error(`${cube.name}.${name}: declare boolean meta.pii`);
        }
        if (member.meta.pii) {
          throw new Error(`${cube.name}.${name}: restricted PII cannot be modeled in Cube without per-user authorization`);
        }
      }
    }
  }
}

/** Compile the maintained model offline with the pinned Cube schema compiler. */
async function compileModel() {
  const { prepareCompiler } = require('@cubejs-backend/schema-compiler');
  const files = (await readdir(MODEL)).filter(name => name.endsWith('.js')).sort();
  const contents = await Promise.all(files.map(name => readFile(join(MODEL, name), 'utf8')));
  const repository = {
    localPath: () => MODEL,
    dataSchemaFiles: async () => files.map((name, index) => ({ fileName: `model/${name}`, content: contents[index] ?? '' })),
  };
  const { compiler, cubeEvaluator, metaTransformer } = prepareCompiler(repository, { standalone: true });
  await compiler.compile();
  return { cubeEvaluator, metaTransformer };
}

test('Cube requires explicit boolean PII declarations even on hidden members', () => {
  for (const pii of [undefined, null, 'false', 0, 1]) {
    assert.throws(() => checkCubePii([{
      name: 'customers', dimensions: { email: { public: false, meta: { pii } } },
    }]), /customers.email.*boolean/);
  }
  assert.throws(() => checkCubePii([{
    name: 'customers', segments: { selected: { sql: 'something' } },
  }]), /customers.selected.*boolean/);
});

test('PII cannot enter a Cube model, including hidden members', () => {
  assert.throws(() => checkCubePii([{
    name: 'contacts', dimensions: { email: { public: false, meta: { pii: true } } },
  }]), /contacts.email.*restricted PII/);
});

test('the model compiles, classifies every member and documents its public interface', { timeout: 60000 }, async () => {
  const { cubeEvaluator, metaTransformer } = await compileModel();
  checkCubePii(Object.values(cubeEvaluator.evaluatedCubes));
  // The platform smoke check requires /meta to list at least one cube or view.
  const visible = metaTransformer.cubes
    .map((/** @type {{ config: any }} */ cube) => cube.config)
    .filter((/** @type {any} */ config) => config.isVisible);
  assert.ok(visible.length > 0, 'the model must expose at least one cube or view');
  /** @type {string[]} */
  const missing = [];
  for (const config of visible) {
    for (const item of [config, ...config.measures, ...config.dimensions]) {
      if (item.isVisible !== false && !String(item.description ?? '').trim()) missing.push(item.name);
    }
  }
  assert.deepEqual(missing, [], 'add a description to each public cube, view and member');
});
