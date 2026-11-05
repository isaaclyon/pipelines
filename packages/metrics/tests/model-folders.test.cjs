const { test } = require('node:test');
const assert = require('node:assert/strict');
const { mkdirSync, mkdtempSync, writeFileSync } = require('node:fs');
const { tmpdir } = require('node:os');
const { dirname, join } = require('node:path');
const { checkModel, loadModel, readModel } = require('../model-repository.cjs');

const TOOLKIT = join(__dirname, '..', 'model');

/** @param {Record<string, string>} files @returns {string} */
function folder(files) {
  const root = mkdtempSync(join(tmpdir(), 'metrics-model-'));
  for (const [name, content] of Object.entries(files)) {
    mkdirSync(dirname(join(root, name)), { recursive: true });
    writeFileSync(join(root, name), content);
  }
  return root;
}

/** @param {string} name */
const cubeYaml = name => `cubes:
  - name: ${name}
    sql: SELECT 1 AS id
    description: Test cube
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
        meta: {pii: false}
        description: Test identifier
    measures:
      - name: count
        type: count
        meta: {pii: false}
        description: Number of test rows
`;

/** @param {string} name */
const viewYaml = name => `views:
  - name: ${name}
    description: Test view
    cubes:
      - join_path: placeholder
        includes: [count]
`;

test('client model files load beside the toolkit model', { timeout: 60000 }, async () => {
  const client = folder({ 'cubes/client_orders.yml': cubeYaml('client_orders') });
  const model = readModel(TOOLKIT, client);
  assert.deepEqual(model.files.map(file => file.fileName), ['placeholder.js', join('cubes', 'client_orders.yml')]);
  const { metaTransformer } = await checkModel(model);
  const names = metaTransformer.cubes.map((/** @type {{ config: { name: string } }} */ cube) => cube.config.name).sort();
  assert.deepEqual(names, ['client_orders', 'placeholder']);
});

test('a missing client folder adds no models', () => {
  const model = readModel(TOOLKIT, join(folder({}), 'model'));
  assert.deepEqual(model.files.map(file => file.fileName), ['placeholder.js']);
});

test('a client file with a toolkit file path fails, naming both files', () => {
  const client = folder({ 'placeholder.js': "cube('client_placeholder', { sql: 'SELECT 1' });" });
  assert.throws(() => readModel(TOOLKIT, client), error => {
    assert.ok(error instanceof Error);
    assert.ok(error.message.includes(join(TOOLKIT, 'placeholder.js')), error.message);
    assert.ok(error.message.includes(join(client, 'placeholder.js')), error.message);
    return true;
  });
});

for (const kind of ['cube', 'view']) {
  test(`a ${kind} name defined in both folders fails, naming both files`, { timeout: 60000 }, async () => {
    const toolkit = folder({
      'placeholder.yml': cubeYaml('placeholder'),
      'views/shared.yml': viewYaml('shared_view'),
    });
    const name = kind === 'cube' ? 'placeholder' : 'shared_view';
    const client = folder({ 'client/extra.yml': kind === 'cube' ? cubeYaml(name) : viewYaml(name) });
    const toolkitFile = join(toolkit, kind === 'cube' ? 'placeholder.yml' : join('views', 'shared.yml'));
    await assert.rejects(checkModel(readModel(toolkit, client)), error => {
      assert.ok(error instanceof Error);
      assert.match(error.message, new RegExp(`${kind} ${name} is defined more than once`));
      assert.ok(error.message.includes(toolkitFile), error.message);
      assert.ok(error.message.includes(join(client, 'client', 'extra.yml')), error.message);
      return true;
    });
  });
}

test('startup stops when a client cube repeats a toolkit cube name', { timeout: 60000 }, () => {
  const client = folder({ 'extra.yml': cubeYaml('placeholder') });
  assert.throws(() => loadModel(TOOLKIT, client), error => {
    assert.ok(error instanceof Error);
    assert.match(error.message, /cube placeholder is defined more than once/);
    assert.ok(error.message.includes(join(TOOLKIT, 'placeholder.js')), error.message);
    assert.ok(error.message.includes(join(client, 'extra.yml')), error.message);
    return true;
  });
});

test('startup serves the checked toolkit and client files', { timeout: 60000 }, async () => {
  const client = folder({ 'client_orders.yml': cubeYaml('client_orders') });
  const repository = loadModel(TOOLKIT, client);
  const files = await repository.dataSchemaFiles();
  assert.deepEqual(files.map(file => file.fileName), ['placeholder.js', 'client_orders.yml']);
});
