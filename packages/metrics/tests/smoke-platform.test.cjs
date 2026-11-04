const { test } = require('node:test');
const assert = require('node:assert/strict');
const { createHmac } = require('node:crypto');
const { checkPlatform, cubeToken, readStates } = require('../smoke-platform.cjs');

/** @param {Record<string, unknown>} entry */
const loaded = entry => ({ loadStatus: 'LOADED', locationOrLoadError: { __typename: 'RepositoryLocation' }, ...entry });

const repositories = {
  __typename: 'RepositoryConnection',
  nodes: [{
    name: 'repo',
    location: { name: 'loc' },
    schedules: [{ name: 'refresh', defaultStatus: 'STOPPED', scheduleState: { status: 'RUNNING' } }],
    sensors: [{ name: 'watch', defaultStatus: 'RUNNING', sensorState: { status: 'RUNNING' } }],
  }],
};

/**
 * Answers each workspace query with the next entry list, repeating the last.
 * @param {Array<Array<Record<string, unknown>>>} workspaces
 * @param {{ status?: number, body?: unknown }} [meta]
 */
function fakePlatform(workspaces, meta = {}) {
  /** @type {Array<{ url: string, init: RequestInit | undefined }>} */
  const calls = [];
  let polls = 0;
  /** @type {typeof globalThis.fetch} */
  const fakeFetch = async (input, init) => {
    const url = String(input);
    calls.push({ url, init });
    if (url.endsWith('/graphql') && String(init?.body).includes('workspaceOrError')) {
      const entries = workspaces[Math.min(polls++, workspaces.length - 1)];
      return Response.json({ data: { workspaceOrError: { __typename: 'Workspace', locationEntries: entries } } });
    }
    if (url.endsWith('/graphql')) return Response.json({ data: { repositoriesOrError: repositories } });
    return Response.json(meta.body ?? { cubes: [{ name: 'orders' }] }, { status: meta.status ?? 200 });
  };
  return { fetch: fakeFetch, calls, sleep: async () => {}, secret: 'secret' };
}

test('passes when code locations load, states read, and Cube returns metadata', async () => {
  const platform = fakePlatform([[loaded({ name: 'a' }), loaded({ name: 'b' })]]);
  assert.equal(await checkPlatform(platform), '2 code locations loaded; 2 schedules and sensors read; Cube metadata lists 1 cubes');
  const meta = platform.calls.find(call => call.url.endsWith('/cubejs-api/v1/meta'));
  assert.match(new Headers(meta?.init?.headers).get('Authorization') ?? '', /^[\w-]+\.[\w-]+\.[\w-]+$/);
});

test('waits for code locations that are still loading', async () => {
  const platform = fakePlatform([
    [loaded({ name: 'a' }), { name: 'b', loadStatus: 'LOADING', locationOrLoadError: null }],
    [loaded({ name: 'a' }), loaded({ name: 'b' })],
  ]);
  await checkPlatform(platform);
});

test('fails when a code location is still loading at the deadline', async () => {
  const platform = fakePlatform([[{ name: 'slow', loadStatus: 'LOADING', locationOrLoadError: null }]]);
  await assert.rejects(checkPlatform({ ...platform, loadTimeoutMs: 0 }), /still loading.*slow/);
});

test('fails when a code location does not load', async () => {
  const platform = fakePlatform([[
    loaded({ name: 'a' }),
    loaded({ name: 'b', locationOrLoadError: { __typename: 'PythonError', message: 'ImportError: broken' } }),
  ]]);
  await assert.rejects(checkPlatform(platform), /b did not load: ImportError: broken/);
});

test('fails when no code location is registered', async () => {
  await assert.rejects(checkPlatform(fakePlatform([[]])), /no code locations/);
});

test('fails when Cube metadata lists no cubes', async () => {
  const platform = fakePlatform([[loaded({ name: 'a' })]], { body: { cubes: [] } });
  await assert.rejects(checkPlatform(platform), /no cubes/);
});

test('fails when Cube rejects the metadata request', async () => {
  const platform = fakePlatform([[loaded({ name: 'a' })]], { status: 403, body: { error: 'Invalid token' } });
  await assert.rejects(checkPlatform(platform), /403/);
});

test('reads schedule and sensor states with their code defaults', async () => {
  assert.deepEqual(await readStates(fakePlatform([[loaded({ name: 'a' })]])), {
    'loc/repo/schedule/refresh': { status: 'RUNNING', default: 'STOPPED' },
    'loc/repo/sensor/watch': { status: 'RUNNING', default: 'RUNNING' },
  });
});

test('signs an HS256 token that Cube can verify with the API secret', () => {
  const [header, payload, signature] = cubeToken('secret', 1000).split('.');
  assert.ok(header && payload && signature);
  assert.deepEqual(JSON.parse(Buffer.from(header, 'base64url').toString()), { alg: 'HS256', typ: 'JWT' });
  assert.deepEqual(JSON.parse(Buffer.from(payload, 'base64url').toString()), { iat: 1000, exp: 1300 });
  const expected = createHmac('sha256', 'secret').update(`${header}.${payload}`).digest('base64url');
  assert.equal(signature, expected);
});
