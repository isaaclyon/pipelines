// Read-only platform smoke check for every target (ADR 0019). It launches no
// runs and queries no warehouse, so remote deployments can run it safely.
// `--states` prints schedule and sensor states for the deploy command. A newer
// release compares against an older release's output, so keep its shape stable.
const { createHmac } = require('node:crypto');

/**
 * @typedef {{ fetch: typeof fetch, sleep?: (ms: number) => Promise<void>, loadTimeoutMs?: number }} Dagster
 * @typedef {Record<string, { status: string, default: string }>} States
 */

/** @param {unknown} value @returns {Record<string, unknown>} */
function object(value) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(`Expected an object, got ${JSON.stringify(value)}`);
  }
  return /** @type {Record<string, unknown>} */ (value);
}

/** @param {unknown} value @returns {unknown[]} */
function array(value) {
  if (!Array.isArray(value)) throw new Error(`Expected a list, got ${JSON.stringify(value)}`);
  return value;
}

/** @param {unknown} value @returns {string} */
function string(value) {
  if (typeof value !== 'string') throw new Error(`Expected a string, got ${JSON.stringify(value)}`);
  return value;
}

/** @param {unknown} value */
function base64url(value) {
  return Buffer.from(JSON.stringify(value)).toString('base64url');
}

/**
 * Cube accepts HS256 tokens signed with CUBEJS_API_SECRET.
 * @param {string} secret @param {number} nowSeconds
 */
function cubeToken(secret, nowSeconds) {
  const unsigned = `${base64url({ alg: 'HS256', typ: 'JWT' })}.${base64url({ iat: nowSeconds, exp: nowSeconds + 300 })}`;
  return `${unsigned}.${createHmac('sha256', secret).update(unsigned).digest('base64url')}`;
}

/** @param {typeof fetch} fetcher @param {string} query */
async function graphql(fetcher, query) {
  const response = await fetcher('http://webserver:3000/graphql', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(15000),
  });
  if (!response.ok) throw new Error(`Dagster GraphQL returned ${response.status}`);
  return object(object(await response.json()).data);
}

/**
 * The webserver reports healthy before its workspace finishes loading.
 * @param {Dagster} dagster
 */
async function waitForCodeLocations({ fetch: fetcher, sleep = ms => new Promise(r => setTimeout(r, ms)), loadTimeoutMs = 60000 }) {
  const deadline = Date.now() + loadTimeoutMs;
  for (;;) {
    const workspace = object((await graphql(fetcher, `{ workspaceOrError { __typename
      ... on Workspace { locationEntries { name loadStatus
        locationOrLoadError { __typename ... on PythonError { message } } } }
      ... on PythonError { message } } }`)).workspaceOrError);
    if (workspace.__typename !== 'Workspace') throw new Error(`Dagster workspace: ${JSON.stringify(workspace)}`);
    const entries = array(workspace.locationEntries).map(object);
    if (entries.length === 0) throw new Error('Dagster has no code locations');
    const loading = entries.filter(entry => entry.loadStatus !== 'LOADED').map(entry => String(entry.name));
    if (loading.length === 0) {
      for (const entry of entries) {
        const location = entry.locationOrLoadError === null ? {} : object(entry.locationOrLoadError);
        if (location.__typename !== 'RepositoryLocation') {
          throw new Error(`Code location ${String(entry.name)} did not load: ${String(location.message)}`);
        }
      }
      return entries.length;
    }
    if (Date.now() >= deadline) throw new Error(`Code locations still loading: ${loading.join(', ')}`);
    await sleep(2000);
  }
}

/** @param {Dagster} dagster @returns {Promise<States>} */
async function readStates(dagster) {
  await waitForCodeLocations(dagster);
  const connection = object((await graphql(dagster.fetch, `{ repositoriesOrError { __typename
    ... on RepositoryConnection { nodes { name location { name }
      schedules { name defaultStatus scheduleState { status } }
      sensors { name defaultStatus sensorState { status } } } } } }`)).repositoriesOrError);
  if (connection.__typename !== 'RepositoryConnection') throw new Error(`Dagster repositories: ${JSON.stringify(connection)}`);
  /** @type {States} */
  const states = {};
  for (const repository of array(connection.nodes).map(object)) {
    const prefix = `${string(object(repository.location).name)}/${string(repository.name)}`;
    for (const kind of ['schedule', 'sensor']) {
      for (const item of array(repository[`${kind}s`]).map(object)) {
        states[`${prefix}/${kind}/${string(item.name)}`] = {
          status: string(object(item[`${kind}State`]).status),
          default: string(item.defaultStatus),
        };
      }
    }
  }
  return states;
}

/** @param {typeof fetch} fetcher @param {string} secret */
async function checkCubeMetadata(fetcher, secret) {
  const response = await fetcher('http://localhost:4000/cubejs-api/v1/meta', {
    headers: { Authorization: cubeToken(secret, Math.floor(Date.now() / 1000)) },
    signal: AbortSignal.timeout(15000),
  });
  if (!response.ok) throw new Error(`Cube metadata returned ${response.status}`);
  // A model that fails to load or is missing leaves no public cubes or views.
  const cubes = array(object(await response.json()).cubes).length;
  if (cubes === 0) throw new Error('Cube metadata lists no cubes or views');
  return cubes;
}

/** @param {Dagster & { secret: string }} options */
async function checkPlatform(options) {
  const locations = await waitForCodeLocations(options);
  const states = Object.keys(await readStates(options)).length;
  const cubes = await checkCubeMetadata(options.fetch, options.secret);
  return `${locations} code locations loaded; ${states} schedules and sensors read; Cube metadata lists ${cubes} cubes`;
}

module.exports = { checkPlatform, cubeToken, readStates };

if (require.main === module) {
  const run = process.argv.includes('--states')
    ? readStates({ fetch }).then(states => JSON.stringify(states))
    : checkPlatform({ fetch, secret: string(process.env.CUBEJS_API_SECRET) });
  run.then(output => console.log(output)).catch(error => { console.error(error); process.exitCode = 1; });
}
