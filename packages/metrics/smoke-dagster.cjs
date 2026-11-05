const assert = require('node:assert/strict');

/** @param {unknown} value @returns {Record<string, unknown>} */
function object(value) {
  assert.ok(value !== null && typeof value === 'object' && !Array.isArray(value));
  return /** @type {Record<string, unknown>} */ (value);
}

/** @param {string} query @param {Record<string, unknown>} variables */
async function graphql(query, variables = {}) {
  const response = await fetch('http://webserver:3000/graphql', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, variables }),
    signal: AbortSignal.timeout(15000),
  });
  assert.equal(response.status, 200);
  const result = object(await response.json());
  assert.equal(result.errors, undefined, JSON.stringify(result.errors));
  return object(result.data);
}

async function main() {
  const data = await graphql(`{
    repositoriesOrError {
      __typename
      ... on RepositoryConnection { nodes { name location { name } } }
      ... on PythonError { message }
    }
  }`);
  const connection = object(data.repositoriesOrError);
  assert.equal(connection.__typename, 'RepositoryConnection', JSON.stringify(connection));
  assert.ok(Array.isArray(connection.nodes));
  const repositories = connection.nodes.map((/** @type {unknown} */ value) => {
    const repository = object(value);
    const location = object(repository.location);
    assert.equal(typeof repository.name, 'string');
    assert.equal(typeof location.name, 'string');
    return { name: String(repository.name), location: { name: String(location.name) } };
  });
  assert.deepEqual(repositories.map(r => r.location.name).sort(), ['ingestion', 'transformations']);

  for (const repository of repositories) {
    const runId = await runJob(repository, 'scaffold_check');
    console.log(`${repository.location.name}: isolated scaffold_check run ${runId} succeeded`);
  }

  const transformations = repositories.find(r => r.location.name === 'transformations');
  assert.ok(transformations);
  const docsRun = await runJob(transformations, 'dbt_docs');
  await checkDocsSite();
  console.log(`transformations: dbt_docs run ${docsRun} built the password-protected docs and lineage pages`);
}

/**
 * Launch a job and wait for it to succeed.
 * @param {{ name: string, location: { name: string } }} repository @param {string} jobName
 */
async function runJob(repository, jobName) {
  const launch = await graphql(`mutation Launch($params: ExecutionParams!) {
    launchRun(executionParams: $params) {
      __typename
      ... on LaunchRunSuccess { run { runId } }
      ... on PythonError { message }
    }
  }`, { params: { selector: {
    repositoryLocationName: repository.location.name,
    repositoryName: repository.name,
    jobName,
  }, runConfigData: {} } });
  const launchRun = object(launch.launchRun);
  assert.equal(launchRun.__typename, 'LaunchRunSuccess', JSON.stringify(launch));
  const runId = object(launchRun.run).runId;
  assert.equal(typeof runId, 'string');
  const deadline = Date.now() + 150000;
  let status;
  while (Date.now() < deadline) {
    const result = await graphql(`query Run($id: ID!) {
      runOrError(runId: $id) { __typename ... on Run { status } }
    }`, { id: runId });
    const run = object(result.runOrError);
    assert.equal(run.__typename, 'Run');
    status = run.status;
    assert.ok(typeof status === 'string');
    if (['SUCCESS', 'FAILURE', 'CANCELED'].includes(status)) break;
    await new Promise(resolve => setTimeout(resolve, 2000));
  }
  assert.equal(status, 'SUCCESS', `${repository.location.name} ${jobName} run ${runId}: ${status}`);
  return runId;
}

/** The docs service requires the password and serves the pages the job wrote. */
async function checkDocsSite() {
  const password = process.env.DOCS_SMOKE_PASSWORD;
  assert.ok(password, 'Set DOCS_SMOKE_PASSWORD to the fixture password');
  /** @param {string} path @param {string | undefined} credentials */
  const get = (path, credentials) => fetch(`http://docs:8080${path}`, {
    headers: credentials ? { Authorization: `Basic ${Buffer.from(credentials).toString('base64')}` } : {},
    signal: AbortSignal.timeout(15000),
  });
  for (const path of ['/', '/lineage/']) {
    assert.equal((await get(path, undefined)).status, 401);
    assert.equal((await get(path, 'analytics:wrong-password')).status, 401);
    const page = await get(path, `analytics:${password}`);
    assert.equal(page.status, 200);
    assert.equal(page.headers.get('server'), null);
    // The lineage page carries third-party analytics code; the policy blocks
    // every connection it could make even if its opt-out were ignored.
    assert.match(page.headers.get('content-security-policy') ?? '', /connect-src 'none'/);
    assert.match(await page.text(), path === '/' ? /stg_shopify_orders__orders/ : /colibriTelemetry = false/);
  }
}

main().catch(error => { console.error(error); process.exitCode = 1; });
