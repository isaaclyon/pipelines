// Explicit development-only administrator fixture. Never runs in ordinary checks.
const assert = require('node:assert/strict');
const { generateKeyPairSync, randomBytes } = require('node:crypto');
const { mkdir, writeFile, unlink } = require('node:fs/promises');
const { spawnSync } = require('node:child_process');
const { SnowflakeDriver } = require('@cubejs-backend/snowflake-driver');

async function main() {
  assert.equal(process.env.CUBE_READER_ACCEPTANCE, 'development');
  assert.equal(process.env.ANALYTICS_ENVIRONMENT, 'development');
  const database = process.env.CUBEJS_DB_NAME || '';
  const warehouse = process.env.CUBEJS_DB_SNOWFLAKE_WAREHOUSE || '';
  assert.match(database, /^[A-Z][A-Z0-9_]*$/);
  assert.match(warehouse, /^[A-Z][A-Z0-9_]*$/);
  const prefix = `CUBE_READER_${randomBytes(6).toString('hex').toUpperCase()}`;
  const schema = `${prefix}_MARTS`;
  const user = `${prefix}_USER`;
  const role = `${prefix}_ROLE`;
  const writer = `${prefix}_WRITER`;
  const keyPath = '/run/secrets/snowflake_reader_private_key';
  const admin = new SnowflakeDriver({ dataSource: 'default', role: 'SYSADMIN', executionTimeout: 30 });
  /** @type {Set<string>} */
  const created = new Set();
  /** @param {string} sql */
  const sql = (sql) => admin.query(sql, []);
  /** @type {Error[]} */
  const cleanupErrors = [];
  try {
    const identity = await sql('SELECT CURRENT_USER() AS ADMIN_USER, CURRENT_ROLE() AS ADMIN_ROLE, CURRENT_WAREHOUSE() AS ADMIN_WH, CURRENT_DATABASE() AS ADMIN_DB');
    assert.ok(Array.isArray(identity) && identity.length === 1);
    /** @type {unknown} */
    const context = identity[0];
    assert.ok(context && typeof context === 'object' &&
      'ADMIN_ROLE' in context && 'ADMIN_WH' in context && 'ADMIN_DB' in context && 'ADMIN_USER' in context);
    assert.equal(context.ADMIN_ROLE, 'SYSADMIN');
    assert.equal(context.ADMIN_WH, warehouse);
    assert.equal(context.ADMIN_DB, database);
    assert.ok(typeof context.ADMIN_USER === 'string' && context.ADMIN_USER.length > 0);
    console.log(`Verified individual administrator development context. Fixture prefix: ${prefix}`);
    const { privateKey, publicKey } = generateKeyPairSync('rsa', {
      modulusLength: 2048,
      privateKeyEncoding: { type: 'pkcs8', format: 'pem' },
      publicKeyEncoding: { type: 'spki', format: 'der' },
    });
    await sql('USE ROLE SECURITYADMIN');
    await sql(`CREATE ROLE ${role}`); created.add('role');
    await sql(`CREATE ROLE ${writer}`); created.add('writer');
    await sql(`CREATE USER ${user} TYPE=SERVICE RSA_PUBLIC_KEY='${publicKey.toString('base64')}' DEFAULT_ROLE=${role} DEFAULT_WAREHOUSE=${warehouse}`);
    created.add('user');
    await sql(`GRANT ROLE ${role} TO USER ${user}`);
    for (const target of [role, writer]) {
      await sql(`GRANT USAGE ON DATABASE ${database} TO ROLE ${target}`);
      await sql(`GRANT USAGE ON WAREHOUSE ${warehouse} TO ROLE ${target}`);
    }
    await sql('USE ROLE SYSADMIN');
    await sql(`CREATE SCHEMA ${schema}`); created.add('schema');
    await sql(`CREATE TABLE ${schema}.dim_customers (id INTEGER)`);
    await sql(`INSERT INTO ${schema}.dim_customers VALUES (1),(2)`);
    for (const table of ['dim_customer_details', 'dim_order_contacts', 'raw_customers']) {
      await sql(`CREATE TABLE ${schema}.${table} (private_value VARCHAR)`);
      await sql(`INSERT INTO ${schema}.${table} VALUES ('synthetic@example.invalid')`);
    }
    await sql(`GRANT ALL PRIVILEGES ON SCHEMA ${schema} TO ROLE ${writer}`);
    await sql(`GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ${schema} TO ROLE ${writer}`);
    await sql(`GRANT USAGE ON SCHEMA ${schema} TO ROLE ${role}`);
    await sql(`GRANT SELECT ON TABLE ${schema}.dim_customers TO ROLE ${role}`);
    await mkdir('/run/secrets', { recursive: true });
    // Exclusive creation refuses an existing key mount or credential file.
    await writeFile(keyPath, privateKey, { mode: 0o600, flag: 'wx' }); created.add('key');
    const result = spawnSync(process.execPath, ['--test', 'tests/snowflake-reader-acceptance.test.cjs'], {
      timeout: 90000,
      stdio: 'inherit',
      env: {
        ...process.env,
        SHOPIFY_CUSTOMERS_ENABLED: 'true',
        CUBEJS_DB_USER: user, CUBEJS_DB_SNOWFLAKE_ROLE: role,
        CUBEJS_DB_SNOWFLAKE_AUTHENTICATOR: 'SNOWFLAKE_JWT',
        CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PATH: keyPath,
        // Prevent an administrator's inline key or password from reaching Cube.
        CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY: '', CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PASS: '', CUBEJS_DB_PASS: '',
        ANALYTICS_MARTS_READER_ROLE: role,
        ANALYTICS_MARTS_WRITER_ROLE: writer,
        ANALYTICS_MARTS_WRITER_USER: context.ADMIN_USER,
        ANALYTICS_MARTS_WRITER_KEY_HOST_PATH: '/individual-administrator-key',
        ANALYTICS_MARTS_READER_KEY_HOST_PATH: '/temporary-disposable-reader-key',
        CUBE_READER_FIXTURE_SCHEMA: schema,
      },
    });
    assert.equal(result.status, 0, 'Dedicated reader acceptance failed');
  } finally {
    // Attempt every cleanup even if one object cannot be removed.
    for (const [kind, statement] of [
      ['schema', `USE ROLE SYSADMIN`],
      ['schema', `DROP SCHEMA ${schema} CASCADE`],
      ['role', 'USE ROLE SECURITYADMIN'],
      ['user', `DROP USER ${user}`],
      ['role', `DROP ROLE ${role}`],
      ['writer', `DROP ROLE ${writer}`],
    ]) {
      if (!kind || !statement || !created.has(kind)) continue;
      try { await sql(statement); } catch (error) {
        cleanupErrors.push(error instanceof Error ? error : new Error(String(error)));
      }
    }
    if (created.has('key')) await unlink(keyPath);
    await admin.release();
    if (cleanupErrors.length) throw new AggregateError(cleanupErrors, `Fixture cleanup failed: ${prefix}`);
    console.log(`Disposable reader user, roles, schema and key removed: ${prefix}`);
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
