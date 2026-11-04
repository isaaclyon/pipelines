// Published-mart helpers for Cube model files. Model files run in Cube's
// compiler sandbox, which has no `process`; they `require` this module through
// Node, so it can read the service environment.

/**
 * Demand-driven cache invalidation for every maintained cube over published marts.
 * @param {string} schema
 */
function publicationRefreshKey(schema) {
  if (!/^[a-z][a-z0-9_]*_marts$/.test(schema)) throw new Error('Invalid published marts schema');
  return { sql: () => `SELECT publication_id FROM ${schema}._publication`, every: '1 second' };
}

/**
 * The published marts this Cube process reads: backend from CUBEJS_DB_TYPE and
 * schema from ANALYTICS_MARTS_SCHEMA in the Snowflake connection's database.
 * @param {NodeJS.ProcessEnv} environment
 * @param {'orders' | 'customers'} group
 */
function publishedMarts(environment = process.env, group = 'orders') {
  const backend = environment.CUBEJS_DB_TYPE;
  const governedOrdersEnabled = governedOrderMetricsEnabled(environment);
  const localDuckDb = environment.ANALYTICS_ENVIRONMENT === 'local' && backend === 'duckdb';
  const independent = Boolean(environment.ANALYTICS_CUSTOMERS_MARTS_SCHEMA);
  if (independent && environment.ANALYTICS_CUSTOMERS_MARTS_SCHEMA === environment.ANALYTICS_MARTS_SCHEMA) {
    throw new Error('Publication groups require distinct schemas');
  }
  const schema = (group === 'customers' && independent
    ? environment.ANALYTICS_CUSTOMERS_MARTS_SCHEMA : environment.ANALYTICS_MARTS_SCHEMA) ?? '';
  if (backend !== 'snowflake' && !localDuckDb) throw new Error('Invalid publication backend');
  const refreshKey = publicationRefreshKey(schema);
  return {
    governedOrdersEnabled,
    customersEnabled: environment.SHOPIFY_CUSTOMERS_ENABLED === 'true',
    refreshKey,
    dataSource: group === 'customers' && independent ? 'customers' : 'default',
    // Legacy combined publications retain the hidden join during migration.
    // Independent publications do not promise a common customer/order version.
    customerJoinEnabled: !independent,
    /** @param {string} name */
    table(name) {
      if (name !== '_publication' && !/^[a-z][a-z0-9_]*$/.test(name)) throw new Error('Invalid mart name');
      return `${schema}.${name}`;
    },
  };
}

/** @param {NodeJS.ProcessEnv} environment */
function governedOrderMetricsEnabled(environment) {
  const flag = environment.GOVERNED_ORDER_METRICS_ENABLED ?? 'false';
  if (flag !== 'true' && flag !== 'false') {
    throw new Error('GOVERNED_ORDER_METRICS_ENABLED must be true or false');
  }
  if (flag === 'true' && (environment.ANALYTICS_ENVIRONMENT !== 'local' || environment.CUBEJS_DB_TYPE !== 'duckdb')) {
    throw new Error('Governed order metrics require explicit local DuckDB serving');
  }
  return flag === 'true';
}

module.exports = { publicationRefreshKey, publishedMarts, governedOrderMetricsEnabled };
