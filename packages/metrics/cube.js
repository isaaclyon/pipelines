const environment = process.env.ANALYTICS_ENVIRONMENT;
require('./publication.cjs').governedOrderMetricsEnabled(process.env);

if (environment !== 'local' && environment !== 'ci' &&
    environment !== 'development' && environment !== 'production') {
  throw new Error('ANALYTICS_ENVIRONMENT must be local, ci, development, or production');
}
if (process.env.CUBEJS_DB_TYPE !== 'snowflake') {
  throw new Error('CUBEJS_DB_TYPE must be snowflake');
}

// Customer PII lives in restricted warehouse relations. Activation must not
// leave Cube connected with the ingestion/transformation writer role. Partial
// reader migrations also fail while customer models remain disabled.
const readerConfigured = Boolean(process.env.ANALYTICS_MARTS_READER_ROLE ||
  process.env.ANALYTICS_MARTS_READER_KEY_HOST_PATH ||
  (process.env.CUBEJS_DB_USER && process.env.ANALYTICS_MARTS_WRITER_USER &&
    process.env.CUBEJS_DB_USER.toLowerCase() !== process.env.ANALYTICS_MARTS_WRITER_USER.toLowerCase()));
if (process.env.SHOPIFY_CUSTOMERS_ENABLED === 'true' || readerConfigured) {
  const reader = (process.env.ANALYTICS_MARTS_READER_ROLE || '').toLowerCase();
  const writer = (process.env.ANALYTICS_MARTS_WRITER_ROLE || '').toLowerCase();
  const effective = (process.env.CUBEJS_DB_SNOWFLAKE_ROLE || '').toLowerCase();
  if (!/^[a-z][a-z0-9_]{0,254}$/.test(reader) || !writer || reader === writer ||
      reader !== effective || ['public', 'accountadmin', 'sysadmin', 'securityadmin'].includes(reader)) {
    throw new Error('Customer analytics requires a separate restricted Snowflake reader role');
  }
  const readerUser = (process.env.CUBEJS_DB_USER || '').toLowerCase();
  const writerUser = (process.env.ANALYTICS_MARTS_WRITER_USER || '').toLowerCase();
  const path = require('node:path');
  const readerKey = process.env.ANALYTICS_MARTS_READER_KEY_HOST_PATH || '';
  const writerKey = process.env.ANALYTICS_MARTS_WRITER_KEY_HOST_PATH || '';
  if (!readerUser || !writerUser || readerUser === writerUser ||
      !path.isAbsolute(readerKey) || !path.isAbsolute(writerKey) ||
      path.normalize(readerKey) === path.normalize(writerKey) ||
      process.env.CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PATH !== '/run/secrets/snowflake_reader_private_key') {
    throw new Error('Customer analytics requires a separate Snowflake reader identity and key');
  }
}

module.exports = /** @satisfies {import('@cubejs-backend/server-core').CreateOptions} */ ({
  schemaPath: 'model',
  queryRewrite: require('./reporting-timezone.cjs').queryRewrite,
  ...(process.env.SHOPIFY_CUSTOMERS_ENABLED === 'true' ? {
    // Drop secondary roles as defense in depth for the dedicated reader user.
    driverFactory: (/** @type {{ dataSource?: string } | undefined} */ context) => {
      const { SnowflakeReaderDriver } = require('./snowflake-reader-driver.cjs');
      // The vendor driver's own typings fail this repository's exactOptionalPropertyTypes
      // check against BaseDriver; the runtime class extends the same BaseDriver.
      return /** @type {import('@cubejs-backend/base-driver').BaseDriver} */ (
        /** @type {unknown} */ (new SnowflakeReaderDriver(context?.dataSource && context.dataSource !== 'customers' ? { dataSource: context.dataSource } : {})));
    },
  } : {}),
});
