const environment = process.env.ANALYTICS_ENVIRONMENT;

if (environment !== 'local' && environment !== 'ci' &&
    environment !== 'development' && environment !== 'production') {
  throw new Error('ANALYTICS_ENVIRONMENT must be local, ci, development, or production');
}
if (process.env.CUBEJS_DB_TYPE !== 'snowflake') {
  throw new Error('CUBEJS_DB_TYPE must be snowflake');
}

module.exports = /** @satisfies {import('@cubejs-backend/server-core').CreateOptions} */ ({
  schemaPath: 'model',
  queryRewrite: require('./reporting-timezone.cjs').queryRewrite,
});
