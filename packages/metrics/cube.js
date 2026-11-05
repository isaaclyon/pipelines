const environment = process.env.ANALYTICS_ENVIRONMENT;

if (environment !== 'local' && environment !== 'ci' &&
    environment !== 'development' && environment !== 'production') {
  throw new Error('ANALYTICS_ENVIRONMENT must be local, ci, development, or production');
}
if (process.env.CUBEJS_DB_TYPE !== 'snowflake') {
  throw new Error('CUBEJS_DB_TYPE must be snowflake');
}

const { queryRewrite, reportingTimeZone } = require('./reporting-timezone.cjs');
const timeZone = reportingTimeZone(process.env);
const model = require('./model-repository.cjs').loadModel();

module.exports = /** @satisfies {import('@cubejs-backend/server-core').CreateOptions} */ ({
  // The toolkit model and the client model, read and checked once at startup.
  repositoryFactory: () => model,
  queryRewrite: queryRewrite(timeZone),
});
