// Reporting days use the reporting time zone, replacing any client timezone.
// dbt dates facts with the same variable and default.
const REPORTING_TIMEZONE = process.env.ANALYTICS_REPORTING_TIME_ZONE || 'America/Los_Angeles';

/** @type {import('@cubejs-backend/api-gateway').QueryRewriteFn} */
const queryRewrite = async query => ({ ...query, timezone: REPORTING_TIMEZONE });

module.exports = { REPORTING_TIMEZONE, queryRewrite };
