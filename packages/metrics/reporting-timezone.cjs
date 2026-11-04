// Revenue days use the reporting time zone; availability instants must remain
// unambiguous UTC. Replace client timezones for both surfaces. They must be
// queried separately. dbt dates orders with the same variable and default.
const REPORTING_TIMEZONE = process.env.ANALYTICS_REPORTING_TIME_ZONE || 'America/Los_Angeles';

/** @type {import('@cubejs-backend/api-gateway').QueryRewriteFn} */
const queryRewrite = async query => {
  const members = [
    ...(query.measures ?? []), ...(query.dimensions ?? []),
    ...(query.timeDimensions ?? []).map(time => time.dimension),
    ...(query.segments ?? []),
  ];
  const availability = members.filter(member => typeof member === 'string' && member.startsWith('data_availability.'));
  if (availability.length > 0 && availability.length !== members.length) {
    throw new Error('Query data_availability separately from reporting facts');
  }
  return { ...query, timezone: availability.length > 0 ? 'UTC' : REPORTING_TIMEZONE };
};

module.exports = { REPORTING_TIMEZONE, queryRewrite };
