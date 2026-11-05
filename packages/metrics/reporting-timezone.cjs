// Reporting days use the client's reporting time zone, replacing any time
// zone a query sends. Every service reads it from the same client.yaml.
const { readFileSync } = require('node:fs');
const { isAbsolute } = require('node:path');
const { parse } = require('yaml');

const CONFIG_VARIABLE = 'ANALYTICS_CLIENT_CONFIG';
const DEFAULT_PATH = '/etc/analytics/client.yaml';

/**
 * Read `reporting_time_zone` from the client configuration file. This reads
 * only what Cube needs; the client-config package validates the whole file.
 * @param {Record<string, string | undefined>} env
 * @returns {string}
 */
function reportingTimeZone(env) {
  const path = env[CONFIG_VARIABLE]?.trim() || DEFAULT_PATH;
  if (!isAbsolute(path)) throw new Error(`${CONFIG_VARIABLE} must be an absolute path`);
  let text;
  try {
    text = readFileSync(path, 'utf8');
  } catch {
    throw new Error(`${path}: client configuration is unreadable; mount client.yaml there or set ${CONFIG_VARIABLE}`);
  }
  /** @type {unknown} */
  let document;
  try {
    document = parse(text);
  } catch {
    throw new Error(`${path}: file is not valid YAML`);
  }
  if (document === null || typeof document !== 'object' || Array.isArray(document)) {
    throw new Error(`${path}: file must be a mapping`);
  }
  const config = /** @type {Record<string, unknown>} */ (document);
  if (config.version !== 1) throw new Error(`${path}: version must be 1`);
  const zone = config.reporting_time_zone;
  if (typeof zone !== 'string' || !zone.trim()) throw new Error(`${path}: reporting_time_zone must be nonempty text`);
  try {
    new Intl.DateTimeFormat('en-US', { timeZone: zone });
  } catch {
    throw new Error(`${path}: reporting_time_zone must be an IANA time zone`);
  }
  return zone;
}

/** @param {string} timeZone @returns {import('@cubejs-backend/api-gateway').QueryRewriteFn} */
const queryRewrite = timeZone => async query => ({ ...query, timezone: timeZone });

module.exports = { reportingTimeZone, queryRewrite };
