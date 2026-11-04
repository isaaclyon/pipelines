// @ts-nocheck Cube model DSL globals are provided by the schema compiler.
const { publishedMarts } = require('../publication.cjs');
const marts = publishedMarts();

cube('published_data_availability', {
  // A malformed marker yields no status, never a certified answer. This scalar
  // validation does not join source status or marker rows to order facts.
  sql: `SELECT * FROM ${marts.table('fct_data_availability')}
    WHERE publication_id = (
      SELECT CASE WHEN COUNT(*) = 1 AND COUNT(published_at) = 1
        THEN NULLIF(TRIM(MAX(publication_id)), '') ELSE NULL END
      FROM ${marts.table('_publication')}
    )`,
  refreshKey: marts.refreshKey,
  public: false,
  dimensions: {
    availability_id: {
      meta: { pii: false },
      sql: 'availability_id', type: 'string', primaryKey: true, public: false,
      description: 'Shop-scoped availability record, or initial unknown record',
    },
    publication_id: {
      meta: { pii: false },
      sql: 'publication_id', type: 'string',
      description: 'Optional diagnostic generation stored with this source evidence; not required for consumer queries',
    },
    shopify_shop_id: {
      meta: { pii: false },
      sql: 'shopify_shop_id', type: 'string', description: 'Source shop identity; null before any shop evidence exists',
    },
    environment: {
      meta: { pii: false },
      sql: 'environment', type: 'string', description: 'Explicit environment of the candidate that published these facts and evidence',
    },
    ...Object.fromEntries([
      ['evidence_captured_at', 'UTC candidate capture time, not source freshness or final publication time', false],
      ['shopify_coverage_started_at', 'Inclusive UTC start of the latest connected verified updated-at interval; earlier gaps remain uncovered', false],
      ['shopify_coverage_ended_at', 'Exclusive UTC end of the verified updated-at interval; null without evidence', false],
      ['loop_snapshot_ended_at', 'Latest Loop snapshot extraction endpoint, even if incomplete; not a vendor updated-at guarantee', false],
    ].map(([name, description, pii]) => [name, { meta: { pii }, sql: name, type: 'time', description }])),
    is_loop_snapshot_complete: {
      meta: { pii: false },
      sql: 'is_loop_snapshot_complete', type: 'boolean',
      description: 'Latest Loop evidence matches a successful load and current header counts; false when missing or disabled',
    },
    is_loop_enabled: {
      meta: { pii: false },
      sql: 'is_loop_enabled', type: 'boolean', description: 'Whether this candidate reads the maintained Loop header feed',
    },
    coverage_limitations: {
      meta: { pii: false },
      sql: 'coverage_limitations', type: 'string', description: 'Known limitations; freshness alone does not establish metric completeness',
    },
  },
});

view('data_availability', {
  description: 'Source evidence captured at warehouse publication. Queries are eventually consistent; this evidence does not certify separately queried revenue. Assess evidence age at query time. Missing status is not zero or a successful publication.',
  cubes: [{ joinPath: published_data_availability, includes: '*' }],
});
