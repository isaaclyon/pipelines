// Generate the dbt exposures that continue dbt lineage from the published marts
// to the public Cube views. The transformations project checks in the output;
// tests/exposures.test.cjs fails when it differs from the current model.
//
//   node exposures.cjs > ../transformations/dbt/models/exposures/_cube__exposures.yml
//
// Each public view becomes one exposure. Its description maps every view field
// to the mart columns its SQL reads; `meta.cube_fields` repeats the map so dbt
// checks can confirm each column exists. The model compiles offline for each
// customer/governed-order flag combination. Distinct definitions receive gated
// exposures; governed-order consumers are enabled only for explicit local use.

const { readdir, readFile } = require('node:fs/promises');
const { join } = require('node:path');

const MODEL = join(__dirname, 'model');
// A sentinel schema: compiled table names are matched against it.
const SCHEMA = 'exposure_marts';
const OWNER = 'Analytics Platform';
const CUSTOMERS = "env_var('SHOPIFY_CUSTOMERS_ENABLED', 'false') == 'true'";
const GOVERNED = "(env_var('ANALYTICS_ENVIRONMENT', '') == 'local' and env_var('GOVERNED_ORDER_METRICS_ENABLED', 'false') == 'true')";

/**
 * @typedef {{ field: string, kind: string, columns: string[], description: string, pii: boolean, directColumn: string | null }} Field
 * @typedef {{ view: string, description: string, marts: string[], sources: Record<string, string>, fields: Field[], isInventory?: boolean }} Exposure
 */

/**
 * Compile public views and a separate inventory of all cube dimensions.
 * @param {boolean} customersEnabled
 * @param {string} [modelDirectory] Model directory; tests supply isolated fixtures.
 * @param {boolean} [governedOrdersEnabled]
 * @returns {Promise<Exposure[]>}
 */
async function compileExposures(customersEnabled, modelDirectory = MODEL, governedOrdersEnabled = false) {
  const { prepareCompiler } = require('@cubejs-backend/schema-compiler');
  // Model files read their configuration through publication.cjs at compile time.
  const saved = { ...process.env };
  Object.assign(process.env, {
    CUBEJS_DB_TYPE: governedOrdersEnabled ? 'duckdb' : 'snowflake', ANALYTICS_MARTS_SCHEMA: SCHEMA,
    ANALYTICS_ENVIRONMENT: governedOrdersEnabled ? 'local' : 'ci',
    GOVERNED_ORDER_METRICS_ENABLED: String(governedOrdersEnabled),
    SHOPIFY_CUSTOMERS_ENABLED: String(customersEnabled),
  });
  try {
    const files = (await readdir(modelDirectory)).filter(name => name.endsWith('.js')).sort();
    const contents = await Promise.all(files.map(name => readFile(join(modelDirectory, name), 'utf8')));
    const repository = {
      localPath: () => modelDirectory,
      dataSchemaFiles: async () => files.map((name, index) => ({ fileName: `model/${name}`, content: contents[index] ?? '' })),
    };
    const { compiler, cubeEvaluator, metaTransformer } = prepareCompiler(repository, { allowNodeRequire: true, standalone: true });
    await compiler.compile();
    checkCubePii(Object.values(cubeEvaluator.evaluatedCubes));
    // Cube's /meta decides which views and members are public.
    const views = metaTransformer.cubes
      .map((/** @type {{ config: any }} */ cube) => cube.config)
      .filter((/** @type {any} */ config) => config.type === 'view' && config.isVisible)
      .map((/** @type {any} */ config) => describeMembers(cubeEvaluator, config))
      .sort((/** @type {Exposure} */ a, /** @type {Exposure} */ b) => a.view.localeCompare(b.view));
    // Public-view documentation is not a complete security inventory: hidden
    // or currently unused aliases still need to agree with their mart columns.
    const dimensions = Object.values(cubeEvaluator.evaluatedCubes)
      .filter((/** @type {any} */ cube) => !cube.isView)
      .flatMap((/** @type {any} */ cube) => Object.entries(cube.dimensions ?? {})
        .map(([name, member]) => ({
          name: `${cube.name}.${name}`, aliasMember: `${cube.name}.${name}`,
          description: /** @type {any} */ (member).description, isVisible: true,
        })));
    const inventory = describeMembers(cubeEvaluator, {
      name: 'model_classifications',
      description: 'Classification validation for all Cube dimensions, including hidden and unused aliases. This inventory is not a public query interface.',
      dimensions, measures: [],
    });
    return [...views, { ...inventory, isInventory: true }];
  } finally {
    for (const key of Object.keys(process.env)) if (!(key in saved)) delete process.env[key];
    Object.assign(process.env, saved);
  }
}

/**
 * The one mart a cube reads, and its source SQL without the schema.
 * @param {any} cube
 */
function cubeSource(cube) {
  const sql = String(cube.sqlTable ? cube.sqlTable() : cube.sql()).replace(/\s+/g, ' ').trim();
  const marts = [...sql.matchAll(new RegExp(`\\b${SCHEMA}\\.([a-z][a-z0-9_]*)`, 'g'))].map(match => match[1] ?? '');
  if (new Set(marts).size !== 1 || !marts[0]) {
    throw new Error(`${cube.name}: read exactly one published mart through publishedMarts().table()`);
  }
  return { mart: marts[0], sql: sql.replaceAll(`${SCHEMA}.`, '') };
}

/**
 * Mart columns a member's SQL reads. Members must name columns directly or
 * as ${CUBE}.column, so the map is exact rather than guessed.
 * @param {any} cubeEvaluator
 * @param {string} cube
 * @param {any} member
 * @param {string[]} [visited]
 */
function memberColumns(cubeEvaluator, cube, member, visited = []) {
  if (!member.sql) return { columns: [], direct: false };
  const args = cubeEvaluator.funcArguments(member.sql).map((/** @type {string} */ id) => `{${id.trim()}}`);
  /** @type {string[]} */
  const referencedColumns = [];
  const sql = String(member.sql(...args)).replace(/\{([a-z_][a-z0-9_]*)\}/g, (_match, name) => {
    const referenced = cubeEvaluator.cubeFromPath(cube).measures?.[name];
    if (!referenced || visited.includes(name)) throw new Error(`${cube}: invalid or cyclic measure reference ${name}`);
    referencedColumns.push(...memberColumns(cubeEvaluator, cube, referenced, [...visited, name]).columns);
    for (const filter of referenced.filters ?? []) {
      referencedColumns.push(...memberColumns(cubeEvaluator, cube, filter, [...visited, name]).columns);
    }
    return '0';
  });
  if (/^[a-z_][a-z0-9_]*$/.test(sql)) return { columns: [sql], direct: true };
  const columns = [...referencedColumns, ...[...sql.matchAll(/\{CUBE\}\.([a-z_][a-z0-9_]*)/g)].map(match => match[1] ?? '')];
  const rest = sql.replace(/'(?:[^']|'')*'/g, '').replace(/\{CUBE\}\.[a-z_][a-z0-9_]*/g, '')
    .replace(/\b(?:IS|NOT|NULL|TRUE|FALSE|AND|OR|NULLIF)\b/gi, '');
  if (!columns.length || /[a-z_{]/i.test(rest)) {
    throw new Error(`${cube}: write member SQL as a column name or with \${CUBE}.column references: ${sql}`);
  }
  return { columns: [...new Set(columns)], direct: /^\{CUBE\}\.[a-z_][a-z0-9_]*$/.test(sql.trim()) };
}

/**
 * @param {any} cubeEvaluator
 * @param {any} view Member selection from a public view or the complete internal inventory.
 * @returns {Exposure}
 */
function describeMembers(cubeEvaluator, view) {
  /** @type {Record<string, string>} */
  const sources = {};
  /** @type {Field[]} */
  const fields = [];
  for (const group of ['measures', 'dimensions']) {
    for (const member of view[group]) {
      if (!member.isVisible) continue;
      if (!member.aliasMember) throw new Error(`${member.name}: views must include cube members`);
      const [cubeName = '', memberName = ''] = String(member.aliasMember).split('.');
      const cube = cubeEvaluator.cubeFromPath(cubeName);
      const source = cubeSource(cube);
      sources[source.mart] = source.sql;
      const original = cube[group][memberName];
      const references = memberColumns(cubeEvaluator, cubeName, original);
      const filterColumns = (original.filters ?? [])
        .flatMap((/** @type {any} */ member) => memberColumns(cubeEvaluator, cubeName, member).columns);
      const columns = [...new Set([...references.columns, ...filterColumns])]
        .map(column => `${source.mart}.${column}`);
      const directColumn = group === 'dimensions' && references.direct ? columns[0] ?? null : null;
      const kind = group === 'measures' ? `${original.type} measure` : `${original.type} dimension`;
      fields.push({ field: member.name, kind, columns, description: String(member.description ?? ''), pii: original.meta.pii, directColumn });
    }
  }
  return {
    view: view.name,
    description: String(view.description ?? ''),
    marts: Object.keys(sources).sort(),
    sources,
    fields: fields.sort((a, b) => a.field.localeCompare(b.field)),
  };
}

/** @param {string} text */
const cell = text => text.replace(/\|/g, '\\|').replace(/\s+/g, ' ').trim();
/** @param {string} text */
const code = text => '`' + text.replace(/`/g, "'") + '`';

/**
 * One exposure entry, optionally enabled by a dbt condition.
 * @param {Exposure} exposure
 * @param {string | undefined} enabled
 */
function renderExposure(exposure, enabled) {
  const label = exposure.isInventory ? 'Cube model classification inventory' : 'Cube view';
  const description = [
    `${label} ${code(exposure.view)}: ${cell(exposure.description)}`,
    '',
    'Generated from `packages/metrics/model` by `scripts/platform local exposures`; do not edit by hand.',
    '',
    'Marts read by this definition:',
    '',
    ...exposure.marts.map(mart => exposure.sources[mart] === mart ? `- ${code(mart)}` : `- ${code(mart)}: ${code(exposure.sources[mart] ?? '')}`),
    '',
    '| Cube field | Kind | Mart columns | Description |',
    '| --- | --- | --- | --- |',
    ...exposure.fields.map(field => `| ${code(field.field)} | ${field.kind} | ${
      field.columns.length ? field.columns.map(code).join(', ') : 'row count'} | ${cell(field.description)} |`),
  ];
  const meta = exposure.fields.map(field => `          ${JSON.stringify(field.field)}: ${JSON.stringify(field.columns)}`);
  return [
    `  - name: ${JSON.stringify(`cube_${exposure.view}`)}`,
    `    label: ${JSON.stringify(`${label}: ${exposure.view}`)}`,
    '    type: application',
    `    owner: {name: ${JSON.stringify(OWNER)}}`,
    '    description: |-',
    ...description.map(line => line ? `      ${line}` : ''),
    '    depends_on:',
    ...exposure.marts.map(mart => `      - ${JSON.stringify(`ref('${mart}')`)}`),
    '    config:',
    ...(enabled ? [`      enabled: ${JSON.stringify(`{{ ${enabled} }}`)}`] : []),
    '      meta:',
    '        cube_fields:',
    ...meta,
    '        cube_pii:',
    ...exposure.fields.map(field => `          ${JSON.stringify(field.field)}: ${field.pii}`),
    '        cube_direct_columns:',
    ...exposure.fields.map(field => `          ${JSON.stringify(field.field)}: ${JSON.stringify(field.directColumn)}`),
  ].join('\n');
}

/** The checked-in exposures file for the current Cube model. */
async function renderExposures() {
  /** @type {Array<{views: Exposure[], condition: string}>} */
  const variants = [];
  for (const governed of [false, true]) {
    for (const customers of [false, true]) {
      variants.push({
        views: await compileExposures(customers, MODEL, governed),
        condition: `(${customers ? CUSTOMERS : CUSTOMERS.replace('==', '!=')}) and ${governed ? GOVERNED : `not ${GOVERNED}`}`,
      });
    }
  }
  const views = [...new Set(variants.flatMap(variant => variant.views.map(exposure => exposure.view)))].sort();
  const entries = views.flatMap(view => {
    /** @type {Map<string, {exposure: Exposure, conditions: string[]}>} */
    const definitions = new Map();
    for (const variant of variants) {
      const exposure = variant.views.find(item => item.view === view);
      if (!exposure) continue;
      const key = JSON.stringify(exposure);
      const definition = definitions.get(key) ?? { exposure, conditions: [] };
      definition.conditions.push(variant.condition);
      definitions.set(key, definition);
    }
    return [...definitions.values()].map(({ exposure, conditions }) =>
      renderExposure(exposure, conditions.length === variants.length ? undefined
        : conditions.map(condition => `(${condition})`).join(' or ')));
  });
  return [
    '# Generated by packages/metrics/exposures.cjs; do not edit.',
    '# Regenerate with: scripts/platform local exposures',
    'version: 2',
    'exposures:',
    entries.join('\n\n'),
    '',
  ].join('\n');
}

/**
 * Until Cube has per-user authorization, no Cube member may hold PII.
 * Check hidden members too: hiding is not a substitute for the policy.
 * Author declarations are decisions, not inferred from SQL or lineage.
 * @param {Array<{name: string, public?: boolean, dimensions?: Record<string, any>, measures?: Record<string, any>, segments?: Record<string, any>}>} cubes
 */
function checkCubePii(cubes) {
  for (const cube of cubes) {
    for (const group of [cube.dimensions, cube.measures, cube.segments]) {
      for (const [name, member] of Object.entries(group ?? {})) {
        if (typeof member.meta?.pii !== 'boolean') {
          throw new Error(`${cube.name}.${name}: declare boolean meta.pii`);
        }
        if (member.meta.pii) {
          throw new Error(`${cube.name}.${name}: restricted PII cannot be modeled in Cube without per-user authorization`);
        }
      }
    }
  }
}

module.exports = { compileExposures, renderExposures, checkCubePii };

if (require.main === module) {
  renderExposures().then(text => process.stdout.write(text), error => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
