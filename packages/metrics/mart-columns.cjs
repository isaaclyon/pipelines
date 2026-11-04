// Cube dimensions whose meaning comes from dbt (ADR 0014, 2026-10-22 addendum).
// dbt YAML owns each published column's description, type and PII flag; the
// transformations package checks them into its reviewed mart-contract snapshot
// for the customers-enabled project. Cube keeps the allowlist, computed and
// renamed fields, measures, joins and views. Model files `require` this module
// through Node, like publication.cjs.

const { existsSync, readFileSync } = require('node:fs');
const { join } = require('node:path');

// The image copies the snapshot beside this module; a checkout reads it from
// the sibling package.
const found = [
  join(__dirname, 'mart-contracts.json'),
  join(__dirname, '..', 'transformations', 'dbt', 'customer-mart-contracts.json'),
].find(path => existsSync(path));
if (!found) throw new Error('dbt mart contract snapshot is missing');
const SNAPSHOT = found;

/**
 * @typedef {{ name: string, data_type: string, description: string, pii: boolean }} ContractColumn
 * @typedef {{ relation: string, access: string, columns: ContractColumn[] }} MartContract
 */

/** @type {MartContract[]} */
const contracts = JSON.parse(readFileSync(SNAPSHOT, 'utf8'));

/**
 * Columns of one analytical mart in the reviewed dbt contract.
 * @param {string} relation
 */
function contractColumns(relation) {
  const mart = contracts.find(item => item.relation === relation);
  if (!mart) throw new Error(`${relation}: not in the dbt mart contract snapshot`);
  // Restricted marts hold personal data the Cube reader role cannot read.
  if (mart.access !== 'analytical') throw new Error(`${relation}: restricted mart; Cube reads analytical marts only`);
  return new Map(mart.columns.map(column => [column.name, column]));
}

/** @param {string} dataType dbt contract type, such as decimal(38, 9). */
function cubeType(dataType) {
  const base = dataType.replace(/\(.*\)$/, '');
  if (base === 'varchar') return 'string';
  if (base === 'boolean') return 'boolean';
  if (base === 'timestamp' || base === 'date') return 'time';
  if (base === 'bigint' || base === 'decimal') return 'number';
  throw new Error(`No Cube type for dbt type ${dataType}`);
}

/**
 * Cube dimensions that read mart columns by name, described by dbt.
 * @param {string} relation
 * @param {string[]} names
 */
function dbtDimensions(relation, names) {
  const columns = contractColumns(relation);
  return Object.fromEntries(names.map(name => {
    const column = columns.get(name);
    if (!column) throw new Error(`${relation}.${name}: not in the dbt mart contract snapshot`);
    // Cube's compiler wraps member SQL written in a model file in a function;
    // members built here must supply the function themselves.
    return [name, { sql: () => name, type: cubeType(column.data_type), description: column.description, meta: { pii: column.pii } }];
  }));
}

module.exports = { SNAPSHOT, contractColumns, dbtDimensions };
