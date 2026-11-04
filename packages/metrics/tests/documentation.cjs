const assert = require('node:assert/strict');

/** @typedef {{ name: string, description?: string }} Described */

/**
 * Every public view, measure and dimension in Cube `/meta` has a description.
 * @param {Record<string, any>} meta Cube `/meta` response.
 * @param {string[]} views Public views to check; test-only probes are excluded.
 */
function assertDocumented(meta, views) {
  /** @type {string[]} */
  const missing = [];
  for (const name of views) {
    /** @type {(Described & { measures: Described[], dimensions: Described[] }) | undefined} */
    const view = meta.cubes.find((/** @type {Described} */ cube) => cube.name === name);
    assert.ok(view, `${name} is public`);
    for (const item of [view, ...view.measures, ...view.dimensions]) {
      if (!item.description?.trim()) missing.push(item.name);
    }
  }
  assert.deepEqual(missing, [], 'add a description to each public view and member');
}

module.exports = { assertDocumented };
