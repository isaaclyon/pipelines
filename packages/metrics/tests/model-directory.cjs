const { copyFile, cp } = require('node:fs/promises');
const { join } = require('node:path');
const { SNAPSHOT } = require('../mart-columns.cjs');

const ROOT = join(__dirname, '..');

/**
 * Copy the Cube model and the modules and dbt snapshot it requires into an
 * isolated configuration directory.
 * @param {string} directory
 */
async function copyModel(directory) {
  await cp(join(ROOT, 'model'), join(directory, 'model'), { recursive: true });
  for (const file of ['publication.cjs', 'mart-columns.cjs']) await copyFile(join(ROOT, file), join(directory, file));
  await copyFile(SNAPSHOT, join(directory, 'mart-contracts.json'));
}

module.exports = { copyModel };
