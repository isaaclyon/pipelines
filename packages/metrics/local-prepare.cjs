// Offline worker used only after the owning worktree's service is stopped.
const assert = require('node:assert/strict');
const { readFile, writeFile } = require('node:fs/promises');
const { join, relative } = require('node:path');
const { copyLocalDatabase } = require('./local-copy.cjs');
const { exportLocalMarts } = require('./local-export.cjs');

async function main() {
  assert.equal(process.env.ANALYTICS_ENVIRONMENT, 'local', 'Explicit local environment required');
  assert.equal(process.env.LOCAL_SOURCE_STOPPED, 'true', 'Confirm all source clients are stopped');
  const [, , source = '', directory = '', schema = '', privacyPath = ''] = process.argv;
  const privacyText = await readFile(privacyPath, 'utf8');
  const privacy = JSON.parse(privacyText);
  // Retain the supplied evidence unchanged, including missing/unknown labels.
  await writeFile(join(directory, 'privacy.json'), privacyText, { flag: 'wx', mode: 0o600 });
  const working = await copyLocalDatabase(source, directory);
  const receipt = await exportLocalMarts(working, directory, schema, privacy);
  await writeFile(join(directory, 'receipt.json'), JSON.stringify({
    file: relative(directory, receipt.path), schema, publicationId: receipt.publicationId,
  }), { flag: 'wx', mode: 0o600 });
}
main().catch(error => { console.error(error); process.exit(1); });
