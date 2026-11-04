const assert = require('node:assert/strict');
const { constants } = require('node:fs');
const { copyFile, lstat, realpath, stat, unlink } = require('node:fs/promises');
const { join, isAbsolute } = require('node:path');

/** Make a new independent copy, never overwrite a branch's working file.
 * The owner must checkpoint and close the source first. Never open it writable
 * or repair recovery state. Operators must keep unmanaged clients stopped.
 * Docker Desktop locks are not reliable across host/bind-mount contexts:
 * cube.sh must refuse host holders and running source-mounting containers first.
 * @param {string} source
 * @param {string} directory Explicit worktree-owned destination directory.
 */
async function copyLocalDatabase(source, directory) {
  assert.ok(isAbsolute(source) && isAbsolute(directory), 'Copy paths must be absolute');
  assert.equal(await realpath(source), source, 'Source must not contain a symlink');
  assert.equal(await realpath(directory), directory, 'Destination must not contain a symlink');
  const info = await stat(source);
  assert.ok(info.isFile() && info.nlink === 1, 'Source must be a regular, unshared file');
  assert.ok((await stat(directory)).isDirectory());
  const destination = join(directory, 'working.duckdb');
  assert.notEqual(source, destination, 'Source and destination must differ');
  await noWal(source);
  const before = info;
  await copyFile(source, destination, constants.COPYFILE_EXCL);
  try {
    const after = await stat(source);
    assert.equal(after.ino, before.ino, 'Source changed during copy');
    assert.equal(after.dev, before.dev, 'Source changed during copy');
    assert.equal(after.nlink, before.nlink, 'Source changed during copy');
    assert.equal(after.ctimeMs, before.ctimeMs, 'Source changed during copy');
    assert.equal(after.size, before.size, 'Source changed during copy');
    assert.equal(after.mtimeMs, before.mtimeMs, 'Source changed during copy');
    await noWal(source);
    return destination;
  } catch (error) {
    await unlink(destination);
    throw error;
  }
}

/** @param {string} source */
async function noWal(source) {
  const wal = await lstat(source + '.wal').catch((/** @type {NodeJS.ErrnoException} */ error) => {
    if (error.code !== 'ENOENT') throw error;
    return null;
  });
  assert.equal(wal, null, 'Source WAL exists; source owner must checkpoint and close it before copying; no automatic repair');
}

module.exports = { copyLocalDatabase };
