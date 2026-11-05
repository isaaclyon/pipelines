// Cube loads the toolkit model and a client's model as one model (ADR 0030).
// A client's metrics image is built FROM the toolkit image and copies its
// Cube definitions into CLIENT_MODEL. Cube keeps the last definition of a
// repeated file path or cube name without an error, so either repeat stops
// startup instead of letting one definition silently replace the other.
const { spawnSync } = require('node:child_process');
const { existsSync, readdirSync, readFileSync } = require('node:fs');
const { join, relative } = require('node:path');

const TOOLKIT_MODEL = join(__dirname, 'model');
const CLIENT_MODEL = join(__dirname, 'client', 'model');
// The file types Cube's own file repository compiles.
const MODEL_FILE = /\.(js|ya?ml|jinja|py)$/;

/**
 * @typedef {import('@cubejs-backend/shared').FileContent} FileContent
 * @typedef {{ root: string, files: FileContent[], paths: Record<string, string> }} Model
 */

/** @param {string} dir @returns {string[]} */
function modelPaths(dir) {
  return readdirSync(dir, { withFileTypes: true }).flatMap(entry => {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) return modelPaths(path);
    return MODEL_FILE.test(entry.name) ? [path] : [];
  });
}

/**
 * Read the toolkit folder, then the client folder if it exists. File names are
 * relative to their folder, as Cube expects; `paths` maps them back.
 * @returns {Model}
 */
function readModel(toolkit = TOOLKIT_MODEL, client = CLIENT_MODEL) {
  /** @type {Record<string, string>} */
  const paths = {};
  /** @type {FileContent[]} */
  const files = [];
  for (const root of existsSync(client) ? [toolkit, client] : [toolkit]) {
    for (const path of modelPaths(root).sort()) {
      const fileName = relative(root, path);
      const other = paths[fileName];
      if (other !== undefined) {
        throw new Error(`Model file ${fileName} is defined more than once: ${other} and ${path}. Rename the client file.`);
      }
      paths[fileName] = path;
      files.push({ fileName, content: readFileSync(path, 'utf8') });
    }
  }
  return { root: toolkit, files, paths };
}

/** @param {Model} model @returns {import('@cubejs-backend/shared').SchemaFileRepository} */
function repository(model) {
  return { localPath: () => model.root, dataSchemaFiles: async () => model.files };
}

/**
 * Compile the model with the pinned Cube compiler and reject repeated cube or
 * view names, naming every file that defines one.
 * @param {Model} model
 */
async function checkModel(model) {
  const { prepareCompiler } = require('@cubejs-backend/schema-compiler');
  const compilers = prepareCompiler(repository(model), { standalone: true });
  const { cubeEvaluator } = compilers;
  // The evaluator receives every cube and view definition with its file;
  // Cube itself keeps only the last definition of each name.
  /** @type {Array<{ name: string, fileName: string, isView?: boolean }>} */
  let definitions = [];
  const compile = cubeEvaluator.compile.bind(cubeEvaluator);
  cubeEvaluator.compile = (cubes, errorReporter) => {
    definitions = cubes;
    compile(cubes, errorReporter);
  };
  /** @type {unknown} */
  let failure;
  try {
    await compilers.compiler.compile();
  } catch (error) {
    failure = error;
  }
  /** @type {Map<string, string[]>} */
  const files = new Map();
  for (const { name, fileName, isView } of definitions) {
    const key = `${isView ? 'view' : 'cube'} ${name}`;
    files.set(key, [...files.get(key) ?? [], model.paths[fileName] ?? fileName]);
  }
  const repeated = [...files].filter(([, paths]) => paths.length > 1)
    .map(([key, paths]) => `${key} is defined more than once: ${paths.join(' and ')}`);
  if (repeated.length > 0) throw new Error(`${repeated.join('; ')}. Rename the client definition.`);
  if (failure !== undefined) throw failure;
  return compilers;
}

/**
 * Read and check the model before Cube starts, so a repeated name stops
 * startup. The compiler is asynchronous and Cube loads its configuration
 * synchronously, so a child process compiles the exact files Cube serves.
 */
function loadModel(toolkit = TOOLKIT_MODEL, client = CLIENT_MODEL) {
  const model = readModel(toolkit, client);
  const check = spawnSync(process.execPath, [__filename], { input: JSON.stringify(model), encoding: 'utf8' });
  if (check.status !== 0) throw new Error(check.stderr.trim() || `Cube model check failed: ${check.signal}`);
  return repository(model);
}

module.exports = { CLIENT_MODEL, TOOLKIT_MODEL, checkModel, loadModel, readModel };

if (require.main === module) {
  checkModel(JSON.parse(readFileSync(0, 'utf8'))).catch(error => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
