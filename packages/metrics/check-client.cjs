// Offline check of a client's Cube files: `node /cube/conf/check-client.cjs`
// in the client's metrics image. It runs the model and client.yaml checks of
// Cube startup (cube.js), with no database or network: read the reporting
// time zone from client.yaml, then compile the toolkit model and the client
// model in /cube/conf/client/model as one, rejecting a repeated file path or
// cube or view name. It exits 1 with the reason when a check fails.
const { reportingTimeZone } = require('./reporting-timezone.cjs');
const { CLIENT_MODEL, TOOLKIT_MODEL, checkModel, readModel } = require('./model-repository.cjs');

/**
 * @param {Record<string, string | undefined>} env
 * @returns {Promise<string>} A summary of what passed.
 */
async function checkClient(env, toolkit = TOOLKIT_MODEL, client = CLIENT_MODEL) {
  const timeZone = reportingTimeZone(env);
  const model = readModel(toolkit, client);
  const { cubeEvaluator } = await checkModel(model);
  const clientFiles = Object.values(model.paths).filter(path => !path.startsWith(`${toolkit}/`)).length;
  const cubes = Object.keys(cubeEvaluator.evaluatedCubes).length;
  return `${cubes} cubes and views compiled from ${model.files.length} model files (${clientFiles} from the client); reporting time zone ${timeZone}`;
}

module.exports = { checkClient };

if (require.main === module) {
  checkClient(process.env).then(summary => console.log(`Cube client check passed: ${summary}`)).catch(error => {
    console.error(`Cube client check failed: ${error instanceof Error ? error.message : error}`);
    process.exitCode = 1;
  });
}
