// These expected errors make tsc fail if the corresponding checks stop working.
// This file is type-checked, never executed.

// @ts-expect-error Parameters must have a known type.
function untyped(value) { return value; }

/** @type {string} */
// @ts-expect-error Strict null checking rejects missing values.
const missing = undefined;

/** @type {string[]} */
const values = [];
// @ts-expect-error Indexed values may be absent.
values[0].toUpperCase();

/** @type {{ value?: string }} */
// @ts-expect-error Optional properties cannot explicitly contain undefined.
const optional = { value: undefined };

/** @satisfies {import('@cubejs-backend/server-core').CreateOptions} */
const invalidConfig = {
  // @ts-expect-error Validate against Cube's upstream configuration types.
  schemaPath: 123,
};
