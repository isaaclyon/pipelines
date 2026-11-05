// @ts-nocheck Cube model DSL globals are provided by the schema compiler.
// This cube exists only so the service compiles and the platform smoke check
// sees a cube in /meta. It reads no warehouse table. Replace it with the first
// generic mart's cube in Roadmap Phase 4.
cube('placeholder', {
  sql: 'SELECT 1 AS id',
  description: 'Placeholder with one constant row; reads no warehouse table',
  dimensions: {
    id: {
      sql: 'id', type: 'number', primaryKey: true, public: true, meta: { pii: false },
      description: 'Constant row identifier',
    },
  },
  measures: {
    count: { type: 'count', meta: { pii: false }, description: 'Number of placeholder rows' },
  },
});
