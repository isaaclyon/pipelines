FROM cubejs/cube:v1.6.32@sha256:1c534c8e818e0235fcfa606d53d959fa6b7eb8344b06adc333f6a39f081e7786 AS base
WORKDIR /cube/conf

FROM base AS runtime
# Run as UID 10001, the only reader of mounted remote secret files.
RUN useradd --uid 10001 --user-group --no-create-home --home-dir /cube/conf analytics
COPY --chown=analytics:analytics packages/metrics/cube.js packages/metrics/package.json ./
COPY --chown=analytics:analytics packages/metrics/model ./model
COPY --chown=analytics:analytics packages/metrics/*.cjs ./
USER analytics

FROM base AS check-dependencies
USER root
COPY --chown=node:node packages/metrics/package.json packages/metrics/package-lock.json ./
RUN npm ci --ignore-scripts --no-audit --no-fund

FROM check-dependencies AS checks
COPY --from=runtime --chown=node:node /cube/conf/cube.js /cube/conf/*.cjs ./
COPY --from=runtime --chown=node:node /cube/conf/model ./model
USER node
COPY --chown=node:node packages/metrics/tsconfig.json ./
COPY --chown=node:node packages/metrics/tests ./tests
ENTRYPOINT ["npm", "run", "check"]
