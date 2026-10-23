# @meitogi/signal relay — plain ws://:8787 by default ; TLS_CERT / TLS_KEY to serve wss:// itself.
FROM node:24.22.0-alpine3.24
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund
COPY src ./src
COPY bin ./bin
USER node
EXPOSE 8787
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s \
  CMD wget -q -O /dev/null http://127.0.0.1:8787/healthz || exit 1
CMD ["node", "bin/signal.js"]
