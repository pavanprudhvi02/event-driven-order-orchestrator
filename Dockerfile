# One image definition for every service; pick the service with --build-arg SERVICE=...
FROM node:22-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
COPY packages/common/package.json packages/common/
COPY services/order-service/package.json services/order-service/
COPY services/inventory-service/package.json services/inventory-service/
COPY services/payment-service/package.json services/payment-service/
COPY services/api-gateway/package.json services/api-gateway/
RUN npm ci --omit=dev --no-audit --no-fund

FROM node:22-alpine
ARG SERVICE
ENV NODE_ENV=production \
    SERVICE=${SERVICE}
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY package.json ./
COPY packages/common ./packages/common
COPY services/${SERVICE} ./services/${SERVICE}
USER node
CMD ["sh", "-c", "exec node services/${SERVICE}/src/index.js"]
