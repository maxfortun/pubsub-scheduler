FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY src/ ./src/
COPY vitest.config.js ./

ENV NODE_ENV=production

CMD ["npm", "run", "test"]
