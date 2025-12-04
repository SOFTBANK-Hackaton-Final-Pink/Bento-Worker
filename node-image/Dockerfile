FROM node:18-alpine
WORKDIR /app
RUN npm install express
COPY agent.js .
CMD ["node", "agent.js"]
