# Specify Base Image
FROM docker.io/node:18-alpine
WORKDIR /usr/app
# COPY Files
COPY package.json ./
COPY ./ ./

RUN npm install

CMD ["npm", "run", "serve"]