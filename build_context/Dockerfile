FROM node:25-alpine

RUN apk update && \
    apk upgrade && \
    apk add git && \
    npm update -g npm

# Create app directory
RUN mkdir -p /option-one/app
WORKDIR /option-one

# Install app dependencies (as re-usable layer)
COPY ./app/ /option-one/app
RUN cd app && \
    npm install --omit=dev  && \
    npm audit fix

# Fix "... code: 'MODULE_NOT_FOUND'"
RUN rm -rf /usr/local/lib/node_modules/npm 

# Bundle app source
COPY startDB.js /option-one/startDB.js

# VOLUME /app/config
VOLUME /db
VOLUME /backup

CMD pwd;cd /option-one;ls -al;ls -al app;node startDB
