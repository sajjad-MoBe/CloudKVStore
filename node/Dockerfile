FROM golang:1.24.2-alpine


ADD ./src /src
WORKDIR /src
RUN go mod download
# RUN go build -o node ./
