# syntax=docker/dockerfile:1.7

FROM golang:1.25-alpine AS build
WORKDIR /src
RUN apk add --no-cache git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" -o /out/configmesh-server ./cmd/configmesh-server

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/configmesh-server /app/configmesh-server
USER nonroot:nonroot
EXPOSE 9090
ENTRYPOINT ["/app/configmesh-server"]
