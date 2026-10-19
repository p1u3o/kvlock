# syntax=docker/dockerfile:1
FROM golang:1.23-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/kvrocks-leader ./cmd/kvrocks-leader

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/kvrocks-leader /kvrocks-leader
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/kvrocks-leader"]
