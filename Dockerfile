FROM golang:1.22 AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
RUN CGO_ENABLED=0 go build -o /out/orchd ./cmd/orchd && CGO_ENABLED=0 go build -o /out/loadgen ./cmd/loadgen

FROM gcr.io/distroless/static
COPY --from=build /out/orchd /out/loadgen /
EXPOSE 8080
ENTRYPOINT ["/orchd"]
