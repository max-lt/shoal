FROM rust:1.92-bookworm AS builder

WORKDIR /build
COPY . .
RUN cargo build --release --bin shoald

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/shoald /usr/local/bin/shoald

VOLUME /data
EXPOSE 4820 4821

ENTRYPOINT ["shoald"]
CMD ["start", "-d", "/data", "--s3-listen-addr", "0.0.0.0:4821"]
