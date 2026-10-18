# Builds the dashboard, embeds it in the broker binary, and ships that one binary.
# Base images are pinned by digest so a release can't pick up a changed image; the tag says
# which one it is. To update: docker buildx imagetools inspect <tag>.

FROM node:26-slim@sha256:0d1e33586362bb9e608c4e751e7a9e7edb55c9ac11461ed5666af696cc461dd4 AS dashboard
WORKDIR /src
RUN corepack enable
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY apps/dashboard/package.json apps/dashboard/
RUN pnpm install --frozen-lockfile --filter dashboard
COPY apps/dashboard apps/dashboard
RUN pnpm --filter dashboard build

# Same Debian release as the distroless runtime below, so glibc versions match.
FROM rust:1-slim-bookworm@sha256:a24e292b4af7bdee351a31d3a6379972a5652f0ea22a1b6d6883324ac4f5097c AS server
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates crates
COPY sdks/rust sdks/rust
COPY --from=dashboard /src/apps/dashboard/build apps/dashboard/build
# Without the build, the binary still compiles and only shows "the dashboard is not built".
RUN test -f apps/dashboard/build/index.html || \
    { echo "apps/dashboard/build/index.html is missing, so the binary would have no dashboard" >&2; exit 1; }
RUN cargo build --release --locked -p berthq-cli && mkdir /empty-data

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f
COPY --from=server /src/target/release/berthq /usr/local/bin/berthq
COPY --from=server --chown=65532:65532 /empty-data /data
ENV BERTHQ_HOST=0.0.0.0 \
    BERTHQ_DATA=/data
VOLUME /data
EXPOSE 7331
ENTRYPOINT ["berthq"]
CMD ["start"]
