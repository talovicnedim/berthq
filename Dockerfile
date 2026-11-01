# Builds the dashboard, embeds it in the broker binary, and ships that one binary.
# Base images are pinned by digest so a release can't pick up a changed image; the tag says
# which one it is. To update: docker buildx imagetools inspect <tag>.

FROM node:26-slim@sha256:e4db4c914bf8ad241bd24804fc25d272d89bad97e68cdc8a1414862790a02c39 AS dashboard
WORKDIR /src
RUN corepack enable
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY apps/dashboard/package.json apps/dashboard/
RUN pnpm install --frozen-lockfile --filter dashboard
COPY apps/dashboard apps/dashboard
RUN pnpm --filter dashboard build

# Same Debian release as the distroless runtime below, so glibc versions match.
FROM rust:1-slim-bookworm@sha256:24707e70a4f1e23d9a7b25202ec43126d28a52294b96aad5c670df1fd1f1d400 AS server
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
