# Security policy

The broker holds application data, so security reports are taken seriously.

## Reporting a vulnerability

Please **don't open a public issue**. Instead, use
[GitHub's private vulnerability reporting](https://github.com/talovicnedim/berthq/security/advisories/new)
for this repository.

Include what you found, how to reproduce it, and the impact you expect. You'll get an answer
within a few days, and credit in the release notes if you'd like.

## Supported versions

Only the latest release receives security fixes.

## Running it safely

- By default the broker listens on `127.0.0.1` with no API keys. On any other address (`--host
0.0.0.0`, the Docker image) it refuses to start without `--api-key`, unless `--no-auth` says
  the network is private on purpose. Add `--tls-cert` / `--tls-key` or a TLS-terminating proxy
  when traffic leaves the machine.
- Without API keys, the broker refuses browser requests from other web sites, and on a loopback
  address also every host name but `localhost` and loopback addresses. So a web page can't use
  the open API through your browser, not even with DNS rebinding. With `--no-auth` on a network
  address any host name works: it relies on the network being private, and a web page opened in
  a browser on that network could still reach the broker through DNS rebinding.
- Give producers `publish:` keys and workers `consume:` keys (`publish+consume:` for an app that
  does both); keep `admin` keys for operators. Backups (`/v1/backup`) need `admin`.
- A `consume` key can create consumers: `consume` creates the consumer it names on first use.
  Each new consumer keeps messages from being deleted by retention until it acks them, so give
  `consume` keys only to your own workers.
- Keep the port private: a firewall, a private network, or a Kubernetes ClusterIP Service with a
  NetworkPolicy (see `deploy/kubernetes/berthq.yaml`).
- Berthq closes connections that send no request within 30 seconds, but it doesn't limit how
  many connections are open. An HTTP/2 client that answers pings can keep an idle connection
  open. Where untrusted clients can reach the port, put a proxy with connection limits in front.
- Every message is stored in the data file: protect the data directory (and backups) like a
  database.

## Scope

Especially interesting:

- Requests that bypass API keys or their scopes
- A web page that can use an open API (no keys) through the visitor's browser
- Losing an acknowledged publish, or giving a message to two workers of the same consumer while
  the first one's lease is still valid
- Filling the disk past `--min-free-disk` so that acks or deletes stop working
- Crafted queue names, ids or bodies that crash the broker or corrupt the data file
