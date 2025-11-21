# Tor HTTP proxy (auxiliary service)

This directory contains the configuration for an additional Tor-based HTTP
proxy service used by the feed summarizer when running under
`docker-compose`, and was re-used (under MIT license) from https://github.com/zhaow-de/rotating-tor-http-proxy.

The service is started automatically by `docker-compose` alongside the main
application container. It exposes an HTTP proxy endpoint that routes
outbound requests through the Tor network.

You only need this service if you want to fetch certain feeds via Tor (like geo-locked news sites)

## How it is wired into the summarizer

Inside the compose network, the proxy is reachable as `http://tor:3128`.
To have the summarizer use it for outbound HTTP requests, set the `proxy`
section in `feeds.yaml`:

```yaml
proxy:
  url: http://tor:3128
```

If you omit this section, the summarizer connects directly to origin sites
and this service remains unused.

## Tor proxy configuration

The underlying image provides a rotating Tor HTTP proxy. It supports a few
environment variables that you can tune via `docker-compose` if needed:

- `TOR_INSTANCES`: number of concurrent Tor clients (and Privoxy instances).
  Defaults to `10`, valid range is `1`–`40`.
- `TOR_REBUILD_INTERVAL`: how often all circuits are rebuilt, in seconds.
  Defaults to `1800` (30 minutes). The minimum allowed value is `600`.
- `TOR_EXIT_COUNTRY`: optional country code (or comma-separated list of
  codes) to constrain exit nodes, for example:

  ```env
  TOR_EXIT_COUNTRY=de,ch,at
  ```

  Be aware that forcing exit countries can reduce anonymity and may make it
  slower to establish circuits, especially for countries with few exit
  nodes.

For most use cases you can leave these at their defaults; the service will
still provide a working rotating Tor HTTP proxy on `http://tor:3128`.
