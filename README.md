# aisstream-elixir

Minimal [AISStream](https://aisstream.io) client in Elixir: subscribes to a bounding box and prints ship positions.

It uses [WebSockex](https://hex.pm/packages/websockex) and the `JSON` module from the Elixir standard library. The subscription is re-sent on every reconnection, and reconnection backs off up to 30 seconds.

Proposed as the Elixir example for [aisstream/example](https://github.com/aisstream/example/pull/32).

## Run

Requires Elixir 1.18+ and an [AISStream API key](https://aisstream.io/authenticate).

```sh
AISSTREAM_API_KEY=<your key> elixir main.exs
```

Dependencies are installed on the first run by `Mix.install`.

## Limits

WebSockex doesn't support permessage-deflate, which AISStream needs to serve full bandwidth, so the example uses a small bounding box (Ligurian Sea). For larger areas use a client with compression.
