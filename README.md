# AisStream

Unofficial Elixir client for the [AISStream](https://aisstream.io) WebSocket API. Not affiliated with aisstream.io.

- Connects with permessage-deflate compression: AISStream limits bandwidth on uncompressed connections.
- Re-sends the subscription after every reconnection, waiting 1 s, 2 s, ... up to 30 s between attempts.
- Detects dead connections that never close (Wi-Fi, router) with WebSocket pings.
- Sends every message to a process as `{:aisstream, message}`, where `message` is the decoded JSON map.

## Installation

Requires Elixir 1.18+ (for the built-in `JSON` module). Add `aisstream` to your dependencies in `mix.exs`:

```elixir
def deps do
  [{:aisstream, "~> 0.1"}]
end
```

## Usage

You need an [AISStream API key](https://aisstream.io/authenticate).

```elixir
{:ok, _pid} =
  AisStream.start_link(
    api_key: System.fetch_env!("AISSTREAM_API_KEY"),
    bounding_boxes: [[[43.0, 7.5], [44.6, 10.5]]],
    filter_message_types: ["PositionReport"]
  )

receive do
  {:aisstream, %{"MessageType" => "PositionReport", "Message" => %{"PositionReport" => report}}} ->
    IO.inspect(report["UserID"], label: "MMSI")
end
```

Under a supervisor, pass `:to` with the process that should receive the messages, otherwise they go to the supervisor:

```elixir
children = [
  {AisStream, api_key: api_key, bounding_boxes: boxes, to: MyApp.AisConsumer}
]
```

## Options

| Option | Required | Description |
|---|---|---|
| `:api_key` | yes | AISStream API key |
| `:bounding_boxes` | yes | List of boxes, each `[[lat1, lon1], [lat2, lon2]]` |
| `:filter_message_types` | no | List of message types, e.g. `["PositionReport"]` |
| `:to` | no | pid or registered name that receives the messages. Defaults to the caller of `start_link/1`. When it is a pid, the client stops when that process exits |
| `:ping_interval` | no | Keepalive interval in ms, default `30_000`: the client pings the server and reconnects if nothing, not even a pong, arrived since the previous interval |
| `:url` | no | WebSocket URL. Defaults to `wss://stream.aisstream.io/v0/stream` |

With an invalid API key AISStream closes the connection without an error message, so the client keeps reconnecting, with a growing delay, and logging the disconnection.

## Development

```sh
mix test                                                  # local WebSocket server, no network
AISSTREAM_API_KEY=<your key> mix test --include live      # against stream.aisstream.io
```

## License

MIT, see [LICENSE](LICENSE).
