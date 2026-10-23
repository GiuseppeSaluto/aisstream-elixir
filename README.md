# AisStream

Unofficial Elixir client for the [AISStream](https://aisstream.io) WebSocket API. Not affiliated with aisstream.io.

- permessage-deflate compression
- reconnection with backoff (1-30 s) and keepalive pings
- every message sent to a process as `{:aisstream, message}` (decoded JSON)

## Installation

Requires Elixir 1.18+.

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
    filter_message_types: ["PositionReport"],
    to: self()
  )

receive do
  {:aisstream, %{"MessageType" => "PositionReport", "Message" => %{"PositionReport" => report}}} ->
    IO.inspect(report["UserID"], label: "MMSI")
end
```

```elixir
children = [
  {AisStream,
   api_key: fn -> System.fetch_env!("AISSTREAM_API_KEY") end,
   bounding_boxes: boxes,
   to: MyApp.AisConsumer}
]
```

## Options

| Option | Required | Description |
|---|---|---|
| `:api_key` | yes | AISStream API key, or a zero-arity function returning it |
| `:bounding_boxes` | yes | List of boxes, each `[[lat1, lon1], [lat2, lon2]]` |
| `:filter_message_types` | no | List of message types, e.g. `["PositionReport"]` |
| `:to` | yes | pid or registered name receiving the messages |
| `:name` | no | Name to register the client process under |
| `:ping_interval` | no | Keepalive interval in ms, default `30_000` |
| `:url` | no | Default `wss://stream.aisstream.io/v0/stream` |

Notes:

- If `:to` is a pid, the client stops when it exits; if it is a name with no process registered, messages are dropped.
- The API key is redacted from crash reports. Under a supervisor pass it as a function, so it stays out of
  supervisor reports too.
- AISStream closes the connection without an error on an invalid key: the client keeps retrying and logs it.

## Development

```sh
mix test
AISSTREAM_API_KEY=<key> mix test --include live
```

## License

MIT, see [LICENSE](LICENSE).
