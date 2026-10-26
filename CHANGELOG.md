# Changelog

All notable changes to this project are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-10-26

### Added

- `AisStream.start_link/1` and `child_spec/1`: WebSocket client for the AISStream API, with
  permessage-deflate compression.
- Each decoded message is sent to the `:to` process as `{:aisstream, message}`; while a registered `:to` name has no
  process (e.g. during a restart), messages are dropped instead of crashing the client.
- Options `:api_key`, `:bounding_boxes`, `:to` (required), `:filter_message_types`, `:name`, `:ping_interval`,
  `:max_queue_len`, `:url`;
  unknown options and invalid `:ping_interval`, `:max_queue_len` or `:url` values raise `ArgumentError`.
- Automatic reconnection with a 1-30 s backoff; the subscription is re-sent after every reconnection.
- Keepalive: pings every `:ping_interval` ms and reconnects when the server stops answering, including when it
  never answers the WebSocket handshake.
- The backoff keeps growing when AISStream closes the connection without data or after an error message.
- When `:to` is a pid, the client stops when that process exits.
- `:api_key` also accepts a zero-arity function, which keeps the key out of supervisor reports.
- `:max_queue_len` option: drops messages while the local `:to` process has that many queued (off by default).

### Security

- The API key is redacted from crash reports and `:sys.get_status/1` (`format_status/1`).
