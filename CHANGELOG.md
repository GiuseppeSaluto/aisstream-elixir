# Changelog

All notable changes to this project are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-10-22

### Added

- `AisStream.start_link/1` and `child_spec/1`: WebSocket client for the AISStream API, with
  permessage-deflate compression.
- Each decoded message is sent to the `:to` process (default: the caller) as `{:aisstream, message}`.
- Options `:api_key`, `:bounding_boxes`, `:filter_message_types`, `:to`, `:ping_interval`, `:url`.
- Automatic reconnection with a 1-30 s backoff; the subscription is re-sent after every reconnection.
- Keepalive: pings every `:ping_interval` ms and reconnects when the server stops answering.
- When `:to` is a pid, the client stops when that process exits.
