defmodule AisStream do
  @moduledoc """
  Unofficial client for the [AISStream](https://aisstream.io) WebSocket API.

  Connects with permessage-deflate compression, subscribes to the given bounding boxes and sends
  every message to a process as `{:aisstream, message}`, where `message` is the decoded JSON map.
  It reconnects on its own, re-sending the subscription, with a delay of 1 s, 2 s, ... up to 30 s.

      {:ok, _pid} =
        AisStream.start_link(
          api_key: System.fetch_env!("AISSTREAM_API_KEY"),
          bounding_boxes: [[[43.0, 7.5], [44.6, 10.5]]],
          filter_message_types: ["PositionReport"]
        )

      receive do
        {:aisstream, %{"MessageType" => type}} -> IO.puts(type)
      end

  ## Options

    * `:api_key` (required) - AISStream API key.
    * `:bounding_boxes` (required) - list of boxes, each `[[lat1, lon1], [lat2, lon2]]`.
    * `:filter_message_types` - list of message types, e.g. `["PositionReport"]`.
    * `:to` - pid or registered name that receives the messages. Defaults to the caller of
      `start_link/1`. Under a supervisor always pass it, otherwise messages go to the supervisor.
      When it is a pid, the client stops when that process exits.
    * `:ping_interval` - keepalive interval in ms, default `30_000`. At each interval the client
      pings the server, and reconnects if nothing (not even a pong) arrived since the previous one.
    * `:url` - WebSocket URL. Defaults to `wss://stream.aisstream.io/v0/stream`.

  With an invalid API key AISStream closes the connection without an error message, so the
  client keeps reconnecting, with a growing delay, and logging the disconnection.
  """

  use GenServer, restart: :transient
  require Logger

  @doc """
  Starts the client. Raises `KeyError` if `:api_key` or `:bounding_boxes` is missing.
  """
  def start_link(opts) do
    subscription =
      %{
        "APIKey" => Keyword.fetch!(opts, :api_key),
        "BoundingBoxes" => Keyword.fetch!(opts, :bounding_boxes)
      }

    subscription =
      case opts[:filter_message_types] do
        nil -> subscription
        types -> Map.put(subscription, "FilterMessageTypes", types)
      end

    GenServer.start_link(__MODULE__, %{
      url: URI.parse(Keyword.get(opts, :url, "wss://stream.aisstream.io/v0/stream")),
      subscription: JSON.encode!(subscription),
      to: Keyword.get(opts, :to, self()),
      ping_interval: Keyword.get(opts, :ping_interval, 30_000),
      attempt: 0,
      conn: nil
    })
  end

  @doc false
  def backoff(attempt), do: min(attempt, 30) * 1_000

  @impl true
  def init(state) do
    # Nobody reads the messages once a :to pid is gone. A registered name may be restarted.
    if is_pid(state.to), do: Process.monitor(state.to)
    {:ok, state, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state), do: {:noreply, connect(state)}

  @impl true
  def handle_info(:connect, state), do: {:noreply, connect(state)}

  def handle_info({:DOWN, _ref, :process, pid, _reason}, %{to: pid} = state),
    do: {:stop, :normal, state}

  # Silent dead connections (Wi-Fi, router) never close: ping, and give up if nothing came back
  def handle_info({:keepalive, ref}, %{conn: conn, ref: ref} = state) when conn != nil do
    if state.received? do
      Process.send_after(self(), {:keepalive, ref}, state.ping_interval)
      {:noreply, send_frame(%{state | received?: false}, {:ping, ""})}
    else
      {:noreply, reconnect(state, :keepalive_timeout)}
    end
  end

  # Timer of a connection we already dropped
  def handle_info({:keepalive, _ref}, state), do: {:noreply, state}

  # Messages from a socket we already dropped
  def handle_info(_message, %{conn: nil} = state), do: {:noreply, state}

  def handle_info(message, state) do
    case Mint.WebSocket.stream(state.conn, message) do
      {:ok, conn, responses} ->
        {:noreply, Enum.reduce(responses, %{state | conn: conn}, &handle_response/2)}

      {:error, conn, reason, _responses} ->
        {:noreply, reconnect(%{state | conn: conn}, reason)}

      :unknown ->
        {:noreply, state}
    end
  end

  defp connect(%{url: url} = state) do
    {http, ws} = if url.scheme == "wss", do: {:https, :wss}, else: {:http, :ws}

    with {:ok, conn} <- Mint.HTTP.connect(http, url.host, url.port, protocols: [:http1]),
         {:ok, conn, ref} <-
           Mint.WebSocket.upgrade(ws, conn, url.path || "/", [],
             extensions: [Mint.WebSocket.PerMessageDeflate]
           ) do
      Map.merge(state, %{conn: conn, ref: ref, status: nil, headers: []})
    else
      {:error, reason} -> reconnect(state, reason)
      {:error, conn, reason} -> reconnect(%{state | conn: conn}, reason)
    end
  end

  # A response may have triggered a reconnection: ignore the rest of the batch
  defp handle_response(_response, %{conn: nil} = state), do: state
  defp handle_response({:status, _ref, status}, state), do: %{state | status: status}

  defp handle_response({:headers, _ref, headers}, state),
    do: %{state | headers: state.headers ++ headers}

  defp handle_response({:done, ref}, state) do
    case Mint.WebSocket.new(state.conn, ref, state.status, state.headers) do
      {:ok, conn, websocket} ->
        # The opening counts as received: the first keepalive check sends a ping
        Process.send_after(self(), {:keepalive, ref}, state.ping_interval)

        send_frame(
          Map.merge(state, %{conn: conn, websocket: websocket, received?: true}),
          {:text, state.subscription}
        )

      {:error, conn, reason} ->
        reconnect(%{state | conn: conn}, reason)
    end
  end

  defp handle_response({:data, _ref, data}, state) do
    case Mint.WebSocket.decode(state.websocket, data) do
      {:ok, websocket, frames} ->
        Enum.reduce(frames, %{state | websocket: websocket, received?: true}, &handle_frame/2)

      {:error, websocket, reason} ->
        reconnect(%{state | websocket: websocket}, reason)
    end
  end

  defp handle_response(_response, state), do: state

  defp handle_frame(_frame, %{conn: nil} = state), do: state

  defp handle_frame({type, payload}, state) when type in [:text, :binary] do
    case JSON.decode(payload) do
      {:ok, message} ->
        send(state.to, {:aisstream, message})
        # Reset only now: with an invalid key AISStream opens the connection and closes it at once
        %{state | attempt: 0}

      {:error, reason} ->
        Logger.warning("AisStream: skipping invalid JSON (#{inspect(reason)})")
        state
    end
  end

  defp handle_frame({:ping, data}, state), do: send_frame(state, {:pong, data})
  defp handle_frame({:close, code, reason}, state), do: reconnect(state, {:close, code, reason})
  defp handle_frame({:error, reason}, state), do: reconnect(state, reason)
  defp handle_frame(_frame, state), do: state

  defp send_frame(state, frame) do
    with {:ok, websocket, data} <- Mint.WebSocket.encode(state.websocket, frame),
         {:ok, conn} <- Mint.WebSocket.stream_request_body(state.conn, state.ref, data) do
      %{state | conn: conn, websocket: websocket}
    else
      {:error, %Mint.WebSocket{} = websocket, reason} ->
        reconnect(%{state | websocket: websocket}, reason)

      {:error, conn, reason} ->
        reconnect(%{state | conn: conn}, reason)
    end
  end

  defp reconnect(state, reason) do
    if state.conn, do: Mint.HTTP.close(state.conn)
    attempt = state.attempt + 1

    Logger.warning(
      "AisStream: disconnected (#{inspect(reason)}), reconnecting in #{backoff(attempt)} ms"
    )

    Process.send_after(self(), :connect, backoff(attempt))
    %{state | conn: nil, attempt: attempt}
  end
end
