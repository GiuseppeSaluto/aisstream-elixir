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
    * `:url` - WebSocket URL. Defaults to `wss://stream.aisstream.io/v0/stream`.

  With an invalid API key AISStream closes the connection without an error message, so the
  client keeps reconnecting and logging the disconnection.
  """

  use GenServer
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
      attempt: 0,
      conn: nil
    })
  end

  @doc false
  def backoff(attempt), do: min(attempt, 30) * 1_000

  @impl true
  def init(state), do: {:ok, state, {:continue, :connect}}

  @impl true
  def handle_continue(:connect, state), do: {:noreply, connect(state)}

  @impl true
  def handle_info(:connect, state), do: {:noreply, connect(state)}

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
        send_frame(
          Map.merge(state, %{conn: conn, websocket: websocket, attempt: 0}),
          {:text, state.subscription}
        )

      {:error, conn, reason} ->
        reconnect(%{state | conn: conn}, reason)
    end
  end

  defp handle_response({:data, _ref, data}, state) do
    case Mint.WebSocket.decode(state.websocket, data) do
      {:ok, websocket, frames} ->
        Enum.reduce(frames, %{state | websocket: websocket}, &handle_frame/2)

      {:error, websocket, reason} ->
        reconnect(%{state | websocket: websocket}, reason)
    end
  end

  defp handle_response(_response, state), do: state

  defp handle_frame(_frame, %{conn: nil} = state), do: state

  defp handle_frame({type, payload}, state) when type in [:text, :binary] do
    case JSON.decode(payload) do
      {:ok, message} -> send(state.to, {:aisstream, message})
      {:error, reason} -> Logger.warning("AisStream: skipping invalid JSON (#{inspect(reason)})")
    end

    state
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
