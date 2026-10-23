defmodule AisStream do
  @moduledoc """
  Unofficial client for the [AISStream](https://aisstream.io) WebSocket API.

  Sends every message to the `:to` process as `{:aisstream, message}` (decoded JSON) and
  reconnects on its own.

      {:ok, _pid} =
        AisStream.start_link(
          api_key: System.fetch_env!("AISSTREAM_API_KEY"),
          bounding_boxes: [[[43.0, 7.5], [44.6, 10.5]]],
          filter_message_types: ["PositionReport"],
          to: self()
        )

      receive do
        {:aisstream, %{"MessageType" => type}} -> IO.puts(type)
      end

  ## Options

    * `:api_key` (required) - API key, or a zero-arity function returning it (use the function
      under a supervisor, to keep the key out of its reports).
    * `:bounding_boxes` (required) - list of boxes, each `[[lat1, lon1], [lat2, lon2]]`.
    * `:filter_message_types` - list of message types, e.g. `["PositionReport"]`.
    * `:to` (required) - pid or registered name receiving the messages. The client stops when a
      pid exits; with a name not registered at that moment, messages are dropped.
    * `:name` - name to register the client process under.
    * `:ping_interval` - keepalive interval in ms, default `30_000`.
    * `:url` - default `wss://stream.aisstream.io/v0/stream`.
  """

  use GenServer, restart: :transient
  require Logger

  @type option ::
          {:api_key, String.t() | (-> String.t())}
          | {:bounding_boxes, [[[number()]]]}
          | {:filter_message_types, [String.t()]}
          | {:to, pid() | atom()}
          | {:name, GenServer.name()}
          | {:ping_interval, pos_integer()}
          | {:url, String.t()}

  @doc """
  Starts the client.

  Raises `ArgumentError` on an unknown option and `KeyError` if `:api_key`, `:bounding_boxes`
  or `:to` is missing.
  """
  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    opts =
      Keyword.validate!(opts, [
        :api_key,
        :bounding_boxes,
        :filter_message_types,
        :to,
        :name,
        ping_interval: 30_000,
        url: "wss://stream.aisstream.io/v0/stream"
      ])

    # A function keeps the key out of supervisor reports
    api_key =
      case Keyword.fetch!(opts, :api_key) do
        fun when is_function(fun, 0) -> fun.()
        key -> key
      end

    subscription =
      %{
        "APIKey" => api_key,
        "BoundingBoxes" => Keyword.fetch!(opts, :bounding_boxes)
      }

    subscription =
      case opts[:filter_message_types] do
        nil -> subscription
        types -> Map.put(subscription, "FilterMessageTypes", types)
      end

    GenServer.start_link(
      __MODULE__,
      %{
        url: URI.parse(opts[:url]),
        subscription: JSON.encode!(subscription),
        to: Keyword.fetch!(opts, :to),
        ping_interval: opts[:ping_interval],
        attempt: 0,
        conn: nil
      },
      name: opts[:name]
    )
  end

  # The subscription holds the API key
  @impl true
  def format_status(status),
    do: Map.update(status, :state, nil, &%{&1 | subscription: "[REDACTED]"})

  @doc false
  @spec backoff(pos_integer()) :: pos_integer()
  def backoff(attempt), do: min(attempt, 30) * 1_000

  @impl true
  def init(state) do
    if is_pid(state.to), do: Process.monitor(state.to)
    {:ok, state, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state), do: {:noreply, connect(state)}

  @impl true
  def handle_info(:connect, state), do: {:noreply, connect(state)}

  def handle_info({:DOWN, _ref, :process, pid, _reason}, %{to: pid} = state),
    do: {:stop, :normal, state}

  # Catches connections that die without closing
  def handle_info({:keepalive, ref}, %{conn: conn, ref: ref} = state) when conn != nil do
    if state.received? do
      Process.send_after(self(), {:keepalive, ref}, state.ping_interval)
      {:noreply, send_frame(%{state | received?: false}, {:ping, ""})}
    else
      {:noreply, reconnect(state, :keepalive_timeout)}
    end
  end

  # Stale timer
  def handle_info({:keepalive, _ref}, state), do: {:noreply, state}

  # Stale socket message
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
      # Started here so an unanswered upgrade can't hang the client
      Process.send_after(self(), {:keepalive, ref}, state.ping_interval)
      Map.merge(state, %{conn: conn, ref: ref, status: nil, headers: [], received?: false})
    else
      {:error, reason} -> reconnect(state, reason)
      {:error, conn, reason} -> reconnect(%{state | conn: conn}, reason)
    end
  end

  # An earlier response in the batch may have dropped the connection
  defp handle_response(_response, %{conn: nil} = state), do: state
  defp handle_response({:status, _ref, status}, state), do: %{state | status: status}

  defp handle_response({:headers, _ref, headers}, state),
    do: %{state | headers: state.headers ++ headers}

  defp handle_response({:done, ref}, state) do
    case Mint.WebSocket.new(state.conn, ref, state.status, state.headers) do
      {:ok, conn, websocket} ->
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
        # A registered :to may be restarting: drop rather than crash
        if dest = GenServer.whereis(state.to), do: send(dest, {:aisstream, message})

        # Error messages must not reset the backoff
        if match?(%{"error" => _}, message), do: state, else: %{state | attempt: 0}

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
