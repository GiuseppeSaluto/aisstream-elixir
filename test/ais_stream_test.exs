defmodule AisStreamTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  @boxes [[[45.0, 10.0], [46.5, 12.25]]]

  describe "options" do
    test "missing :api_key raises KeyError" do
      assert_raise KeyError, fn -> AisStream.start_link(bounding_boxes: @boxes, to: self()) end
    end

    test "missing :bounding_boxes raises KeyError" do
      assert_raise KeyError, fn -> AisStream.start_link(api_key: "test-key", to: self()) end
    end

    # A local url keeps the test off the network if the option were not rejected.
    test "missing :to raises KeyError" do
      {_server, url} = start_server()

      error =
        assert_raise KeyError, fn ->
          AisStream.start_link(api_key: "test-key", bounding_boxes: @boxes, url: url)
        end

      assert error.key == :to
    end

    test "an unknown option raises ArgumentError" do
      {_server, url} = start_server()
      opts = [api_key: "test-key", bounding_boxes: @boxes, url: url, to: self()]

      assert_raise ArgumentError, fn -> AisStream.start_link(opts ++ [ping_intervall: 5]) end
    end

    test ":name registers the client process under that name" do
      name = :"aisstream_test_client_#{System.unique_integer([:positive])}"
      {_server, url} = start_server()
      opts = [api_key: "test-key", bounding_boxes: @boxes, url: url, to: self(), name: name]
      pid = start_supervised!({AisStream, opts})

      assert Process.whereis(name) == pid
    end

    test "child_spec/1 starts the client via start_link/1" do
      opts = [api_key: "test-key", bounding_boxes: @boxes, to: self()]
      assert %{start: {AisStream, :start_link, [^opts]}} = AisStream.child_spec(opts)
    end
  end

  describe "subscription" do
    test "negotiates permessage-deflate and subscribes right after connecting" do
      {_server, request, subscription} = start_client()

      assert request =~ ~r/sec-websocket-extensions:[^\r\n]*permessage-deflate/i
      assert subscription == %{"APIKey" => "test-key", "BoundingBoxes" => @boxes}
    end

    test "preserves the URL query in the upgrade target and excludes the fragment" do
      {_server, url} = start_server()
      url = url <> "?token=a%2Fb&filter=PositionReport#local-fragment"
      opts = [api_key: "test-key", bounding_boxes: @boxes, url: url, to: self()]
      start_supervised!({AisStream, opts})

      assert_receive {:connected, request, _}, 1_000
      assert request =~ "GET /v0/stream?token=a%2Fb&filter=PositionReport HTTP/1.1\r\n"
      refute request =~ "local-fragment"
    end

    test "uses a slash for an empty URL path while preserving the query" do
      {_server, url} = start_server()
      url = String.replace(url, "/v0/stream", "?token=example")
      opts = [api_key: "test-key", bounding_boxes: @boxes, url: url, to: self()]
      start_supervised!({AisStream, opts})

      assert_receive {:connected, request, _}, 1_000
      assert request =~ "GET /?token=example HTTP/1.1\r\n"
    end

    test "includes FilterMessageTypes when given" do
      {_server, _request, subscription} = start_client(filter_message_types: ["PositionReport"])

      assert subscription == %{
               "APIKey" => "test-key",
               "BoundingBoxes" => @boxes,
               "FilterMessageTypes" => ["PositionReport"]
             }
    end
  end

  describe "messages" do
    test "text and binary JSON frames are delivered as decoded maps" do
      {server, _, _} = start_client()

      send(server, {:frame, 1, ~s({"MessageType":"PositionReport","MetaData":{"MMSI":1}})})
      send(server, {:frame, 2, ~s({"MessageType":"ShipStaticData"})})

      assert_receive {:aisstream,
                      %{"MessageType" => "PositionReport", "MetaData" => %{"MMSI" => 1}}},
                     1_000

      assert_receive {:aisstream, %{"MessageType" => "ShipStaticData"}}, 1_000
    end

    test "error messages are passed through uninterpreted" do
      {server, _, _} = start_client()

      send(server, {:frame, 1, ~s({"error":"Api Key Is Not Valid"})})

      assert_receive {:aisstream, %{"error" => "Api Key Is Not Valid"}}, 1_000
    end

    test "invalid JSON is logged and skipped, the client keeps running" do
      {server, _, _} = start_client()

      log =
        capture_log(fn ->
          send(server, {:frame, 1, "not json"})
          send(server, {:frame, 1, ~s({"MessageType":"PositionReport"})})
          assert_receive {:aisstream, %{"MessageType" => "PositionReport"}}, 1_000
        end)

      assert log != ""
      refute_received {:aisstream, _}
    end

    test ":to accepts a registered name" do
      name = :"aisstream_test_sink_#{System.unique_integer([:positive])}"
      Process.register(self(), name)
      {server, _, _} = start_client(to: name)

      send(server, {:frame, 1, ~s({"MessageType":"PositionReport"})})

      assert_receive {:aisstream, %{"MessageType" => "PositionReport"}}, 1_000
    end

    test "stops with reason :normal when the :to pid exits" do
      {_server, url} = start_server()
      sink = spawn(fn -> receive(do: (:stop -> :ok)) end)
      opts = [api_key: "test-key", bounding_boxes: @boxes, url: url, to: sink]
      {:ok, pid} = AisStream.start_link(opts)
      ref = Process.monitor(pid)
      assert_receive {:connected, _, _}, 1_000

      send(sink, :stop)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000
    end

    test "keeps running when a registered :to exits and delivers to the new owner of the name" do
      name = :"aisstream_test_sink_#{System.unique_integer([:positive])}"
      sink = spawn(fn -> receive(do: (:stop -> :ok)) end)
      Process.register(sink, name)
      {server, url} = start_server()
      opts = [api_key: "test-key", bounding_boxes: @boxes, url: url, to: name]
      pid = start_supervised!({AisStream, opts})
      assert_receive {:connected, _, _}, 1_000

      ref = Process.monitor(sink)
      send(sink, :stop)
      assert_receive {:DOWN, ^ref, :process, ^sink, :normal}, 1_000
      Process.register(self(), name)
      send(server, {:frame, 1, ~s({"MessageType":"PositionReport"})})

      assert_receive {:aisstream, %{"MessageType" => "PositionReport"}}, 1_000
      assert Process.alive?(pid)
    end

    # The client's receipt of the frame is observed by tracing its mailbox; the
    # :sys call that follows returns only after that frame has been handled.
    test "drops messages while no process has the registered :to name, without crashing" do
      name = :"aisstream_test_sink_#{System.unique_integer([:positive])}"
      {server, url} = start_server()
      opts = [api_key: "test-key", bounding_boxes: @boxes, url: url, to: name]
      pid = start_supervised!({AisStream, opts})
      assert_receive {:connected, _, _}, 1_000

      log =
        capture_log(fn ->
          :erlang.trace(pid, true, [:receive])
          send(server, {:frame, 1, ~s({"MessageType":"PositionReport","MetaData":{"MMSI":1}})})
          assert_receive {:trace, ^pid, :receive, {:tcp, _, _}}, 1_000
          :erlang.trace(pid, false, [:receive])
          :sys.get_state(pid)

          Process.register(self(), name)
          send(server, {:frame, 1, ~s({"MessageType":"PositionReport","MetaData":{"MMSI":2}})})
          assert_receive {:aisstream, %{"MetaData" => %{"MMSI" => 2}}}, 1_000
        end)

      assert Process.alive?(pid)
      refute log =~ "terminating"
      refute_received {:aisstream, %{"MetaData" => %{"MMSI" => 1}}}
      refute_received {:connected, _, _}
    end
  end

  describe "HTTP upgrade errors" do
    test "reconnects after a rejected initial upgrade with a body without leaking the key" do
      {_server, url} = start_server(:reject_once)
      assert_upgrade_recovery(url)
    end

    test "reconnects after a rejected upgrade following an established connection" do
      {server, url} = start_server()
      assert_upgrade_recovery(url, fn -> send(server, :reject_next_upgrade) end)
    end
  end

  defp assert_upgrade_recovery(url, disconnect \\ nil) do
    key = "http-upgrade-secret-5Rt"
    opts = [api_key: key, bounding_boxes: @boxes, url: url, to: self()]

    log =
      capture_log(fn ->
        pid = start_supervised!(Supervisor.child_spec({AisStream, opts}, restart: :temporary))

        if disconnect do
          assert_receive {:connected, _, %{"APIKey" => ^key}}, 1_000
          disconnect.()
        end

        assert_receive {:upgrade_rejected, _}, 2_000
        assert_receive {:connected, _, %{"APIKey" => ^key}}, 4_000
        assert Process.alive?(pid)
        refute_received {:aisstream, _}
      end)

    refute log =~ "terminating"
    refute log =~ key
  end

  describe "reconnection" do
    # Waits for the real first backoff step (1 s): the spec gives no way to shorten it.
    test "reconnects after disconnection and subscribes again" do
      {server, _, subscription} = start_client()

      send(server, :close)

      assert_receive {:connected, _, ^subscription}, 5_000
      send(server, {:frame, 1, ~s({"MessageType":"PositionReport"})})
      assert_receive {:aisstream, %{"MessageType" => "PositionReport"}}, 1_000
    end

    test "backoff grows by 1 s per attempt up to 30 s" do
      assert Enum.map([1, 2, 3, 29, 30, 31, 1_000], &AisStream.backoff/1) ==
               [1_000, 2_000, 3_000, 29_000, 30_000, 30_000, 30_000]
    end

    # The attempt counter is observed by tracing the client's calls to backoff/1.
    test "backoff counter keeps growing when connections close before any message" do
      {server, url} = start_server()

      pid =
        start_supervised!({AisStream, api_key: "k", bounding_boxes: @boxes, url: url, to: self()})

      assert_receive {:connected, _, subscription}, 1_000
      :erlang.trace_pattern({AisStream, :backoff, 1}, true, [:local])
      :erlang.trace(pid, true, [:call])

      send(server, :close)
      assert_receive {:trace, ^pid, :call, {AisStream, :backoff, [1]}}, 1_000
      assert_receive {:connected, _, ^subscription}, 2_000
      send(server, :close)

      assert_receive {:trace, ^pid, :call, {AisStream, :backoff, [2]}}, 1_000
    end

    test "backoff counter resets once a message arrives on a connection" do
      {server, url} = start_server()

      pid =
        start_supervised!({AisStream, api_key: "k", bounding_boxes: @boxes, url: url, to: self()})

      assert_receive {:connected, _, subscription}, 1_000
      :erlang.trace_pattern({AisStream, :backoff, 1}, true, [:local])
      :erlang.trace(pid, true, [:call])

      send(server, :close)
      assert_receive {:trace, ^pid, :call, {AisStream, :backoff, [1]}}, 1_000
      assert_receive {:connected, _, ^subscription}, 2_000
      send(server, {:frame, 1, ~s({"MessageType":"PositionReport"})})
      assert_receive {:aisstream, _}, 1_000
      # Drop calls made for the first disconnection, so only the next ones count.
      flush_traces(pid)
      send(server, :close)

      assert_receive {:trace, ^pid, :call, {AisStream, :backoff, [1]}}, 1_000
    end

    test "an error message is delivered but does not reset the backoff counter" do
      {server, url} = start_server()

      pid =
        start_supervised!({AisStream, api_key: "k", bounding_boxes: @boxes, url: url, to: self()})

      assert_receive {:connected, _, subscription}, 1_000
      :erlang.trace_pattern({AisStream, :backoff, 1}, true, [:local])
      :erlang.trace(pid, true, [:call])

      send(server, :close)
      assert_receive {:trace, ^pid, :call, {AisStream, :backoff, [1]}}, 1_000
      assert_receive {:connected, _, ^subscription}, 2_000
      send(server, {:frame, 1, ~s({"error":"Api Key Is Not Valid"})})
      assert_receive {:aisstream, %{"error" => "Api Key Is Not Valid"}}, 1_000
      flush_traces(pid)
      send(server, :close)

      assert_receive {:trace, ^pid, :call, {AisStream, :backoff, [attempt]}}, 1_000
      assert attempt == 2
    end
  end

  describe "close handshake" do
    for {label, payload} <- [
          {"with a code and reason", <<1_001::16, "going away">>},
          {"with a code only", <<1_000::16>>},
          {"without a payload", ""}
        ] do
      @tag :close_handshake
      test "acknowledges a close #{label} before reconnecting" do
        payload = unquote(payload)
        {server, url} = start_server()
        opts = [api_key: "test-key", bounding_boxes: @boxes, url: url, to: self()]
        pid = start_supervised!(Supervisor.child_spec({AisStream, opts}, restart: :temporary))
        assert_receive {:connected, _, subscription}, 1_000

        send(server, {:frame, 8, payload})

        # Mint normalizes an empty close to code 1000.
        reply = if payload == "", do: <<1_000::16>>, else: payload
        assert_receive {:client_frame, 8, ^reply}, 1_000
        assert_receive {:connected, _, ^subscription}, 3_000
        assert Process.alive?(pid)

        send(server, {:frame, 1, ~s({"MessageType":"PositionReport"})})
        assert_receive {:aisstream, %{"MessageType" => "PositionReport"}}, 1_000
      end
    end
  end

  describe "keepalive" do
    test "sends a ping at the check when something was received" do
      {server, _, _} = start_client(ping_interval: 50)

      send(server, {:frame, 1, ~s({"MessageType":"PositionReport"})})

      assert_receive {:client_frame, 9, _}, 500
    end

    test "reconnects and subscribes again when nothing arrived during an interval" do
      {_server, _, subscription} = start_client(ping_interval: 50)

      assert_receive {:connected, _, ^subscription}, 2_000
    end

    test "stays connected while pings are answered with pongs, without AIS messages" do
      {server, _, _} = start_client(ping_interval: 50)

      # A server ping is the first thing received; no AIS message is ever sent.
      send(server, {:frame, 9, ""})

      # A reconnection would leave a gap of at least 1 s (backoff) between pings.
      for _ <- 1..5 do
        assert_receive {:client_frame, 9, payload}, 500
        send(server, {:frame, 10, payload})
      end

      refute_received {:connected, _, _}
    end

    # The first check (50 ms) plus the first backoff step (1 s) bound the wait.
    test "reconnects when the server never answers the upgrade request" do
      {_server, url} = start_server(:no_upgrade)

      opts = [
        api_key: "test-key",
        bounding_boxes: @boxes,
        url: url,
        to: self(),
        ping_interval: 50
      ]

      start_supervised!({AisStream, opts})

      assert_receive {:upgrade_request, _}, 1_000
      assert_receive {:upgrade_request, _}, 2_000
    end
  end

  describe "api key" do
    test "the key can be given as a zero-arity function, called once" do
      test = self()

      fun = fn ->
        send(test, :api_key_fun_called)
        "fn-key"
      end

      {server, _, subscription} = start_client(api_key: fun)
      assert subscription["APIKey"] == "fn-key"

      send(server, :close)

      assert_receive {:connected, _, %{"APIKey" => "fn-key"}}, 5_000
      assert_received :api_key_fun_called
      refute_received :api_key_fun_called
    end

    test "child_spec/1 with the function form does not contain the key" do
      spec =
        AisStream.child_spec(
          api_key: fn -> "child-spec-secret-7Qz" end,
          bounding_boxes: @boxes,
          to: self()
        )

      refute inspect(spec, limit: :infinity, printable_limit: :infinity) =~
               "child-spec-secret-7Qz"
    end

    # Crash trigger: a call the client does not handle makes the GenServer raise.
    test "the crash report does not contain the key" do
      key = "crash-report-secret-9Xk"
      {_server, url} = start_server()
      opts = [api_key: key, bounding_boxes: @boxes, url: url, to: self()]
      pid = start_supervised!(Supervisor.child_spec({AisStream, opts}, restart: :temporary))
      assert_receive {:connected, _, %{"APIKey" => ^key}}, 1_000
      ref = Process.monitor(pid)

      log =
        capture_log(fn ->
          catch_exit(GenServer.call(pid, :not_handled))
          assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000
        end)

      assert log =~ "terminating"
      refute log =~ "(truncated)"
      refute log =~ key
    end

    test ":sys.get_status/1 does not contain the key and the client keeps working" do
      key = "get-status-secret-4Wm"
      {server, url} = start_server()

      pid =
        start_supervised!({AisStream, api_key: key, bounding_boxes: @boxes, url: url, to: self()})

      assert_receive {:connected, _, %{"APIKey" => ^key}}, 1_000

      status = :sys.get_status(pid)

      refute inspect(status, limit: :infinity, printable_limit: :infinity) =~ key
      send(server, {:frame, 1, ~s({"MessageType":"PositionReport"})})
      assert_receive {:aisstream, %{"MessageType" => "PositionReport"}}, 1_000
    end
  end

  # Never-reading consumers are sink processes; a ping answered with a pong
  # proves the client handled every frame sent before it.
  describe "max_queue_len" do
    test "without the option every message is delivered" do
      sink = start_sink()
      {server, _, _} = start_client(to: sink)

      send_positions(server, 1..10)
      sync(server, "s1")

      assert queue_len(sink) == 10
    end

    test "drops messages while the local consumer's queue is at the limit" do
      sink = start_sink()
      {server, url} = start_server()
      opts = [api_key: "test-key", bounding_boxes: @boxes, url: url, to: sink, max_queue_len: 3]

      capture_log(fn ->
        pid = start_supervised!({AisStream, opts})
        assert_receive {:connected, _, _}, 1_000

        send_positions(server, 1..10)
        sync(server, "s1")

        assert queue_len(sink) == 3
        assert Process.alive?(pid)
        sync(server, "s2")
        refute_received {:connected, _, _}
        assert mmsis(drain(sink)) == [1, 2, 3]
      end)
    end

    test "many dropped messages log a single warning" do
      sink = start_sink()
      {server, url} = start_server()
      opts = [api_key: "test-key", bounding_boxes: @boxes, url: url, to: sink, max_queue_len: 2]

      {pid, log} =
        with_log([level: :warning, metadata: [:pid]], fn ->
          pid = start_supervised!({AisStream, opts})
          assert_receive {:connected, _, _}, 1_000

          send_positions(server, 1..20)
          sync(server, "s1")
          drain(sink)
          # Crossing the limit again within 60 s must not log again.
          send_positions(server, 21..40)
          sync(server, "s2")
          pid
        end)

      tag = "pid=#{:erlang.pid_to_list(pid)} "
      lines = log |> String.split("\n") |> Enum.filter(&(&1 =~ tag and &1 =~ "[warning]"))

      assert length(lines) == 1
      assert Process.alive?(pid)
      assert queue_len(sink) == 2
    end

    test "delivery resumes once the consumer drains its mailbox" do
      sink = start_sink()
      {server, url} = start_server()
      opts = [api_key: "test-key", bounding_boxes: @boxes, url: url, to: sink, max_queue_len: 2]

      capture_log(fn ->
        start_supervised!({AisStream, opts})
        assert_receive {:connected, _, _}, 1_000

        send_positions(server, 1..5)
        sync(server, "s1")
        assert mmsis(drain(sink)) == [1, 2]

        send_positions(server, 6..7)
        sync(server, "s2")
        assert mmsis(drain(sink)) == [6, 7]
      end)
    end

    for value <- [0, -1, 1.5, "10"] do
      test "max_queue_len: #{inspect(value)} raises ArgumentError" do
        {_server, url} = start_server()
        opts = [api_key: "test-key", bounding_boxes: @boxes, url: url, to: self()]

        assert_raise ArgumentError, fn ->
          AisStream.start_link(opts ++ [max_queue_len: unquote(value)])
        end
      end
    end

    test "max_queue_len: nil is accepted" do
      {server, _, _} = start_client(max_queue_len: nil)

      send(server, {:frame, 1, ~s({"MessageType":"PositionReport"})})

      assert_receive {:aisstream, %{"MessageType" => "PositionReport"}}, 1_000
    end
  end

  @tag :live
  test "live: receives AIS messages from stream.aisstream.io" do
    start_supervised!(
      {AisStream,
       api_key: System.fetch_env!("AISSTREAM_API_KEY"),
       bounding_boxes: [[[-90, -180], [90, 180]]],
       to: self()}
    )

    assert_receive {:aisstream, %{"MessageType" => _}}, 30_000
  end

  # Starts a client against a local server and waits for its subscription.
  defp start_client(opts \\ []) do
    {server, url} = start_server()

    start_supervised!(
      {AisStream,
       Keyword.merge([api_key: "test-key", bounding_boxes: @boxes, url: url, to: self()], opts)}
    )

    assert_receive {:connected, request, subscription}, 1_000
    {server, request, subscription}
  end

  # Minimal WebSocket server on :gen_tcp, one connection at a time. It declines
  # permessage-deflate (allowed by RFC 7692), so frames are sent uncompressed.
  # Reports {:connected, request, subscription} to the test, then obeys
  # {:frame, opcode, payload} and :close, then accepts the next connection.
  # Frames sent by the client after the subscription are reported to the test
  # as {:client_frame, opcode, payload}; when the client closes the connection
  # the server accepts the next one.
  # With :no_upgrade it reports {:upgrade_request, request} for each connection,
  # never replies and keeps the socket open, then accepts the next connection.
  defp start_server(mode \\ :upgrade) do
    test = self()
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(listen)
    server = spawn_link(fn -> serve(listen, test, mode) end)
    {server, "ws://127.0.0.1:#{port}/v0/stream"}
  end

  defp serve(listen, test, :no_upgrade) do
    {:ok, socket} = :gen_tcp.accept(listen)
    send(test, {:upgrade_request, recv_request(socket, "")})
    serve(listen, test, :no_upgrade)
  end

  defp serve(listen, test, :reject_once) do
    {:ok, socket} = :gen_tcp.accept(listen)
    request = recv_request(socket, "")

    :ok =
      :gen_tcp.send(
        socket,
        "HTTP/1.1 503 Service Unavailable\r\ncontent-length: 4\r\n\r\n" <> <<0x81, 2, "{}">>
      )

    send(test, {:upgrade_rejected, request})
    # Wait for the client to close, so the body is processed before disconnection.
    {:error, :closed} = :gen_tcp.recv(socket, 0, 5_000)
    :gen_tcp.close(socket)
    serve(listen, test, :upgrade)
  end

  defp serve(listen, test, :upgrade) do
    {:ok, socket} = :gen_tcp.accept(listen)
    request = recv_request(socket, "")
    [_, key] = Regex.run(~r/sec-websocket-key:\s*(\S+)/i, request)
    accept = Base.encode64(:crypto.hash(:sha, key <> "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))

    :ok =
      :gen_tcp.send(
        socket,
        "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nconnection: Upgrade\r\n" <>
          "sec-websocket-accept: #{accept}\r\n\r\n"
      )

    {1, subscription} = recv_frame(socket)
    send(test, {:connected, request, JSON.decode!(subscription)})
    server = self()
    spawn(fn -> read_frames(socket, server, test) end)
    mode = if command_loop(socket) == :reject_next_upgrade, do: :reject_once, else: :upgrade
    serve(listen, test, mode)
  end

  defp command_loop(socket) do
    receive do
      {:frame, opcode, payload} when byte_size(payload) < 126 ->
        :ok =
          :gen_tcp.send(socket, <<1::1, 0::3, opcode::4, 0::1, byte_size(payload)::7>> <> payload)

        command_loop(socket)

      :reject_next_upgrade ->
        :gen_tcp.close(socket)
        :reject_next_upgrade

      msg when msg in [:close, {:closed, socket}] ->
        :gen_tcp.close(socket)
    end
  end

  # A consumer that never reads {:aisstream, _}; {:drain, from} empties its mailbox.
  defp start_sink do
    spawn_link(fn -> sink_loop() end)
  end

  defp sink_loop do
    receive do
      {:drain, from} -> send(from, {:drained, flush_mailbox([])})
    end

    sink_loop()
  end

  defp flush_mailbox(acc) do
    receive do
      msg -> flush_mailbox([msg | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp drain(sink) do
    send(sink, {:drain, self()})
    assert_receive {:drained, messages}, 1_000
    messages
  end

  defp queue_len(pid) do
    {:message_queue_len, len} = Process.info(pid, :message_queue_len)
    len
  end

  defp mmsis(messages), do: Enum.map(messages, fn {:aisstream, m} -> m["MetaData"]["MMSI"] end)

  defp send_positions(server, range) do
    for i <- range do
      send(server, {:frame, 1, ~s({"MessageType":"PositionReport","MetaData":{"MMSI":#{i}}})})
    end
  end

  defp sync(server, tag) do
    send(server, {:frame, 9, tag})
    assert_receive {:client_frame, 10, ^tag}, 1_000
  end

  defp flush_traces(pid) do
    receive do
      {:trace, ^pid, :call, _} -> flush_traces(pid)
    after
      0 -> :ok
    end
  end

  defp read_frames(socket, server, test) do
    case recv_frame(socket, :infinity) do
      {:error, _} ->
        send(server, {:closed, socket})

      {opcode, payload} ->
        send(test, {:client_frame, opcode, payload})
        read_frames(socket, server, test)
    end
  end

  defp recv_request(socket, acc) do
    if String.contains?(acc, "\r\n\r\n") do
      acc
    else
      {:ok, data} = :gen_tcp.recv(socket, 0, 1_000)
      recv_request(socket, acc <> data)
    end
  end

  # Client frames are always masked (RFC 6455 5.3).
  defp recv_frame(socket, timeout \\ 1_000) do
    with {:ok, <<_fin_rsv::4, opcode::4, 1::1, len::7>>} <- :gen_tcp.recv(socket, 2, timeout),
         {:ok, len} <-
           (case len do
              126 -> with {:ok, <<n::16>>} <- :gen_tcp.recv(socket, 2, 1_000), do: {:ok, n}
              127 -> with {:ok, <<n::64>>} <- :gen_tcp.recv(socket, 8, 1_000), do: {:ok, n}
              n -> {:ok, n}
            end),
         {:ok, <<mask::binary-4, masked::binary>>} <- :gen_tcp.recv(socket, len + 4, 1_000) do
      {opcode, :crypto.exor(masked, binary_part(:binary.copy(mask, div(len, 4) + 1), 0, len))}
    end
  end
end
