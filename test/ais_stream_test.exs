defmodule AisStreamTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  @boxes [[[45.0, 10.0], [46.5, 12.25]]]

  describe "options" do
    test "missing :api_key raises KeyError" do
      assert_raise KeyError, fn -> AisStream.start_link(bounding_boxes: @boxes) end
    end

    test "missing :bounding_boxes raises KeyError" do
      assert_raise KeyError, fn -> AisStream.start_link(api_key: "test-key") end
    end

    test "child_spec/1 starts the client via start_link/1" do
      opts = [api_key: "test-key", bounding_boxes: @boxes]
      assert %{start: {AisStream, :start_link, [^opts]}} = AisStream.child_spec(opts)
    end
  end

  describe "subscription" do
    test "negotiates permessage-deflate and subscribes right after connecting" do
      {_server, request, subscription} = start_client()

      assert request =~ ~r/sec-websocket-extensions:[^\r\n]*permessage-deflate/i
      assert subscription == %{"APIKey" => "test-key", "BoundingBoxes" => @boxes}
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

    test ":to defaults to the caller of start_link" do
      {server, url} = start_server()
      {:ok, _pid} = AisStream.start_link(api_key: "test-key", bounding_boxes: @boxes, url: url)
      assert_receive {:connected, _, _}, 1_000

      send(server, {:frame, 1, ~s({"MessageType":"PositionReport"})})

      assert_receive {:aisstream, %{"MessageType" => "PositionReport"}}, 1_000
    end

    test ":to accepts a registered name" do
      name = :"aisstream_test_sink_#{System.unique_integer([:positive])}"
      Process.register(self(), name)
      {server, _, _} = start_client(to: name)

      send(server, {:frame, 1, ~s({"MessageType":"PositionReport"})})

      assert_receive {:aisstream, %{"MessageType" => "PositionReport"}}, 1_000
    end
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
  defp start_server do
    test = self()
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(listen)
    server = spawn_link(fn -> serve(listen, test) end)
    {server, "ws://127.0.0.1:#{port}/v0/stream"}
  end

  defp serve(listen, test) do
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
    command_loop(socket)
    serve(listen, test)
  end

  defp command_loop(socket) do
    receive do
      {:frame, opcode, payload} when byte_size(payload) < 126 ->
        :ok =
          :gen_tcp.send(socket, <<1::1, 0::3, opcode::4, 0::1, byte_size(payload)::7>> <> payload)

        command_loop(socket)

      :close ->
        :gen_tcp.close(socket)
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
  defp recv_frame(socket) do
    {:ok, <<_fin_rsv::4, opcode::4, 1::1, len::7>>} = :gen_tcp.recv(socket, 2, 1_000)

    len =
      case len do
        126 -> with {:ok, <<n::16>>} <- :gen_tcp.recv(socket, 2, 1_000), do: n
        127 -> with {:ok, <<n::64>>} <- :gen_tcp.recv(socket, 8, 1_000), do: n
        n -> n
      end

    {:ok, mask} = :gen_tcp.recv(socket, 4, 1_000)
    {:ok, masked} = :gen_tcp.recv(socket, len, 1_000)
    {opcode, :crypto.exor(masked, binary_part(:binary.copy(mask, div(len, 4) + 1), 0, len))}
  end
end
