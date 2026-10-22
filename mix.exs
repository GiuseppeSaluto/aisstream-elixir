defmodule AisStream.MixProject do
  use Mix.Project

  @source_url "https://github.com/GiuseppeSaluto/aisstream-elixir"

  def project do
    [
      app: :aisstream,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description:
        "Unofficial client for the AISStream WebSocket API, with compression and automatic reconnection.",
      source_url: @source_url,
      package: [licenses: ["MIT"], links: %{"GitHub" => @source_url}],
      docs: [main: "readme", extras: ["README.md", "LICENSE"]]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:mint_web_socket, "~> 1.0"},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false}
    ]
  end
end
