defmodule NebulexExt.MixProject do
  use Mix.Project

  def project do
    [
      app: :nebulex_ext,
      version: "0.1.0",
      elixir: "~> 1.6",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:nebulex, github: "cabol/nebulex", branch: "master"}
    ]
  end
end
