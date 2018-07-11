defmodule NebulexExt.Version.AutoIncr do
  @moduledoc false
  @behaviour Nebulex.Object.Version

  @impl true
  def generate(object) do
    case object.version do
      nil -> 1
      v   -> v + 1
    end
  end
end
