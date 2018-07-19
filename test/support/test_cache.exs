defmodule NebulexExt.TestCache do
  :ok = Application.put_env(:nebulex, NebulexExt.TestCache.Local, [n_shards: 2, version_generator: NebulexExt.Version.AutoIncr])
  :ok = Application.put_env(:nebulex, NebulexExt.TestCache.Replicated, [local: NebulexExt.TestCache.Local])

  defmodule Local do
    use Nebulex.Cache, otp_app: :nebulex, adapter: Nebulex.Adapters.Local
  end

  defmodule Replicated do
    use Nebulex.Cache, otp_app: :nebulex, adapter: NebulexExt.Adapters.Replicated

    def get_and_update_fun(nil), do: {nil, 1}
    def get_and_update_fun(current) when is_integer(current), do: {current, current * 2}

    def wrong_get_and_update_fun(_), do: :other
  end
end
