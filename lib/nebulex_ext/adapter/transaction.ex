defmodule NebulexExt.Adapter.Transaction do
  @moduledoc """
  Extension for Nebulex.Adapter.Transaction

  It adds support for nested transactions
  """

  @doc false
  defmacro __using__(_opts) do
    quote do
      use Nebulex.Adapter.Transaction

      defoverridable [do_transaction: 5, lock_ids: 2]

      ## Helpers

      defp do_transaction(cache, keys, nodes, retries, fun) do
        {new_ids, current_ids} = lock_ids(cache, keys)

        case set_locks(new_ids, nodes, retries) do
          true ->
            try do
              Process.put(cache, %{lock_ids: new_ids ++ current_ids, nodes: nodes})
              fun.()
            after
              after_transaction(cache, new_ids, current_ids, nodes)
            end
          false ->
            raise "transaction aborted"
        end
      end

      defp lock_ids(cache, keys) do
        current_ids = Process.get(cache, [])

        new_ids =
          generate_lock_ids(cache, keys)
          |> Enum.filter(fn({id, _}) -> not Enum.member?(current_ids, id) end)

        {new_ids, current_ids}
      end

      defp generate_lock_ids(cache, nil) do
        [{cache, self()}]
      end

      defp generate_lock_ids(cache, keys) do
        for key <- keys, do: {{cache, key}, self()}
      end

      defp after_transaction(cache, new_ids, current_ids, nodes) do
        case current_ids do
          [] -> Process.delete(cache)
          _  -> Process.put(cache, %{lock_ids: current_ids, nodes: nodes})
        end
        del_locks(new_ids, nodes)
      end
    end
  end
end
