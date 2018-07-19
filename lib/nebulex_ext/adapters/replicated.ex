defmodule NebulexExt.Adapters.Replicated do
  @moduledoc """
  Adapter module for replicated cache.

  This adapter depends on a local cache adapter, it adds a thin layer
  on top of it in order to replicate requests across a group of nodes,
  where is supposed the local cache is running already.

  PG2 is used by the adapter to manage the cluster nodes. When the replicated
  cache is started in a node, it creates a PG2 group and joins it (the cache
  supervisor PID is joined to the group).
  When a write function is invoked, the adapter executes it in all the PG2 group members
  (i.e. the data is replicated to all the nodes).
  When a read function is invoked, the adapter executes it locally (using the local adapter).

  ## Features

    * Support for Replicated Cache
    * Support for transactions via Erlang global name registration facility

  ## Options

  These options should be set in the config file and require
  recompilation in order to make an effect.

    * `:adapter` - The adapter name, in this case, `NebulexExt.Adapters.Replicated`.

    * `:local` - The Local Cache module. The value to this option should be
      `Nebulex.Adapters.Local`, unless you want to provide a custom local
      cache adapter.

    * `:rpc_timeout` - Timeout used on the rpc calls

  ## Example

  `Nebulex.Cache` is the wrapper around the Cache. We can define the
  local and replicated cache as follows:

      defmodule MyApp.LocalCache do
        use Nebulex.Cache, otp_app: :my_app, adapter: Nebulex.Adapters.Local
      end

      defmodule MyApp.ReplicatedCache do
        use Nebulex.Cache, otp_app: :my_app, adapter: NebulexExt.Adapters.Replicated
      end

  Where the configuration for the Cache must be in your application
  environment, usually defined in your `config/config.exs`:

      config :my_app, MyApp.LocalCache,
        n_shards: 2,
        gc_interval: 3600

      config :my_app, MyApp.ReplicatedCache,
        local: MyApp.LocalCache

  For more information about the usage, check `Nebulex.Cache`.

  ## Extended API

  This adapter provides some additional functions to the `Nebulex.Cache` API.

  ### `nodes/0`

  Returns the nodes that belongs to the caller Cache.

      MyCache.nodes()

  ### `new_generation/1`

  Creates a new generation in all nodes that belongs to the caller Cache.

      MyCache.new_generation()

  ## Limitations

  This adapter has some limitation for two functions: `get_and_update/4` and
  `update/5`. They both have a parameter that is an anonymous function, and
  the anonymous function is compiled into the module where it was created,
  which means it necessarily doesn't exists on remote nodes. To ensure they
  work as expected, you must provide functions from modules existing in all
  nodes of the group.
  """

  # Inherit default transaction implementation
  use NebulexExt.Adapter.Transaction

  # Provide Cache Implementation
  @behaviour Nebulex.Adapter

  ## Adapter Impl

  @impl true
  defmacro __before_compile__(env) do
    otp_app = Module.get_attribute(env.module, :otp_app)
    config = Module.get_attribute(env.module, :config)
    rpc_timeout = Keyword.get(config, :rpc_timeout, 1000)

    unless local = Keyword.get(config, :local) do
      raise ArgumentError,
        "missing :local configuration in " <>
        "config #{inspect otp_app}, #{inspect env.module}"
    end

    quote do
      alias Nebulex.Adapters.Local.Generation

      def __local__, do: unquote(local)

      def __rpc_timeout__, do: unquote(rpc_timeout)

      def nodes do
        pg2_namespace()
        |> :pg2.get_members()
        |> Enum.map(&node(&1))
        |> :lists.usort
      end

      def new_generation(opts \\ []) do
        {res, _} = :rpc.multicall(nodes(), Generation, :new, [unquote(local), opts])
        res
      end

      def init(config) do
        :ok = :pg2.create(pg2_namespace())

        unless self() in :pg2.get_members(pg2_namespace()) do
          :ok = :pg2.join(pg2_namespace(), self())
        end

        {:ok, config}
      end

      defp pg2_namespace, do: {:nebulex, __MODULE__}
    end
  end

  ## Adapter Impl

  ## Local operations

  @impl true
  def init(_cache, _opts), do: {:ok, []}

  @impl true
  def get(cache, key, opts), do: cache.__local__.get(key, opts)

  @impl true
  def has_key?(cache, key), do: cache.__local__.has_key?(key)

  @impl true
  def keys(cache), do: cache.__local__.keys()

  @impl true
  def size(cache), do: cache.__local__.size()

  @impl true
  def reduce(cache, acc_in, fun, opts), do: cache.__local__.reduce(acc_in, fun, opts)

  @impl true
  def to_map(cache, opts), do: cache.__local__.to_map(opts)

  @impl true
  def pop(cache, key, opts), do: cache.__local__.pop(key, opts)

  ## Cluster operations

  @impl true
  def set(cache, key, value, opts) do
    cache
    |> exec_and_broadcast(:set, [key, value, opts])
  end

  @impl true
  def update(cache, key, initial, fun, opts) do
    cache
    |> exec_and_broadcast(:update, [key, initial, fun, opts])
  end

  @impl true
  def update_counter(cache, key, incr, opts) do
    cache
    |> exec_and_broadcast(:update_counter, [key, incr, opts])
  end

  @impl true
  def get_and_update(cache, key, fun, opts) when is_function(fun, 1) do
    cache
    |> exec_and_broadcast(:get_and_update, [key, fun, opts])
  end

  @impl true
  def delete(cache, key, opts), do: multicall(cache, :delete, [key, opts])

  @impl true
  def flush(cache), do: multicall(cache, :flush, [])

  ## Private Functions

  defp exec_and_broadcast(cache, operation, args = [key | _]) do
    transaction(cache, [keys: [key], nodes: cache.nodes], fn ->
      result = apply(cache.__local__, operation, args)

      value =
        case operation do
          :get_and_update -> elem(result, 1)
          _ -> result
        end

      set_args = [key, value, [{:on_conflict, :override}]]
      _ = multicall(cache, :set, set_args, [include_local: false, use_transaction: false])

      result
    end)
  end

  defp multicall(cache, fun, args, opts \\ []) do
    case Keyword.get(opts, :use_transaction, true) do
      true  -> multicall_transaction(cache, fun, args, opts)
      false -> do_multicall(cache, fun, args, opts)
    end
  end

  defp multicall_transaction(cache, fun, args, opts) do
    trans_opts = [
      keys: trans_keys(args),
      nodes: multicall_nodes(cache, opts)
    ]

    transaction(cache, trans_opts, fn ->
      do_multicall(cache, fun, args, opts)
    end)
  end

  defp trans_keys(_args = []), do: []
  defp trans_keys(_args = [key | _]), do: [key]

  defp do_multicall(cache, fun, args, opts) do
    cache
    |> multicall_nodes(opts)
    |> :rpc.multicall(cache.__local__, fun, args, cache.__rpc_timeout__)
    |> parse_multicall_result
  end

  defp multicall_nodes(cache, opts) do
    case Keyword.get(opts, :include_local, true) do
      true  -> cache.nodes
      false -> List.delete(cache.nodes, node())
    end
  end

  defp parse_multicall_result({[], []}) do
    nil
  end

  defp parse_multicall_result({results, []}) do
    case badrpc_reasons(results) do
      [{:EXIT, {remote_ex, _}} | _] -> raise remote_ex
      [reason | _] -> {:error, {:badrpc, reason}}
      [] -> hd(results)
    end
  end

  defp parse_multicall_result({_, bad_nodes}) do
    raise "bad nodes on multicall: #{bad_nodes}"
  end

  defp badrpc_reasons(results), do: for {:badrpc, reason} <- results, do: reason
end
