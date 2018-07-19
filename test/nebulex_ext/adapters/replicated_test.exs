defmodule NebulexExt.Adapters.ReplicatedTest do
  use Nebulex.NodeCase
  use Nebulex.CacheTest, cache: NebulexExt.TestCache.Replicated

  alias NebulexExt.TestCache.Replicated
  alias NebulexExt.TestCache.Local

  @primary :"primary@127.0.0.1"
  @cluster :lists.usort([@primary | Application.get_env(:nebulex, :nodes, [])])

  setup do
    {:ok, local} = Local.start_link
    {:ok, replicated} = Replicated.start_link
    node_pid_list = start_caches(Node.list(), [Local, Replicated])
    :ok

    on_exit fn ->
      _ = :timer.sleep(100)
      if Process.alive?(local), do: Local.stop(local, 1)
      if Process.alive?(replicated), do: Replicated.stop(replicated, 1)
      stop_caches(node_pid_list)
    end
  end

  test "fail on __before_compile__ because missing local cache" do
    assert_raise ArgumentError, ~r"missing :local configuration", fn ->
      defmodule WrongReplicated do
        use Nebulex.Cache, otp_app: :nebulex, adapter: NebulexExt.Adapters.Replicated
      end
    end
  end

  test "check cluster nodes" do
    assert node() == @primary
    assert :lists.usort(Node.list()) == @cluster -- [node()]
    assert Replicated.nodes() == @cluster
  end

  test "write operations are replicated to all the local caches" do
    for node <- Replicated.nodes do
      assert :rpc.call(node, Replicated.__local__, :get, [1]) == nil
      assert :rpc.call(node, Replicated.__local__, :get, [2]) == nil
      assert :rpc.call(node, Replicated.__local__, :get, [3]) == nil
      assert :rpc.call(node, Replicated.__local__, :get, [4]) == nil
    end

    assert Replicated.get_and_update(1, &Replicated.get_and_update_fun/1) == {nil, 1}
    assert Replicated.set(2, 2) == 2
    assert Replicated.update(3, 3, &Integer.to_string/1) == 3
    assert Replicated.update_counter(4, 4) == 4

    for node <- Replicated.nodes do
      assert :rpc.call(node, Replicated.__local__, :get, [1]) == 1
      assert :rpc.call(node, Replicated.__local__, :get, [2]) == 2
      assert :rpc.call(node, Replicated.__local__, :get, [3]) == 3
      assert :rpc.call(node, Replicated.__local__, :get, [4]) == 4
    end

    assert Replicated.delete(1) == nil

    for node <- Replicated.nodes do
      assert :rpc.call(node, Replicated.__local__, :get, [1]) == nil
    end

    assert Replicated.flush == :ok

    for node <- Replicated.nodes do
      assert :rpc.call(node, Replicated.__local__, :get, [2]) == nil
      assert :rpc.call(node, Replicated.__local__, :get, [3]) == nil
      assert :rpc.call(node, Replicated.__local__, :get, [4]) == nil
    end
  end

  test "write operations override all the local caches despite of version conflicts" do
    for node <- Node.list() do
      assert :rpc.call(node, Replicated.__local__, :set, [1, 10, [version: -10]]) == 10
      assert :rpc.call(node, Replicated.__local__, :set, [2, 20, [version: -10]]) == 20
      assert :rpc.call(node, Replicated.__local__, :set, [3, 30, [version: -10]]) == 30
      assert :rpc.call(node, Replicated.__local__, :set, [4, 40, [version: -10]]) == 40
    end

    assert Replicated.get_and_update(1, &Replicated.get_and_update_fun/1, version: 5) == {nil, 1}
    assert Replicated.set(2, 22, version: 5) == 22
    assert Replicated.update(3, 33, &Integer.to_string/1, version: 5) == 33
    assert Replicated.update_counter(4, 44, version: 5) == 44

    for node <- Replicated.nodes do
      assert :rpc.call(node, Replicated.__local__, :get, [1]) == 1
      assert :rpc.call(node, Replicated.__local__, :get, [2]) == 22
      assert :rpc.call(node, Replicated.__local__, :get, [3]) == 33
      assert :rpc.call(node, Replicated.__local__, :get, [4]) == 44
    end
  end

  test "get_and_update" do
    assert Replicated.get_and_update(1, &Replicated.get_and_update_fun/1) == {nil, 1}
    assert Replicated.get_and_update(1, &Replicated.get_and_update_fun/1) == {1, 2}
    assert Replicated.get_and_update(1, &Replicated.get_and_update_fun/1) == {2, 4}

    for node <- Replicated.nodes do
      assert :rpc.call(node, Replicated.__local__, :get, [1]) == 4
    end

    assert_raise ArgumentError, fn ->
      Replicated.get_and_update(1, &Replicated.wrong_get_and_update_fun/1)
    end

    assert_raise Nebulex.ConflictError, fn ->
      1
      |> Replicated.set(1, return: :key)
      |> Replicated.get_and_update(&Replicated.get_and_update_fun/1, version: -1)
    end
  end
end
