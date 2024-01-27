defmodule CLI do
  def main(argv) do
    factory = stream_factory(argv)

    mode = :enum_stats
    parallel = true

    {process, merge, decode} =
      case mode do
        :enum_stats ->
          {
            fn -> EnumStats.process(factory) end,
            fn a, b -> EnumStats.merge(a, b) end,
            fn a -> DecodeEnumStats.decode(a) end
          }

        :enums ->
          {Enums.process(), Enums.merge(), DecodeEnums.decode()}

        :keys ->
          {Keys.process(), Keys.merge(), DecodeKeys.decode()}
      end

    case parallel do
      true ->
        {:ok, _} = WorkCollector.start_link({self(), process, merge, decode})

        receive do
          {:done, result} -> IO.puts(result)
        end

      false ->
        process.()
        |> decode.()
        |> Jason.encode!()
        |> IO.puts()
    end
  end

  defp stream_factory([path]) do
    {:ok, pid} = FileServer.start_link(path)

    fn ->
      Stream.resource(
        fn -> {} end,
        fn _ ->
          case GenServer.call(pid, :read) do
            :eof -> {:halt, {}}
            data -> {[data], {}}
          end
        end,
        fn _ -> :ok end
      )
    end
  end

  defp stream_factory([]) do
    {:ok, pid} = StdinServer.start_link()

    fn ->
      Stream.resource(
        fn -> {} end,
        fn _ ->
          case GenServer.call(pid, :read) do
            :eof -> {:halt, {}}
            data -> {[data], {}}
          end
        end,
        fn _ -> :ok end
      )
    end
  end
end

defmodule WorkCollector do
  use GenServer
  require Progress

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init({from, process, merge, decode}) do
    worker_count = System.schedulers_online() * 2

    Progress.start_link(1000 * worker_count)

    for n <- 1..worker_count do
      IO.puts(:stderr, "Starting worker #{n}")

      Task.async(fn ->
        result = process.()
        GenServer.cast(__MODULE__, {:done, result})
      end)
    end

    {:ok, {from, worker_count, %{}, merge, decode}}
  end

  def handle_cast({:done, result}, {from, counter, previous, merge, decode}) do
    result = merge.(previous, result)
    counter = counter - 1

    case counter do
      0 ->
        final =
          decode.(result)
          |> Jason.encode!()

        send(from, {:done, final})
        {:noreply, {from, 0, result}}

      _ ->
        {:noreply, {from, counter, result, merge, decode}}
    end
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state) do
    {:noreply, state}
  end

  def handle_info({_ref, :ok}, state) do
    {:noreply, state}
  end
end

defmodule FileServer do
  use GenServer

  def start_link(path) do
    GenServer.start_link(__MODULE__, path, name: __MODULE__)
  end

  def init(file_path) do
    {:ok, file} = File.open(file_path, [:read, :raw, :read_ahead])
    {:ok, file}
  end

  def handle_call(:read, _from, file) do
    case :file.read_line(file) do
      {:ok, data} ->
        {:reply, data, file}

      :eof ->
        {:reply, :eof, file}
    end
  end

  def terminate(_reason, file) do
    File.close(file)
  end
end

defmodule StdinServer do
  use GenServer

  def start_link() do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_) do
    {:ok, {}}
  end

  def handle_call(:read, _from, _any) do
    case IO.read(:stdio, :line) do
      data when is_binary(data) ->
        {:reply, data, {}}

      _ ->
        {:reply, :eof, {}}
    end
  end
end
