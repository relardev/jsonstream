defmodule CLI do
  def main(argv) do
    {mode, parallel, path, max_enums} = CliParser.parse(argv)

    factory = stream_factory(path)
    opts = [max_enums: max_enums]

    {process, merge, decode} =
      case mode do
        :enum_stats ->
          {
            fn -> EnumStats.process(factory, opts) end,
            fn a, b -> EnumStats.merge(a, b, opts) end,
            fn a -> DecodeEnumStats.decode(a) end
          }

        :enums ->
          {
            fn -> Enums.process(factory, opts) end,
            fn a, b -> Enums.merge(a, b, opts) end,
            fn a -> a end
          }

        :keys ->
          {
            fn -> Keys.process(factory) end,
            fn a, b -> Keys.merge(a, b) end,
            fn a -> a end
          }
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

  defp stream_factory("") do
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

  defp stream_factory(path) do
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
end

defmodule CliParser do
  def parse(argv) do
    {parsed, args, _invalid} =
      OptionParser.parse(argv, switches: [parallel: :boolean, max_enums: :integer])

    # IO.puts(:stderr, "argv: #{inspect(argv)}")
    # IO.puts(:stderr, "parsed: #{inspect(parsed)}, args: #{inspect(args)}")

    {mode, path} =
      case parse_args(args) do
        {:ok, x} ->
          x

        {:error, _} ->
          print_usage_and_exit()
      end

    mode =
      case mode do
        "enum_stats" ->
          :enum_stats

        "enums" ->
          :enums

        "keys" ->
          :keys

        _ ->
          print_usage_and_exit()
      end

    parallel =
      case parsed[:parallel] do
        nil -> true
        p -> p
      end

    max_enums =
      case parsed[:max_enums] do
        x when x < 0 -> print_usage_and_exit()
        nil -> 100
        n -> n
      end

    IO.puts(
      :stderr,
      "starting in #{inspect(mode)} mode, reading from: #{path_representation(path)}"
    )

    {mode, parallel, path, max_enums}
  end

  defp path_representation(""), do: "stdin"
  defp path_representation(path), do: path

  defp parse_args([]), do: {:error, :no_args}
  defp parse_args([mode]), do: {:ok, {mode, ""}}
  defp parse_args([mode, path]), do: {:ok, {mode, path}}
  defp parse_args(_a), do: {:error, :too_many_args}

  defp print_usage_and_exit() do
    message = """
    js - JSON stream analyser
    Usage:  
        js <mode> [file path]

    Modes:
      keys - find all keys in the JSON stream and count how many times each occurs
      enums - find all keys in the JSON stream unique values for each key
      enum_stats - find all keys in the JSON stream and calculate how many times each value occurs

    Options:
      --no-parallel - run in single process mode
      --max-enums <n> - maximum number of unique values to collect for each key (default: 100)
                        applies to both enums and enum_stats modes

    Example:

        $ cat records
        {"person":{"name":"John", "age": 23}}
        {"person":{"name":"Alice", "height": 162}}
        {"person":{"name":"Bob", "age": 23, "height": 180}}

        $ js keys records
        {
          "person": {
            "age": 2,
            "height": 2,
            "name": 3
          }
        }

        $ js enums records
        {
          "person": {
            "age": 23,
            "height": [162, 180],
            "name": ["Bob", "Alice", "John"]
          }
        }

        $ js enum_stats records
        {
          "person": {
            "age": {"23": 2},
            "height": {162": 1, "180": 1},
            "name": {"Alice": 1, "Bob": 1, "John": 1}
          }
        }
    """

    IO.puts(:stderr, message)
    System.halt(1)
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
