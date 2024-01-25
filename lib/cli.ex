defmodule CLI do
  def main(argv) do
    stream(argv)
    |> JkElixir.main()
  end

  defp stream([]) do
    {:ok, pid} = StdinServer.start_link()

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

  defp stream([path]) do
    {:ok, pid} = FileServer.start_link(path)

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
    {:ok, [""]}
  end

  def handle_call(:read, _from, cache) do
    case length(cache) do
      1 ->
        case IO.read(:stdio, 100_000) do
          :eof ->
            {:reply, :eof, []}

          data ->
            lines = String.split(hd(cache) <> data, :binary.compile_pattern(["\n"]))
            {:reply, hd(lines), tl(lines)}
        end

      _ ->
        {:reply, hd(cache), tl(cache)}
    end
  end
end
