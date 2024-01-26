defmodule CLI do
  def main(argv) do
    factory = stream_factory(argv)

    {:ok, _} = MainGenServer.start_link({self(), factory})

    receive do
      {:done, result} -> IO.puts(result)
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
end

defmodule MainGenServer do
  use GenServer

  def start_link(stream_factory) do
    GenServer.start_link(__MODULE__, stream_factory, name: __MODULE__)
  end

  def init({from, stream_factory}) do
    worker_count = System.schedulers_online() * 2

    for n <- 1..worker_count do
      IO.puts(:stderr, "Starting worker #{n}")

      Task.async(fn ->
        {:ok, result} = Keys.process(stream_factory)
        GenServer.call(__MODULE__, {:done, result})
      end)
    end

    {:ok, {from, worker_count, %{}}}
  end

  def handle_call({:done, result}, _from, {from, counter, previous}) do
    result = Keys.merge(previous, result)
    counter = counter - 1

    case counter do
      0 ->
        send(from, {:done, Jason.encode!(result)})
        {:reply, :ok, {from, 0, result}}

      _ ->
        {:reply, :ok, {from, counter, result}}
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
