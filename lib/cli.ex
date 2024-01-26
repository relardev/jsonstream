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

defmodule Progress do
  use GenServer

  def start_link(report_every) do
    GenServer.start_link(__MODULE__, report_every, name: __MODULE__)
  end

  def init(report_every) do
    {:ok, {0, 0, report_every, System.monotonic_time()}}
  end

  def update(count) do
    GenServer.cast(__MODULE__, {:update, count})
  end

  def handle_cast({:update, count}, {current_count, since_last_report, report_every, start_time}) do
    new_count = count + current_count
    new_since_last_report = count + since_last_report

    case new_since_last_report >= report_every do
      true ->
        now = System.monotonic_time()
        rate = current_count / (now - start_time) * 1_000_000_000

        IO.puts(:stderr, "Processed #{new_count} keys at #{rate} keys/sec")

        {:noreply, {new_count, new_since_last_report - report_every, report_every, start_time}}

      false ->
        {:noreply, {new_count, new_since_last_report, report_every, start_time}}
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

    Progress.start_link(5000 * worker_count)

    for n <- 1..worker_count do
      IO.puts(:stderr, "Starting worker #{n}")

      Task.async(fn ->
        {:ok, result} = Keys.process(stream_factory)
        GenServer.cast(__MODULE__, {:done, result})
      end)
    end

    {:ok, {from, worker_count, %{}}}
  end

  def handle_cast({:done, result}, {from, counter, previous}) do
    result = Keys.merge(previous, result)
    counter = counter - 1

    case counter do
      0 ->
        send(from, {:done, Jason.encode!(result)})
        {:noreply, {from, 0, result}}

      _ ->
        {:noreply, {from, counter, result}}
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
