defmodule WorkCollector do
  use GenServer
  require Progress

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init({from, process, merge, decode}) do
    worker_count = System.schedulers_online() * 2

    Progress.start_link(2000)

    for n <- 1..worker_count do
      IO.puts(:stderr, "Starting worker #{n}")

      Task.async(fn ->
        GenServer.cast(__MODULE__, {:done, process.()})
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
