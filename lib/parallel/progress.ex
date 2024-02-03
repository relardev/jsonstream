defmodule Progress do
  use GenServer

  def start_link(report_every) do
    GenServer.start_link(__MODULE__, report_every, name: __MODULE__)
  end

  def init(report_every) do
    Process.send_after(self(), {:report_progress, report_every}, report_every)
    {:ok, {0, System.monotonic_time()}}
  end

  def update(count) do
    GenServer.cast(__MODULE__, {:update, count})
  end

  def handle_cast({:update, new}, {count, start_time}) do
    {:noreply, {count + new, start_time}}
  end

  def handle_info({:report_progress, report_every}, {count, start_time} = state) do
    rate = count / (System.monotonic_time() - start_time) * 1_000_000_000

    formated_rate = :io_lib.format("~.2f", [rate])

    IO.puts(:stderr, "Processed #{count} records at #{formated_rate} records/sec")

    Process.send_after(self(), {:report_progress, report_every}, report_every)

    {:noreply, state}
  end
end
