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

        formated_rate = :io_lib.format("~.2f", [rate])

        IO.puts(:stderr, "Processed #{new_count} records at #{formated_rate} records/sec")

        {:noreply, {new_count, new_since_last_report - report_every, report_every, start_time}}

      false ->
        {:noreply, {new_count, new_since_last_report, report_every, start_time}}
    end
  end
end
