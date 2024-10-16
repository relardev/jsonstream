defmodule Progress do
  use GenServer

  def try_report_progress(x) when is_number(x) do
    case x do
      1000 ->
        GenServer.cast(__MODULE__, {:add, x})
        0

      x ->
        x
    end
  end

  def report_progress(x) when is_number(x) do
    GenServer.cast(__MODULE__, {:add, x})
  end

  def count() do
    GenServer.call(__MODULE__, :count)
  end

  def report_error(message) do
    GenServer.cast(__MODULE__, {:err, message})
  end

  def final_report do
    GenServer.call(__MODULE__, :final_report)
  end

  # Gen server callbacks

  def start_link(report_every) do
    GenServer.start_link(__MODULE__, report_every, name: __MODULE__)
  end

  def init(report_every) do
    Process.send_after(self(), {:report_progress, report_every}, report_every)
    {:ok, {0, %{}, System.monotonic_time()}}
  end

  def handle_cast({:add, new}, {count, errors, start_time}) do
    {:noreply, {count + new, errors, start_time}}
  end

  def handle_cast({:err, message}, {count, errors, start_time}) do
    {:noreply, {count, Map.update(errors, message, 1, &(&1 + 1)), start_time}}
  end

  def handle_call(:count, _from, {count, _, _} = state) do
    {:reply, count, state}
  end

  def handle_call(:final_report, _from, {count, errors, start_time}) do
    rate = count / (System.monotonic_time() - start_time) * 1_000_000_000

    formated_rate = :io_lib.format("~.2f", [rate])

    total = count + map_size(errors)

    errors =
      case map_size(errors) do
        0 ->
          ""

        _ ->
          errors_repr =
            errors
            |> Enum.map(fn {k, v} -> "#{k}: #{v}" end)
            |> Enum.join("\n  ")

          "\nErrors:\n  " <> errors_repr
      end

    IO.puts(
      :stderr,
      "Records processed successfuly #{count}/#{total} records at #{formated_rate} records/sec#{errors}"
    )

    {:reply, :ok, {count, errors, start_time}}
  end

  def handle_info({:report_progress, report_every}, {count, errors, start_time} = state) do
    rate = count / (System.monotonic_time() - start_time) * 1_000_000_000

    formated_rate = :io_lib.format("~.2f", [rate])

    errors_count = Enum.reduce(errors, 0, fn {_, v}, acc -> acc + v end)

    IO.puts(
      :stderr,
      "Processed #{count} records at #{formated_rate} records/sec, errors: #{errors_count}"
    )

    Process.send_after(self(), {:report_progress, report_every}, report_every)

    {:noreply, state}
  end
end
