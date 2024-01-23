defmodule JkElixir do
  require Logger

  def main(stream) do
    stream
    |> Stream.map(&Jason.decode!/1)
    |> Enum.reduce({%{}, 0, System.monotonic_time()}, fn record, acc ->
      result = elem(acc, 0)
      counter = elem(acc, 1)
      timer = elem(acc, 2)

      timer =
        case rem(counter, 1000) do
          0 ->
            now = System.monotonic_time()
            delta = (now - timer) / 1_000_000.0
            formatted_delta = :io_lib.format("~.2f", [delta])

            IO.puts(:stderr, "At #{counter} records, delta=#{formatted_delta}ms")

            now

          _ ->
            timer
        end

      new = keys(record)
      {merge(result, new), counter + 1, timer}
    end)
    |> elem(0)
    |> Jason.encode!()
    |> IO.puts()
  end

  defp keys(record) do
    record
    |> Enum.reduce(%{}, fn {key, v}, acc ->
      Map.put(acc, key, val(v))
    end)
  end

  defp val(v) when is_map(v) do
    keys(v)
  end

  defp val(_v), do: 1

  defp merge(map1, map2) when is_map(map1) and is_map(map2) do
    [map1, map2]
    |> Enum.map(&Map.keys/1)
    |> Enum.map(&MapSet.new/1)
    |> Enum.reduce(MapSet.new(), fn x, acc -> MapSet.union(x, acc) end)
    |> Enum.reduce(%{}, fn key, acc ->
      v1 = Map.get(map1, key)
      v2 = Map.get(map2, key)
      Map.put(acc, key, merge(v1, v2))
    end)
  end

  defp merge(v1, v2) do
    case {to_base(v1), to_base(v2)} do
      {x, y} when is_number(x) and is_number(y) -> x + y
      {x, y} when is_number(x) -> y
      {x, y} when is_number(y) -> x
    end
  end

  defp to_base(v) when is_nil(v), do: 0
  defp to_base(v), do: v
end
