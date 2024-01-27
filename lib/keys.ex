defmodule Keys do
  def process(stream_factory) do
    result =
      stream_factory.()
      |> Stream.map(fn data ->
        Jason.decode!(data)
      end)
      |> Enum.reduce({%{}, 0}, fn record, {result, counter} ->
        counter =
          case counter do
            5000 ->
              Progress.update(counter)
              0

            counter ->
              counter
          end

        new = keys(record)
        {merge(result, new), counter + 1}
      end)
      |> elem(0)

    result
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

  def merge(map1, map2) when is_map(map1) and is_map(map2) do
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

  def merge(v1, v2) do
    case {to_base(v1), to_base(v2)} do
      {x, y} when is_number(x) and is_number(y) -> x + y
      {x, y} when is_number(x) -> y
      {x, y} when is_number(y) -> x
    end
  end

  defp to_base(v) when is_nil(v), do: 0
  defp to_base(v), do: v
end
