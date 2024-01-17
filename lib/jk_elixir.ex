defmodule JkElixir do
  @moduledoc """
  Documentation for `JkElixir`.
  """

  defp val(v) when is_map(v) do
    keys(v)
  end

  defp val(_v), do: 1

  defp keys(record) do
    record
    |> Enum.reduce(%{}, fn {key, v}, acc ->
      Map.put(acc, key, val(v))
    end)
  end

  defp to_base(v) when is_nil(v), do: 0
  defp to_base(v), do: v

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

  def main(path) do
    File.stream!(path)
    |> Stream.map(&Jason.decode!/1)
    |> Enum.reduce(%{}, fn record, acc ->
      new = keys(record)
      merge(acc, new)
    end)
    |> Jason.encode!()
    |> IO.puts()
  end
end
