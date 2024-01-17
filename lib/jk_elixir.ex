defmodule JkElixir do
  @moduledoc """
  Documentation for `JkElixir`.
  """

  defp val(acc, v) when is_map(v) do
    keys(acc, v)
  end

  defp val(_acc, _v), do: 1

  defp keys(_acc, record) do
    record
    |> Enum.reduce(%{}, fn {key, v}, acc ->
      Map.put(acc, key, val(acc, v))
    end)
  end

  defp to_base(v) when is_map(v), do: v
  defp to_base(v) when is_number(v), do: 1
  defp to_base(v) when is_list(v), do: 1
  defp to_base(v) when is_binary(v), do: 1
  defp to_base(v) when is_atom(v), do: 1
  defp to_base(v) when is_boolean(v), do: 1
  defp to_base(v) when is_nil(v), do: 1

  defp merge(map1, map2) when is_map(map1) and is_map(map2) do
    keys = Enum.concat(Map.keys(map1), Map.keys(map2))

    Enum.reduce(keys, %{}, fn key, acc ->
      v1 = Map.get(map1, key)
      v2 = Map.get(map2, key)
      Map.put(acc, key, merge(v1, v2))
    end)
  end

  defp merge(v1, v2) do
    b1 = to_base(v1)
    b2 = to_base(v2)

    case {b1, b2} do
      {1, 1} -> 1
      {1, v} -> v
      {v, 1} -> v
      {_, _} -> merge(v1, v2)
    end
  end

  def main(path) do
    File.stream!(path)
    |> Stream.map(&Jason.decode!/1)
    |> Enum.reduce(%{}, fn record, acc ->
      new = keys(%{}, record)
      merge(acc, new)
    end)
    |> Jason.encode!()
    |> IO.puts()
  end
end
