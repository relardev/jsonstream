defmodule Keys do
  def process(stream_factory, report_progress, try_report_progress, report_failure) do
    result =
      stream_factory.()
      |> Stream.map(fn data ->
        Jason.decode(data)
      end)
      |> Stream.map(fn
        {:ok, data} ->
          {:ok, data}

        {:error, _} ->
          report_failure.("could not decode json")
          {:error, 0}
      end)
      |> Stream.filter(fn
        {:ok, _} -> true
        _ -> false
      end)
      |> Stream.map(fn {:ok, data} -> data end)
      |> Enum.reduce({%{}, 0}, fn record, {acc, counter} ->
        counter = try_report_progress.(counter)

        try do
          new = keys(record)
          {merge(acc, new), counter + 1}
        rescue
          _ ->
            report_failure.("could not merge")
            {acc, counter}
        end
      end)

    elem(result, 1)
    |> report_progress.()

    elem(result, 0)
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

  defp val([hd | _t] = v) when is_map(hd) do
    reduced =
      v
      |> Enum.map(&keys/1)
      |> Enum.reduce(%{}, fn x, acc ->
        Map.merge(x, acc, fn _, v1, v2 -> merge(v1, v2) end)
      end)

    averaged =
      Enum.reduce(reduced, %{}, fn {k, val}, acc ->
        Map.put(acc, k, avg(val, length(v)))
      end)

    [averaged]
  end

  defp val(_v), do: 1

  defp avg(v, len) when is_map(v) do
    Enum.reduce(v, %{}, fn {k, val}, acc ->
      Map.put(acc, k, avg(val, len))
    end)
  end

  defp avg([v], len) when is_map(v) do
    [avg(v, len)]
  end

  defp avg(v, len) when is_number(v) do
    v / len
  end

  def merge([v1], [v2]) when is_map(v1) and is_map(v2) do
    [merge(v1, v2)]
  end

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

defmodule DecodeKeys do
  def decode_outer(a) do
    decode(a, Progress.count())
  end

  defp decode(data, count) when is_map(data) do
    data
    |> Enum.reduce(%{}, fn {k, v}, acc ->
      Map.put(acc, k, decode(v, count))
    end)
  end

  defp decode([data], count) when is_map(data) do
    [decode(data, count)]
  end

  defp decode(value, count) when is_number(value) do
    percent(value / count)
  end

  defp percent(n) do
    formatted = :io_lib.format("~.1f", [n * 100])
    "#{formatted}%"
  end
end
