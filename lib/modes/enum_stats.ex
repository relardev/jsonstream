defmodule EnumStats do
  def process(stream_factory, report_progress, try_report_progress, report_error, opts) do
    result =
      stream_factory.()
      |> Stream.map(fn data ->
        result = Jason.decode(data)

        {ok, _} = result

        if ok == :error do
          report_error.("could not decode json")
        end

        result
      end)
      |> Stream.filter(fn
        {:ok, _} -> true
        _ -> false
      end)
      |> Stream.map(fn {:ok, data} -> data end)
      |> Enum.reduce({%{}, 0}, fn record, {acc, counter} ->
        counter = try_report_progress.(counter)

        try do
          acc =
            tuple(record)
            |> merge(acc, opts)

          {acc, counter + 1}
        rescue
          _ ->
            report_error.("could not merge or tuple")
            {acc, counter}
        end
      end)

    elem(result, 1)
    |> report_progress.()

    elem(result, 0)
  end

  def tuple(record) when is_map(record) do
    record
    |> Enum.reduce(%{}, fn {key, v}, acc ->
      Map.put(acc, key, tuple(v))
    end)
  end

  def tuple(v) when is_number(v) or is_binary(v) or is_boolean(v) do
    {v, 1}
  end

  def tuple(v) when is_list(v) do
    Enum.map(v, &tuple/1)
  end

  def merge(map1, map2, opts) when is_map(map1) and is_map(map2) do
    [map1, map2]
    |> Enum.map(&Map.keys/1)
    |> Enum.map(&MapSet.new/1)
    |> Enum.reduce(MapSet.new(), fn x, acc -> MapSet.union(x, acc) end)
    |> Enum.reduce(%{}, fn key, acc ->
      v1 = Map.get(map1, key)
      v2 = Map.get(map2, key)

      merged = merge(v1, v2, opts)
      # IO.puts("merged for #{key} from #{inspect(v1)} and #{inspect(v2)} to #{inspect(merged)}")
      Map.put(acc, key, merged)
    end)
  end

  def merge(_a, [:too_many_records], _opts) do
    [:too_many_records]
  end

  def merge([:too_many_records], _b, _opts) do
    [:too_many_records]
  end

  def merge(a, nil, _opts) when is_tuple(a) or is_map(a) or is_list(a) do
    a
  end

  def merge(nil, b, _opts) when is_tuple(b) or is_map(b) or is_list(b) do
    b
  end

  def merge({k1, v1}, {k1, v2}, _opts) do
    {k1, v1 + v2}
  end

  def merge(a, b, _opts) when is_tuple(a) and is_tuple(b) do
    [a, b]
  end

  def merge(a, b, opts) when is_tuple(a) and is_list(b) do
    add_tuple(a, b, opts)
  end

  def merge([hd | _t] = a, b, opts) when is_tuple(hd) and is_tuple(b) do
    add_tuple(b, a, opts)
  end

  def merge([hd | _t] = a, b, opts) when is_map(hd) and is_map(b) do
    merge(a, [b], opts)
  end

  def merge([hd1 | _t1] = a, [hd2 | _t2] = b, opts) when is_tuple(hd1) and is_tuple(hd2) do
    Enum.reduce(a, b, fn x, acc ->
      add_tuple(x, acc, opts)
    end)
  end

  def merge([hd1 | _t1] = a, [hd2 | _t2] = b, opts) when is_map(hd1) and is_map(hd2) do
    (a ++ b)
    |> Enum.reduce(%{}, fn map, acc ->
      merge(map, acc, opts)
    end)
  end

  defp add_tuple(_t, [:too_many_records], _opts), do: [:too_many_records]

  defp add_tuple(t, [hd | _tail] = to, max_enums: limit) when is_tuple(hd) and is_tuple(t) do
    case length(to) >= limit do
      true ->
        [:too_many_records]

      false ->
        add_tuple_r(t, to, [])
    end
  end

  defp add_tuple_r({_k, _v} = t, [], to), do: [t | to]
  defp add_tuple_r({k1, v1}, [{k2, v2} | t], to) when k1 == k2, do: [{k1, v1 + v2} | t] ++ to
  defp add_tuple_r(kv, [hd | t], to), do: add_tuple_r(kv, t, [hd | to])
end

defmodule DecodeEnumStats do
  def decode(data) when is_map(data) do
    data
    |> Enum.reduce(%{}, fn {key, v}, acc ->
      Map.put(acc, key, decode(v))
    end)
  end

  def decode([hd | _t] = data) when is_tuple(hd) do
    Enum.reduce(data, [], fn {key, v}, acc ->
      [{key, v} | acc]
    end)
    |> Enum.sort_by(fn {_, v} -> v end, &>=/2)
    |> Enum.reduce([], fn {key, v}, acc ->
      [%{key => v} | acc]
    end)
    |> Enum.reverse()
  end

  def decode([hd | _t]) when is_map(hd) do
    [decode(hd)]
  end

  def decode({key, count}) do
    %{key => count}
  end

  def decode([:too_many_records]), do: ["too_many_records"]
end
