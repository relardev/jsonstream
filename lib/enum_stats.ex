defmodule EnumStats do
  @limit 100
  def process(stream_factory) do
    result =
      stream_factory.()
      |> Stream.map(fn data ->
        Jason.decode!(data)
      end)
      |> Enum.reduce({%{}, 0}, fn record, {acc, counter} ->
        counter =
          case counter do
            1000 ->
              Progress.update(counter)
              0

            counter ->
              counter
          end

        acc =
          tuple(record)
          |> merge(acc)

        {acc, counter + 1}
      end)
      |> elem(0)

    result
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

  def merge(map1, map2) when is_map(map1) and is_map(map2) do
    [map1, map2]
    |> Enum.map(&Map.keys/1)
    |> Enum.map(&MapSet.new/1)
    |> Enum.reduce(MapSet.new(), fn x, acc -> MapSet.union(x, acc) end)
    |> Enum.reduce(%{}, fn key, acc ->
      v1 = Map.get(map1, key)
      v2 = Map.get(map2, key)

      merged = merge(v1, v2)
      # IO.puts("merged for #{key} from #{inspect(v1)} and #{inspect(v2)} to #{inspect(merged)}")
      Map.put(acc, key, merged)
    end)
  end

  def merge(_a, [:too_many_records]) do
    [:too_many_records]
  end

  def merge([:too_many_records], _b) do
    [:too_many_records]
  end

  def merge(a, nil) when is_tuple(a) or is_map(a) or is_list(a) do
    a
  end

  def merge(nil, b) when is_tuple(b) or is_map(b) or is_list(b) do
    b
  end

  def merge({k1, v1}, {k1, v2}) do
    {k1, v1 + v2}
  end

  def merge(a, b) when is_tuple(a) and is_tuple(b) do
    [a, b]
  end

  def merge(a, b) when is_tuple(a) and is_list(b) do
    add_tuple(a, b)
  end

  def merge([hd | _t] = a, b) when is_tuple(hd) and is_tuple(b) do
    add_tuple(b, a)
  end

  def merge([hd | _t] = a, b) when is_map(hd) and is_map(b) do
    merge(a, [b])
  end

  def merge([hd1 | _t1] = a, [hd2 | _t2] = b) when is_tuple(hd1) and is_tuple(hd2) do
    Enum.reduce(a, b, fn x, acc ->
      add_tuple(x, acc)
    end)
  end

  def merge([hd1 | _t1] = a, [hd2 | _t2] = b) when is_map(hd1) and is_map(hd2) do
    (a ++ b)
    |> Enum.reduce(%{}, fn map, acc ->
      merge(map, acc)
    end)
  end

  defp add_tuple(_t, [:too_many_records]), do: [:too_many_records]

  defp add_tuple(t, [hd | _tail] = to) when is_tuple(hd) and is_tuple(t) do
    case length(to) >= @limit do
      true ->
        [:too_many_records]

      false ->
        add_tuple(t, to, [])
    end
  end

  defp add_tuple({_k, _v} = t, [], to), do: [t | to]
  defp add_tuple({k1, v1}, [{k2, v2} | t], to) when k1 == k2, do: [{k1, v1 + v2} | t] ++ to
  defp add_tuple(kv, [hd | t], to), do: add_tuple(kv, t, [hd | to])
end

defmodule DecodeEnumStats do
  def decode(data) when is_map(data) do
    data
    |> Enum.reduce(%{}, fn {key, v}, acc ->
      Map.put(acc, key, decode(v))
    end)
  end

  def decode([hd | _t] = data) when is_tuple(hd) do
    Enum.reduce(data, %{}, fn {key, v}, acc ->
      Map.put(acc, key, v)
    end)
  end

  def decode([hd | _t]) when is_map(hd) do
    [decode(hd)]
  end

  def decode({key, count}) do
    %{key => count}
  end

  def decode([:too_many_records]), do: ["too_many_records"]
end
