defmodule Enums do
  @limit 100
  def process(stream_factory) do
    result =
      stream_factory.()
      |> Stream.map(fn data ->
        Jason.decode!(data)
      end)
      |> Enum.reduce({%{}, 0}, fn record, {result, counter} ->
        counter =
          case counter do
            1000 ->
              Progress.update(counter)
              0

            counter ->
              counter
          end

        {merge(result, record), counter + 1}
      end)
      |> elem(0)

    result
  end

  def merge(map1, map2) when is_map(map1) and is_map(map2) do
    [map1, map2]
    |> Enum.map(&Map.keys/1)
    |> Enum.map(&MapSet.new/1)
    |> Enum.reduce(MapSet.new(), fn x, acc -> MapSet.union(x, acc) end)
    |> Enum.reduce(%{}, fn key, acc ->
      {v1, v2} = sort(Map.get(map1, key), Map.get(map2, key))

      Map.put(acc, key, merge(v1, v2))
    end)
  end

  def merge([:too_many_records], _b) do
    [:too_many_records]
  end

  def merge(_a, [:too_many_records]) do
    [:too_many_records]
  end

  def merge(a, a) do
    a
  end

  def merge(a, b)
      when (is_number(a) or is_binary(a) or is_boolean(a)) and
             (is_number(b) or is_binary(b) or is_boolean(b)) do
    [a, b]
  end

  def merge(a, b) when is_list(a) and is_list(b) do
    case collapse(b) do
      {:map, b} ->
        {:map, a} = collapse(a)
        merge(a, b)

      {:base, b} ->
        case length(b) + length(a) >= @limit do
          true ->
            [:too_many_records]

          false ->
            Enum.uniq(a ++ b)
        end
    end
  end

  def merge(a, nil) do
    a
  end

  def merge(nil, b) do
    b
  end

  def merge(a, b) when is_list(a) and (is_boolean(b) or is_number(b) or is_binary(b)) do
    merge(b, a)
  end

  def merge(a, b) when (is_boolean(a) or is_number(a) or is_binary(a)) and is_list(b) do
    case length(b) >= @limit do
      true ->
        [:too_many_records]

      false ->
        if Enum.member?(b, a) do
          b
        else
          [a | b]
        end
    end
  end

  def merge(a, b) when is_map(a) and is_list(b) do
    {:map, b} = collapse(b)
    merge(a, b)
  end

  def collapse(a) when is_list(a) do
    case a do
      [head | _tail] ->
        case is_map(head) do
          true ->
            {:map, Enum.reduce(a, %{}, fn x, acc -> merge(x, acc) end)}

          false ->
            {:base, a}
        end

      [] ->
        {:base, nil}
    end
  end

  def sort(a, b) do
    case a < b do
      true -> {a, b}
      false -> {b, a}
    end
  end
end
