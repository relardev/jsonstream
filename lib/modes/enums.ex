defmodule Enums do
  def process(stream_factory, report_progress, try_report_progress, report_error, opts) do
    result =
      stream_factory.()
      |> Stream.map(fn data ->
        Jason.decode(data)
      end)
      |> Stream.map(fn
        {:ok, data} ->
          {:ok, data}

        {:error, _} ->
          report_error.("could not decode json")
          {:error, 0}
      end)
      |> Stream.filter(fn
        {:ok, _} -> true
        _ -> false
      end)
      |> Stream.map(fn {:ok, data} -> data end)
      |> Enum.reduce({%{}, 0}, fn record, {result, counter} ->
        counter = try_report_progress.(counter)

        try do
          {merge(result, record, opts), counter + 1}
        rescue
          _ ->
            report_error.("could not merge")
            {result, counter}
        end
      end)

    elem(result, 1)
    |> report_progress.()

    elem(result, 0)
  end

  def merge(map1, map2, opts) when is_map(map1) and is_map(map2) do
    [map1, map2]
    |> Enum.map(&Map.keys/1)
    |> Enum.map(&MapSet.new/1)
    |> Enum.reduce(MapSet.new(), fn x, acc -> MapSet.union(x, acc) end)
    |> Enum.reduce(%{}, fn key, acc ->
      {v1, v2} = sort(Map.get(map1, key), Map.get(map2, key))

      Map.put(acc, key, merge(v1, v2, opts))
    end)
  end

  def merge([:too_many_records | tl], _b, _opts) do
    [:too_many_records | tl]
  end

  def merge(_a, [:too_many_records | tl], _opts) do
    [:too_many_records | tl]
  end

  def merge(a, a, _opts) do
    a
  end

  def merge(a, b, _opts)
      when (is_number(a) or is_binary(a) or is_boolean(a)) and
             (is_number(b) or is_binary(b) or is_boolean(b)) do
    [a, b]
  end

  def merge(a, b, [max_enums: limit] = opts) when is_list(a) and is_list(b) do
    case collapse(b, opts) do
      {:map, b} ->
        {:map, a} = collapse(a, opts)
        merge(a, b, opts)

      {:base, b} ->
        case length(b) + length(a) >= limit do
          true ->
            [:too_many_records] ++ Enum.take(a, 3)

          false ->
            Enum.uniq(a ++ b)
        end
    end
  end

  def merge(a, nil, _opts) do
    a
  end

  def merge(nil, b, _opts) do
    b
  end

  def merge(a, b, opts) when is_list(a) and (is_boolean(b) or is_number(b) or is_binary(b)) do
    merge(b, a, opts)
  end

  def merge(a, b, max_enums: limit)
      when (is_boolean(a) or is_number(a) or is_binary(a)) and is_list(b) do
    case length(b) >= limit do
      true ->
        [:too_many_records] ++ Enum.take(b, 3)

      false ->
        if Enum.member?(b, a) do
          b
        else
          [a | b]
        end
    end
  end

  def merge(a, b, opts) when is_map(a) and is_list(b) do
    {:map, b} = collapse(b, opts)
    merge(a, b, opts)
  end

  def collapse(a, opts) when is_list(a) do
    case a do
      [head | _tail] ->
        case is_map(head) do
          true ->
            {:map, Enum.reduce(a, %{}, fn x, acc -> merge(x, acc, opts) end)}

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
