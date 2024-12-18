defmodule JsonSchema do
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
        # IO.puts("incomming: #{inspect(result)}")

        s = schema(record)

        # IO.puts("Schema: #{inspect(s)}")

        res = merge(result, s, opts)

        # IO.puts("Result: #{inspect(res)}")
        {res, counter + 1}
      end)

    elem(result, 1)
    |> report_progress.()

    elem(result, 0)
  end

  defp schema(record) when is_map(record) do
    properties =
      record
      |> Enum.reduce(%{}, fn {k, v}, acc ->
        Map.put(acc, k, schema(v))
      end)

    %{
      type: "object",
      properties: properties
    }
  end

  defp schema(record)
       when is_number(record) or is_boolean(record) or is_binary(record) or is_nil(record) do
    %{type: repr(record)}
  end

  defp schema(record) when is_list(record) do
    items =
      record
      |> Enum.map(&schema/1)
      |> Enum.reduce(%{}, fn %{type: t} = s, acc ->
        case t do
          "object" -> merge(acc, s, %{})
          "array" -> merge(acc, s, %{})
          _ -> Map.put(acc, :type, t)
        end
      end)

    %{type: "array", items: items}
  end

  defp repr(v) when is_number(v) do
    case v do
      _ when v == trunc(v) -> "integer"
      _ -> "number"
    end
  end

  defp repr(v) when is_boolean(v) do
    "boolean"
  end

  defp repr(v) when is_binary(v) do
    "string"
  end

  defp repr(v) when is_nil(v) do
    "null"
  end

  def merge(a, a, _opts) do
    a
  end

  def merge(a, nil, _opts) do
    a
  end

  def merge(nil, b, _opts) do
    b
  end

  def merge(a, b, _opts) when a == %{} and is_map(b) do
    b
  end

  def merge(%{properties: :too_many_properties}, _, _opts) do
    %{type: "object", properties: :too_many_properties}
  end

  def merge(_, %{properties: :too_many_properties}, _opts) do
    %{type: "object", properties: :too_many_properties}
  end

  def merge(%{properties: prop_a}, %{properties: prop_b}, opts) do
    prop =
      [prop_a, prop_b]
      |> Enum.map(&Map.keys/1)
      |> Enum.map(&MapSet.new/1)
      |> Enum.reduce(MapSet.new(), fn x, acc -> MapSet.union(x, acc) end)
      |> Enum.reduce(%{}, fn key, acc ->
        v1 = Map.get(prop_a, key)
        v2 = Map.get(prop_b, key)
        Map.put(acc, key, merge(v1, v2, opts))
      end)

    prop =
      if map_size(prop) > opts[:max_properties] do
        :too_many_properties
      else
        prop
      end

    %{type: "object", properties: prop}
  end

  def merge(%{type: "array", items: a}, %{type: "array", items: b}, opts) do
    %{type: "array", items: merge(a, b, opts)}
  end

  def merge(%{type: "integer"}, %{type: "number"}, _opts) do
    %{type: "number"}
  end

  def merge(%{type: "number"}, %{type: "integer"}, _opts) do
    %{type: "number"}
  end

  # def merge(%{oneOf: a}, %{oneOf: b}, _opts) do
  #   %{oneOf: merge(a, b, %{})}
  # end

  def merge(%{oneOf: a}, %{type: _} = b, _opts) do
    res =
      case Enum.find(a, fn x -> x == b end) do
        nil -> [b | a]
        _ -> a
      end

    %{oneOf: res}
  end

  def merge(%{type: _} = a, %{oneOf: b}, _opts) do
    res =
      case Enum.find(b, fn x -> x == a end) do
        nil -> [a | b]
        _ -> b
      end

    %{oneOf: res}
  end

  def merge(%{oneOf: a}, %{oneOf: b}, _opts) do
    res =
      a
      |> Enum.reduce(b, fn x, acc ->
        case Enum.find(acc, fn y -> y == x end) do
          nil -> [x | acc]
          _ -> acc
        end
      end)

    %{oneOf: res}
  end

  # TODO its missing number integer and bool array mergeing
  def merge(%{type: "string"} = a, %{type: "array", items: %{type: "string"}} = b, _opts) do
    %{oneOf: [a, b]}
  end

  def merge(%{type: "array", items: %{type: "string"}} = a, %{type: "string"} = b, _opts) do
    %{oneOf: [a, b]}
  end

  def merge(a, %{type: "null"}, _opts) do
    a
  end

  def merge(%{type: "null"}, b, _opts) do
    b
  end

  def merge(a, b, _opts) when b == %{} do
    a
  end

  def merge(a, b, _opts) when a == %{} do
    b
  end

  def decode(%{type: "object", properties: :too_many_properties}) do
    %{type: "object", additionalProperties: true}
  end

  def decode(%{type: "object", properties: prop}) do
    prop =
      Enum.reduce(prop, %{}, fn {k, v}, acc ->
        Map.put(acc, k, decode(v))
      end)

    %{type: "object", properties: prop}
  end

  def decode(%{type: "string"} = a), do: a
  def decode(%{type: "integer"} = a), do: a
  def decode(%{type: "number"} = a), do: a
  def decode(%{type: "null"} = a), do: a
  def decode(%{type: "boolean"} = a), do: a
end
