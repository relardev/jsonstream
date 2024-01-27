defmodule Count do
  def process(factory) do
    result =
      factory.()
      |> Stream.map(fn _ -> 1 end)
      |> Enum.sum()

    {:ok, result}
  end

  def merge(a, b) when is_number(a) do
    a + b
  end

  def merge(a, b) when is_map(a) do
    b
  end
end
