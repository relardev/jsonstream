defmodule CLI do
  def main(argv) do
    stream(argv)
    |> JkElixir.main()
  end

  defp stream([]), do: IO.stream(:stdio, :line)
  defp stream(path), do: File.stream!(path)
end
