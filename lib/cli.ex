defmodule CLI do
  def main(argv) do
    stream(argv)
    |> JkElixir.main()
  end

  defp stream([]), do: File.stream!("/dev/stdin")
  defp stream(path), do: File.stream!(path)
end
