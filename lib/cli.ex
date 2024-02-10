defmodule CLI do
  def main(argv) do
    {mode, parallel, path, max_enums} = CliParser.parse(argv)

    factory =
      case path do
        "" ->
          fn -> StdinServer.start_link() end
          path

        path ->
          fn -> FileServer.start_link(path) end
      end
      |> StreamInput.from()

    opts = [max_enums: max_enums]

    {process, merge, decode} =
      case mode do
        :enum_stats ->
          {
            fn ->
              EnumStats.process(
                factory,
                &Progress.report_progress/1,
                &Progress.try_report_progress/1,
                &Progress.report_error/1,
                opts
              )
            end,
            fn a, b -> EnumStats.merge(a, b, opts) end,
            fn a -> DecodeEnumStats.decode(a) end
          }

        :enums ->
          {
            fn ->
              Enums.process(
                factory,
                &Progress.report_progress/1,
                &Progress.try_report_progress/1,
                &Progress.report_error/1,
                opts
              )
            end,
            fn a, b -> Enums.merge(a, b, opts) end,
            fn a -> a end
          }

        :keys ->
          {
            fn ->
              Keys.process(
                factory,
                &Progress.report_progress/1,
                &Progress.try_report_progress/1,
                &Progress.report_error/1
              )
            end,
            fn a, b -> Keys.merge(a, b) end,
            fn a -> a end
          }
      end

    case parallel do
      true ->
        {:ok, _} = WorkCollector.start_link({self(), process, merge, decode})

        receive do
          {:done, result} ->
            GenServer.call(Progress, :final_report)
            IO.puts(result)
        end

      false ->
        process.()
        |> decode.()
        |> Jason.encode!()
        |> IO.puts()
    end
  end
end

defmodule CliParser do
  def parse(argv) do
    {parsed, args, _invalid} =
      OptionParser.parse(argv, switches: [parallel: :boolean, max_enums: :integer])

    # IO.puts(:stderr, "argv: #{inspect(argv)}")
    # IO.puts(:stderr, "parsed: #{inspect(parsed)}, args: #{inspect(args)}")

    {mode, path} =
      case parse_args(args) do
        {:ok, x} ->
          x

        {:error, _} ->
          print_usage_and_exit()
      end

    mode =
      case mode do
        "enum_stats" ->
          :enum_stats

        "enums" ->
          :enums

        "keys" ->
          :keys

        _ ->
          print_usage_and_exit()
      end

    parallel =
      case parsed[:parallel] do
        nil -> true
        p -> p
      end

    max_enums =
      case parsed[:max_enums] do
        x when x < 0 -> print_usage_and_exit()
        nil -> 100
        n -> n
      end

    IO.puts(
      :stderr,
      "starting in #{inspect(mode)} mode, reading from: #{path_representation(path)}"
    )

    {mode, parallel, path, max_enums}
  end

  defp path_representation(""), do: "stdin"
  defp path_representation(path), do: path

  defp parse_args([]), do: {:error, :no_args}
  defp parse_args([mode]), do: {:ok, {mode, ""}}
  defp parse_args([mode, path]), do: {:ok, {mode, path}}
  defp parse_args(_a), do: {:error, :too_many_args}

  defp print_usage_and_exit() do
    message = """
     js - JSON stream analyser
     Usage:  
         js <mode> [file path]

     Modes:
       keys - find all keys in the JSON stream and count how many times each occurs
       enums - find all keys in the JSON stream unique values for each key
       enum_stats - find all keys in the JSON stream and calculate how many times each value occurs

     Options:
       --no-parallel - run in single process mode
       --max-enums <n> - maximum number of unique values to collect for each key (default: 100)
                         applies to both enums and enum_stats modes

     Example:
         $ cat records
         {"person":{"name":"John", "age": 23}}
         {"person":{"name":"Alice", "height": 162}}
         {"person":{"name":"Bob", "age": 23, "height": 180}}

         $ js keys records
         {
           "person": {
             "age": 2,
             "height": 2,
             "name": 3
           }
         }

         $ js enums records
         {
           "person": {
             "age": 23,
             "height": [162, 180],
             "name": ["Bob", "Alice", "John"]
           }
         }

         $ js enum_stats records
         {
           "person": {
             "age": {"23": 2},
             "height": [
               {"180": 1},
               {"162": 1}
             ],
             "name": [
               {"John": 1},
               {"Alice": 1},
               {"Bob": 1}
             ]
           }
         }
    """

    IO.puts(:stderr, message)
    System.halt(1)
  end
end
