defmodule StreamInput do
  def from(gen_server_start) do
    {:ok, pid} = gen_server_start.()

    fn ->
      Stream.resource(
        fn -> {} end,
        fn _ ->
          case GenServer.call(pid, :read) do
            :eof -> {:halt, {}}
            data -> {[data], {}}
          end
        end,
        fn _ -> :ok end
      )
    end
  end
end
