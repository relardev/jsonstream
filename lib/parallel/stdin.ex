defmodule StdinServer do
  use GenServer

  def start_link() do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_) do
    {:ok, {}}
  end

  def handle_call(:read, _from, _any) do
    case IO.read(:stdio, :line) do
      data when is_binary(data) ->
        {:reply, data, {}}

      _ ->
        {:reply, :eof, {}}
    end
  end
end
