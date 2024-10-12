defmodule FileServer do
  use GenServer

  def start_link(path) do
    GenServer.start_link(__MODULE__, path, name: __MODULE__)
  end

  def init(file_path) do
    {:ok, file} = File.open(file_path, [:read, :raw, :read_ahead])
    {:ok, file}
  end

  def handle_call(:read, _from, file) do
    line = produce_line(file)
    {:reply, line, file}
  end

  def terminate(_reason, file) do
    File.close(file)
  end

  def produce_line(file) do
    case :file.read_line(file) do
      {:ok, "//" <> _} ->
        produce_line(file)

      {:ok, data} ->
        data

      :eof ->
        :eof
    end
  end
end
