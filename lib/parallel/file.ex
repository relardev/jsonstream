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
    case :file.read_line(file) do
      {:ok, data} ->
        {:reply, data, file}

      :eof ->
        {:reply, :eof, file}
    end
  end

  def terminate(_reason, file) do
    File.close(file)
  end
end
