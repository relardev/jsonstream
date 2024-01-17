defmodule JkElixirTest do
  use ExUnit.Case
  doctest JkElixir

  test "greets the world" do
    assert JkElixir.hello() == :world
  end
end
