# Description

JSON Stream is a tool for analysing streams of jsons. Input file needs to have json in each line, eg:

```
$ cat records
{"person":{"name":"John", "age": 23}}
{"person":{"name":"Alice", "height": 162}}
{"person":{"name":"Bob", "age": 23, "height": 180}}
```

# Usage

`js <mode> [file path]`

Modes:

1. keys - find all keys in and count how many times each occurs
1. enums - find unique values for each key
1. enum_stats - calculate how many times each value for a given key occurs


## Examples

### Keys

```
$ js keys records
{
  "person": {
    "age": 2,
    "height": 2,
    "name": 3
  }
}
```

### Enums
```
$ js enums records
{
  "person": {
    "age": 23,
    "height": [162, 180],
    "name": ["Bob", "Alice", "John"]
  }
}
```

### Enum Stats

```
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
```

# Run With Docker

To analyse `file` using mode `keys` do:

```
<file docker run -i relar/jsonstream js keys
```

but reading from stdin is slower than reading from file, for bigger jobs mount volume and use reading from file

# From Source

1. install elixir
1. download dependencies - `mix deps.get`
1. build binary - `mix escrypt.build`
1. you have binary `js` ready to go
1. pass stdin or filename: `js keys records.json`
