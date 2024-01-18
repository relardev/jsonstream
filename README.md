# Description

Count number of each key in list of jsons. Input file needs to have json in each line, eg:

```
{"a": 1234, "b": 5678, "c": 9012}
{"d": 3456 }
{"f": {"g": 7890} }
{"f": {"d": 7890} }
{"c": "1234"}
{"list":[1,2,3]}
{"list":{"ars":[1,2,555]}}
```

# Run With Docker

To analyse `file` do:

```
<file docker run -i relar/jsonstream
```
# From Source

1. install elixir
1. download dependencies - `mix deps.get`
1. build binary - `mix escrypt.build`
1. you have binary `jk_elixir` ready to go
1. pass stdin or filename: `jk_elixir records.json`
