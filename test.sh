#!/bin/bash

mode=$1

mix escript.build

if [ "$mode" == "snapshot" ]; then
	./js keys test/1/have | jq --sort-keys . > test/1/keys_want
	./js enums test/1/have | jq --sort-keys . > test/1/enums_want
	./js enum_stats test/1/have | jq --sort-keys . > test/1/enum_stats_want
else
	diff <(./js keys test/1/have | jq --sort-keys . ) test/1/keys_want
	diff <(./js enums test/1/have | jq --sort-keys . ) test/1/enums_want
	diff <(./js enum_stats test/1/have | jq --sort-keys . ) test/1/enum_stats_want
fi

