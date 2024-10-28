#!/bin/bash

snapshot() {
	echo "Snapshot $1"
	./js keys test/$1/have --no-parallel | jq --sort-keys . > test/$1/keys_want
	./js enums test/$1/have --no-parallel | jq --sort-keys . > test/$1/enums_want
	./js enum_stats test/$1/have --no-parallel | jq --sort-keys . > test/$1/enum_stats_want
}

run_test() {
	diff <(./js keys test/$1/have --no-parallel | jq --sort-keys . ) test/$1/keys_want
	if [ $? -ne 0 ]; then
		echo "test $1 faild on: keys"
		exit 1
	fi

	diff <(./js enums test/$1/have --no-parallel | jq --sort-keys . ) test/$1/enums_want
	if [ $? -ne 0 ]; then
		echo "test $1 faild on: enums"
		exit 1
	fi

	diff <(./js enum_stats test/$1/have --no-parallel | jq --sort-keys . ) test/$1/enum_stats_want
	if [ $? -ne 0 ]; then
		echo "test $1 faild on: enum_stats"
		exit 1
	fi
}

mode=$1

mix escript.build

if [ "$mode" == "snapshot" ]; then
	if [ -z "$2" ]; then
		echo "Usage: $0 snapshot <test>"
		exit 1
	fi
	snapshot $2
else
	run_test 1
	run_test 2
	run_test 3
fi
