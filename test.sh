#!/bin/bash

mix escript.build
diff <(./js keys test/1/have | jq . ) test/1/keys_want
diff <(./js enums test/1/have | jq . ) test/1/enums_want
diff <(./js enum_stats test/1/have | jq . ) test/1/enum_stats_want
