.PHONY: bb examples
bb:
	find baseballdatabank-master -name \*.csv -exec echo "cat" {} "| cz" \; | sh > bb.zng
examples:
	@echo 'zq -f table "count()" bb.zng'
	@echo 'zq -f ndjson dimaggio bb.zng'
	@echo 'zq -f ndjson dimaggio bb.zng | jq'
	@echo 'zq -f table dimagvi01 bb.zng'
	@echo 'zq -f ndjson "dimagvi01 | r=first(.) by typeof(.)" bb.zng | jq'
	@echo 'zq -f table "dimagvi01 | first(.) by typeof(.) | count()" bb.zng'
	@echo 'zq -f table "by typeof(.) | count()" bb.zng'
	@echo 'zq -f table "by pb=typeof(PB),wp=typeof(WP),sb=typeof(SB)" bb.zng'
	@echo 'zq "* | ( filter PB=null | put PB=null ; filter PB != null ) | filter *" bb.zng > bb2.zng'
	@echo 'zq -f table "by pb=typeof(PB),wp=typeof(WP),sb=typeof(SB)" bb2.zng'
	@echo 'zq -f table "dimaggio | cut nameGiven,nameLast,weight,year" bb.zng'
	@echo 'zq -f ndjson "dimagvi01 | first(.) by typeof(.)" bb.zng | jq | grep dimagvi01'
	@echo 'zq -f ndjson "count() by playerID" bb.zng'
	@echo 'zq -f table "count() by playerID | count()" bb.zng'
