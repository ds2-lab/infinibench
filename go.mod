module github.com/wangaoone/redbench

go 1.15

replace github.com/mason-leap-lab/infinicache => github.com/wangaoone/LambdaObjectstore v1.2.0

require (
	github.com/ScottMansfield/nanolog v0.2.0
	github.com/aws/aws-sdk-go v1.31.0
	github.com/buraksezer/consistent v0.0.0-20191006190839-693edf70fd72
	github.com/cespare/xxhash v1.1.0
	github.com/dustin/go-humanize v1.0.0
	github.com/go-redis/redis v6.15.9+incompatible
	github.com/google/uuid v1.1.1
	github.com/mason-leap-lab/infinicache v1.0.1
	github.com/zhangjyr/hashmap v1.0.2
)
