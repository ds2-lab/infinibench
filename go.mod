module github.com/wangaoone/redbench

go 1.15

replace github.com/mason-leap-lab/infinicache => ../../mason-leap-lab/infinicache

// replace github.com/mason-leap-lab/infinicache => github.com/wangaoone/LambdaObjectstore v1.2.0

require (
	github.com/ScottMansfield/nanolog v0.2.0
	github.com/aws/aws-sdk-go v1.38.38
	github.com/buraksezer/consistent v0.9.0
	github.com/cespare/xxhash v1.1.0
	github.com/dustin/go-humanize v1.0.0
	github.com/go-redis/redis/v8 v8.11.4
	github.com/google/uuid v1.2.0
	github.com/hidez8891/shm v0.0.0-20200313135933-0ec4df5f28c7 // indirect
	github.com/mason-leap-lab/go-utils v1.2.6
	github.com/mason-leap-lab/infinicache v1.0.1
	github.com/zhangjyr/hashmap v1.0.2
)
