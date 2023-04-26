package simple_distributed_lock

import (
	"context"
	"github.com/go-redis/redis/v7"
	c "github.com/smartystreets/goconvey/convey" // 别名导入
	"github.com/stvp/tempredis"
	"testing"
	"time"
)

var servers []*tempredis.Server // Temporary redis-server processes for golang testing

const poolSize = 10

func newPools() []Pool {
	pools := make([]Pool, poolSize)

	for i := 0; i < poolSize; i++ {
		client := redis.NewClient(&redis.Options{
			Network: "unix",
			Addr:    servers[i].Socket(),
		})
		pools[i] = NewPool(client)
	}

	return pools
}

func newTestMutexes(pools []Pool, name string, n int) []*Mutex {
	mutexes := make([]*Mutex, n)
	for i := 0; i < n; i++ {
		mutexes[i] = &Mutex{
			name:         name,
			expiry:       8 * time.Second,
			tries:        32,
			delayFunc:    func(tries int) time.Duration { return 500 * time.Millisecond },
			genValueFunc: genValue,
			factor:       0.01,
			quorum:       len(pools)/2 + 1,
			Pools:        pools,
		}
	}
	return mutexes
}

func TestStandAloneAllow(t *testing.T) {
	c.Convey("standalone_token_bucket", t, func() {
		var (
			limiter = NewStandAloneTokenBucket(1, 10)
		)
		actual := limiter.AllowN(context.Background(), time.Now(), 19)
		c.So(!actual, c.ShouldBeTrue)
	})

}
