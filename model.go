package simple_distributed_lock

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"time"

	"github.com/go-redis/redis/v7"
)

type Pool interface {
	Get(ctx context.Context) (Conn, error)
}

type Conn interface {
	Get(ctx context.Context, name string) (string, error)
	Set(ctx context.Context, name string, value string) (bool, error)
	SetNX(ctx context.Context, name string, value string, expiry time.Duration) (bool, error)
	Eval(ctx context.Context, script *Script, keysAndArgs ...interface{}) (interface{}, error)
	PTTL(ctx context.Context, name string) (time.Duration, error)
}

type Script struct {
	KeyCount int
	Src      string
	Hash     string
}

func NewScript(keyCount int, src string) *Script {
	h := sha1.New()
	_, _ = io.WriteString(h, src)
	return &Script{keyCount, src, hex.EncodeToString(h.Sum(nil))}
}

type pool struct {
	delegate *redis.Client
}

type conn struct {
	delegate *redis.Client
}

// A DelayFunc is used to decide the amount of time to wait between retries.
type DelayFunc func(tries int) time.Duration

// A Mutex is a distributed mutual exclusion lock.
type Mutex struct {
	name   string
	expiry time.Duration

	tries     int
	delayFunc DelayFunc

	factor float64

	quorum int

	genValueFunc func() (string, error)
	value        string
	until        time.Time

	Pools []Pool
}
