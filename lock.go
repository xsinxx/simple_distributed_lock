package simple_distributed_lock

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"github.com/hashicorp/go-multierror"
	"sync"
	"time"
)

// Lock locks m. In case it returns an error on failure, you may retry to acquire the lock by calling this method again.
func (m *Mutex) Lock(ctx context.Context) error {
	value, err := m.genValueFunc()
	if err != nil {
		return err
	}

	for i := 0; i < m.tries; i++ {
		if i != 0 {
			time.Sleep(m.delayFunc(i))
		}

		start := time.Now()

		n, err := m.actOnPoolsAsync(func(pool Pool) (bool, error) {
			return m.acquire(ctx, pool, value)
		})
		if n == 0 && err != nil {
			return err
		}

		now := time.Now()
		until := now.Add(m.expiry - now.Sub(start) - time.Duration(int64(float64(m.expiry)*m.factor)))
		if n >= m.quorum && now.Before(until) {
			m.value = value
			m.until = until
			return nil
		}
		_, _ = m.actOnPoolsAsync(func(pool Pool) (bool, error) {
			return m.release(ctx, pool, value)
		})
	}

	return errors.New("retry fail")
}

// Unlock unlocks m and returns the status of unlock.
func (m *Mutex) Unlock(ctx context.Context) (bool, error) {
	n, err := m.actOnPoolsAsync(func(pool Pool) (bool, error) {
		return m.release(ctx, pool, m.value)
	})
	if n < m.quorum {
		return false, err
	}
	return true, nil
}

// Extend resets the mutex's expiry and returns the status of expiry extension.
func (m *Mutex) Extend(ctx context.Context) (bool, error) {
	n, err := m.actOnPoolsAsync(func(pool Pool) (bool, error) {
		return m.touch(ctx, pool, m.value, int(m.expiry/time.Millisecond))
	})
	if n < m.quorum {
		return false, err
	}
	return true, nil
}

func (m *Mutex) Valid(ctx context.Context) (bool, error) {
	n, err := m.actOnPoolsAsync(func(pool Pool) (bool, error) {
		return m.valid(ctx, pool)
	})
	return n >= m.quorum, err
}

func (m *Mutex) valid(ctx context.Context, pool Pool) (bool, error) {
	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err
	}
	reply, err := conn.Get(ctx, m.name)
	if err != nil {
		return false, err
	}
	return m.value == reply, nil
}

func genValue() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func (m *Mutex) acquire(ctx context.Context, pool Pool, value string) (bool, error) {
	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err
	}
	reply, err := conn.SetNX(ctx, m.name, value, m.expiry)
	if err != nil {
		return false, err
	}
	return reply, nil
}

var deleteScript = NewScript(1, `
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	else
		return 0
	end
`)

func (m *Mutex) release(ctx context.Context, pool Pool, value string) (bool, error) {
	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err
	}
	status, err := conn.Eval(ctx, deleteScript, m.name, value)
	if err != nil {
		return false, err
	}
	return status != 0, nil
}

var touchScript = NewScript(1, `
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("PEXPIRE", KEYS[1], ARGV[2])
	else
		return 0
	end
`)

func (m *Mutex) touch(ctx context.Context, pool Pool, value string, expiry int) (bool, error) {
	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err
	}
	status, err := conn.Eval(ctx, touchScript, m.name, value, expiry)
	if err != nil {
		return false, err
	}
	return status != "ERR", nil
}

func (m *Mutex) actOnPoolsAsync(actFn func(Pool) (bool, error)) (int, error) {
	type result struct {
		Status bool
		Err    error
	}
	var wg sync.WaitGroup
	ch := make(chan result)
	for _, pool := range m.Pools {
		go func(pool Pool) {
			wg.Add(1)
			defer wg.Done()
			r := result{}
			r.Status, r.Err = actFn(pool)
			ch <- r
		}(pool)
	}
	wg.Wait()
	close(ch)

	n := 0
	var err error
	for r := range ch {
		if r.Status {
			n++
		} else if r.Err != nil {
			err = multierror.Append(err, r.Err)
		}
	}
	return n, err
}
