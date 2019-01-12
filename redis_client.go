package storage

import (
	"reflect"
	"sync/atomic"
	"time"

	log "github.com/golang/glog"
	"github.com/go-redis/redis"
)

// 定义一个redis的最小接口，
// 方便定义mockredis进行单元测试,
// 方便实现shard版的RedisClient等
type RedisClient interface {
	Get(key string) ([]byte, error)
	Set(key string, value interface{}, expiration time.Duration) error
	MGet(keys ...string) ([]interface{}, error)
	MSet(expiration time.Duration, pairs ...interface{}) error
	Expire(key string, expiration time.Duration) (bool, error)
	Del(keys ...string) (int64, error)
	Incr(key string, step int64) (int64, error)
	Decr(key string, step int64) (int64, error)
	ZrangeByScore(key string, max, min string, count int) ([]string, error)
	ZrevRangeByScore(key string, max, min string, count int) ([]string, error)
	ZAdd(key string, score float64, value interface{}) error
	ZRem(key string, value interface{}) error
	ZCount(key, max, min string) (int, error)
	ZAddM(key string, members ...redis.Z) error
	Ping() error
}

type redisClient struct {
	client *redis.Client
}

func NewRedisClient(client *redis.Client) (r RedisClient) {
	return redisClient{client}
}

func newRedisClient(client *redis.Client, options *redis.Options) (r RedisClient) {
	if client == nil {
		panic("redisclient *redis.Client can NOT be nil")
	}

	r = NewRedisClient(redis.NewClient(options))
	return
}

func (r redisClient) Ping() error {
	return r.client.Ping().Err()
}

func (r redisClient) Get(key string) ([]byte, error) {
	if atomic.LoadInt32(&logFlag) != 0 {
		startTime := time.Now()
		defer func() {
			log.Infof("raw_client_get %s use %d microsecond", key, time.Now().Sub(startTime)/time.Microsecond)

		}()
	}
	return r.client.Get(key).Bytes()
}

func (r redisClient) Set(key string, value interface{}, expiration time.Duration) error {
	if atomic.LoadInt32(&logFlag) != 0 {
		startTime := time.Now()
		defer func() {
			log.Infof("raw_client_set %s use %d microsecond", key, time.Now().Sub(startTime)/time.Microsecond)

		}()
	}
	return r.client.Set(key, value, expiration).Err()
}

func (r redisClient) MGet(keys ...string) ([]interface{}, error) {
	if atomic.LoadInt32(&logFlag) != 0 {
		startTime := time.Now()
		defer func() {
			log.Infof("raw_client_mget %d use %d microsecond", len(keys), time.Now().Sub(startTime)/time.Microsecond)
		}()
	}

	value, err := r.client.MGet(keys...).Result()
	return value, err
}

func (r redisClient) MSet(expiration time.Duration, pairs ...interface{}) error {
	if atomic.LoadInt32(&logFlag) != 0 {
		startTime := time.Now()
		defer func() {
			log.Infof("raw_client_mset %d use %d microsecond", len(pairs)/2, time.Now().Sub(startTime)/time.Microsecond)
		}()
	}
	err := r.client.MSet(pairs...).Err()
	if err != nil {
		return err
	}
	if expiration > 0 {
		for i := 0; i < len(pairs); i = i + 2 {
			switch pairs[i].(type) {
			case []byte:
				r.client.Expire(string(pairs[i].([]byte)), expiration)
			case BytesValue:
				r.client.Expire(string(pairs[i].(BytesValue)), expiration)
			default:
				log.Error("raw_client_mset expire unsupport keytype ", reflect.TypeOf(pairs[i]))
			}
		}
	}

	return err
}

func (r redisClient) Expire(key string, expiration time.Duration) (bool, error) {
	return r.client.Expire(key, expiration).Result()
}

func (r redisClient) Del(keys ...string) (int64, error) {
	if atomic.LoadInt32(&logFlag) != 0 {
		startTime := time.Now()
		defer func() {
			log.Infof("raw_client_del %d use %d microsecond", len(keys), time.Now().Sub(startTime)/time.Microsecond)
		}()
	}
	return r.client.Del(keys...).Result()
}

func (r redisClient) Incr(key string, step int64) (int64, error) {
	return r.client.IncrBy(key, step).Result()
}

func (r redisClient) Decr(key string, step int64) (int64, error) {
	return r.client.DecrBy(key, step).Result()
}

func (r redisClient) ZrangeByScore(key string, max, min string, count int) ([]string, error) {
	zrangeBy := redis.ZRangeBy{
		Max:    max,
		Min:    min,
		Offset: 0,
		Count:  int64(count),
	}
	stringSliceCmd := r.client.ZRangeByScore(key, zrangeBy)
	return stringSliceCmd.Result()
}

func (r redisClient) ZrevRangeByScore(key string, max, min string, count int) ([]string, error) {
	zrangeBy := redis.ZRangeBy{
		Max:    max,
		Min:    min,
		Offset: 0,
		Count:  int64(count),
	}
	stringSliceCmd := r.client.ZRevRangeByScore(key, zrangeBy)
	return stringSliceCmd.Result()
}

func (r redisClient) ZAdd(key string, score float64, value interface{}) error {
	z := redis.Z{
		Score:  score,
		Member: value,
	}
	intCmd := r.client.ZAdd(key, z)
	if intCmd.Err() != nil {
		return intCmd.Err()
	}
	return nil
}

func (r redisClient) ZAddM(key string, members ...redis.Z) error {

	intCmd := r.client.ZAdd(key, members...)
	if intCmd.Err() != nil {
		return intCmd.Err()
	}
	return nil
}

func (r redisClient) ZRem(key string, value interface{}) error {
	intCmd := r.client.ZRem(key, value)
	if intCmd.Err() != nil {
		return intCmd.Err()
	}
	return nil
}

func (r redisClient) ZCount(key, max, min string) (int, error) {
	intCmd := r.client.ZCount(key, min, max)
	count, err := intCmd.Result()
	return int(count), err
}

var logFlag int32 = 1

func SetLogFlag(flag int32) {
	atomic.StoreInt32(&logFlag, flag)

}
