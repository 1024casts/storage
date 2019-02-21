package storage

import (
	"time"

	"github.com/dropbox/godropbox/errors"
	log "github.com/golang/glog"
	"github.com/1024casts/go-common/context"
)

type CounterStorage interface {
	Get(ctx *context.Context, key Key) (value int64, err error)
	Set(ctx *context.Context, key Key, value int64) error
	Incr(ctx *context.Context, key Key, step int64) (newValue int64, err error)
	Decr(ctx *context.Context, key Key, step int64) (newValue int64, err error)
	Delete(ctx *context.Context, key ...Key) error
	MultiGet(ctx *context.Context, keys []Key, values map[Key]int64) (err error)
	MultiSet(ctx *context.Context, m map[Key]int64) error
}

type CounterRedisStorage struct {
	client             RedisClient
	KeyPrefix          string
	BenchMarkKeyPrefix string
	DefaultExpireTime  time.Duration
	encoding           Encoding
}

func NewCounterRedisStorage(client RedisClient, keyPrefix string, BenchMarkKeyPrefix string, defaultExpireTime time.Duration) CounterStorage {
	return CounterRedisStorage{client, keyPrefix, BenchMarkKeyPrefix, defaultExpireTime, Int64Encoding{}}
}

func (this CounterRedisStorage) Incr(ctx *context.Context, key Key, step int64) (newValue int64, err error) {
	var cacheKey string
	cacheKey, err = BuildCacheKey(this.KeyPrefix, key)
	if err != nil {
		return 0, errors.Wrap(err, "build cache key error")

	}

	result, errcache := this.client.Incr(cacheKey, step)
	if this.DefaultExpireTime > 0 {
		this.client.Expire(cacheKey, this.DefaultExpireTime)
	}

	return int64(result), errcache
}

func (this CounterRedisStorage) Decr(ctx *context.Context, key Key, step int64) (newValue int64, err error) {
	var cacheKey string
	cacheKey, err = BuildCacheKey(this.KeyPrefix, key)
	if err != nil {
		return 0, errors.Wrap(err, "build cache key error")

	}
	result, errcache := this.client.Decr(cacheKey, step)
	if result < 0 {
		return 0, err
	}
	if this.DefaultExpireTime > 0 {
		this.client.Expire(cacheKey, this.DefaultExpireTime)
	}
	return int64(result), errcache
}

func (this CounterRedisStorage) Get(ctx *context.Context, key Key) (value int64, err error) {
	var cacheKey string
	cacheKey, err = BuildCacheKey(this.KeyPrefix, key)
	if err != nil {
		return 0, errors.Wrap(err, "build cache key error")

	}

	data, err := this.client.Get(cacheKey)
	if err != nil {
		if err.Error() == "redis: nil" {
			// log.Infoln(err)
		} else {
			return 0, errors.Wrapf(err, "get from redis error key is %s", cacheKey)
		}
	}
	if data == nil || len(data) == 0 {
		return 0, EmptyObjectError{key.String()}
	}
	err = this.encoding.Unmarshal(data, &value)
	if err != nil {
		return 0, errors.Wrapf(err, "unmarshal  error , is %s ", string(data))
	}
	return value, nil
}

func (this CounterRedisStorage) Set(ctx *context.Context, key Key, value int64) error {

	var (
		cacheKey string
		err      error
	)
	cacheKey, err = BuildCacheKey(this.KeyPrefix, key)
	if err != nil {
		return errors.Wrap(err, "build cache key error")
	}

	buf, err := this.encoding.Marshal(value)
	if err != nil {
		return errors.Wrapf(err, "marshal  error,data is %+v", value)
	}

	if err = this.client.Set(cacheKey, buf, this.DefaultExpireTime); err != nil {
		return errors.Wrap(err, "redis set error")
	}
	return nil
}

func (this CounterRedisStorage) Delete(ctx *context.Context, keyList ...Key) error {
	if len(keyList) == 0 {
		return nil
	}
	var cacheKeyList []string = make([]string, len(keyList))
	var err error
	for storagekeyIdx, key := range keyList {
		cacheKeyList[storagekeyIdx], err = BuildCacheKey(this.KeyPrefix, key)
		if err != nil {
			return errors.Wrapf(err, "build cache key error ,key is %+v", key)
		}
	}
	_, err = this.client.Del(cacheKeyList...)
	if err != nil {
		return errors.Wrapf(err, "redis delete error,keys is %+v", keyList)
	}
	return nil
}

func (this CounterRedisStorage) MultiGet(ctx *context.Context, keys []Key, values map[Key]int64) (err error) {
	if len(keys) == 0 {
		return nil
	}

	cacheKeys := make([]string, len(keys))
	for index, key := range keys {
		cacheKey := ""
		cacheKey, err = BuildCacheKey(this.KeyPrefix, key)
		if err != nil {
			return errors.Wrapf(err, "build cache key error ,key is %+v", key)
		}
		cacheKeys[index] = cacheKey
	}

	val, err := this.client.MGet(cacheKeys...)
	if err != nil {
		return errors.Wrap(err, "redis get error")
	}

	for i, value := range val {
		if value == nil {
			continue
		}
		var object int64
		err := this.encoding.Unmarshal([]byte(value.(string)), &object)
		if err != nil {
			log.Warningf("cant't unmarshal json ,json string is %+v", value)
			continue
		}
		values[keys[i]] = object
	}
	return nil
}

func (this CounterRedisStorage) MultiSet(ctx *context.Context, valueMap map[Key]int64) error {
	if len(valueMap) == 0 {
		return nil
	}
	values := make([]interface{}, 0, 2*len(valueMap))
	for key, value := range valueMap {
		buf, err := this.encoding.Marshal(value)
		if err != nil {
			log.Warningf("cant't unmarshal json ,json string is %+v", value)
			continue
		}
		cacheKey := ""
		cacheKey, err = BuildCacheKey(this.KeyPrefix, key)
		if err != nil {
			log.Warningf("build cache key error ,key is %+v", key)
			continue
		}
		values = append(values, ([]byte(cacheKey)))
		values = append(values, (buf))
	}

	err := this.client.MSet(this.DefaultExpireTime, values...)
	if err != nil {
		return errors.Wrap(err, "redis set error")
	}
	return nil
}
