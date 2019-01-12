package storage

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/dropbox/godropbox/errors"
	log "github.com/golang/glog"
)

type Order int

type RedisStorage struct {
	client             RedisClient
	KeyPrefix          string
	BenchMarkKeyPrefix string
	DefaultExpireTime  time.Duration
	encoding           Encoding
	newObject          func() interface{}
	order              Order // int list( ASC or DESC)
	needBenchMark      bool
}

type BytesValue []byte

func (this BytesValue) MarshalBinary() (data []byte, err error) {
	return this, nil
}

func NewRedisStorage(client RedisClient, keyPrefix string, defaultExpireTime time.Duration, encoding Encoding, newObject func() interface{}, needBenchMark bool) RedisStorage {
	return newRedisStorage(client, keyPrefix, defaultExpireTime, 0, encoding, newObject, needBenchMark)
}

func newRedisStorage(client RedisClient, keyPrefix string, defaultExpireTime time.Duration, order Order, encoding Encoding, newObject func() interface{}, needBenchMark bool) RedisStorage {
	keyPrefix = strings.Replace(keyPrefix, "_", "~", -1)
	if needBenchMark == true {
		return RedisStorage{client, keyPrefix, fmt.Sprintf("bench~%s", keyPrefix), defaultExpireTime, encoding, newObject, order, needBenchMark}
	} else {
		return RedisStorage{client, keyPrefix, keyPrefix, defaultExpireTime, encoding, newObject, order, needBenchMark}
	}
}

func (this RedisStorage) Get(key Key, value interface{}) error {
	var cacheKey string
	var err error
	cacheKey, err = BuildCacheKey(this.KeyPrefix, key)
	if err != nil {
		return errors.Wrap(err, "build cache key error")
	}

	data, err := this.client.Get(cacheKey)
	if err != nil {
		if err.Error() == "redis: nil" {
			// log.Infoln(err)
		} else {
			return errors.Wrapf(err, "get from redis error key is %s", cacheKey)
		}
	}
	if data == nil {
		return EmptyObjectError{key.String()}
	}
	err = Unmarshal(this.encoding, data, value)
	// err = this.encoding.Unmarshal(data, value)
	if err != nil {
		return errors.Wrapf(err, "unmarshal json error ,key=%s,cachekey=%s type=%v ,json is %s ", key.String(), cacheKey, reflect.TypeOf(value), string(data))
	}
	return nil
}

func (this RedisStorage) Add(key Key, object interface{}) error {
	return this.Set(key, object)
}

func (this RedisStorage) Set(key Key, object interface{}) error {
	buf, err := Marshal(this.encoding, object)
	// buf, err := this.encoding.Marshal(object)
	if err != nil {
		return errors.Wrapf(err, "marshal json error,data is %+v", object)
	}
	var cacheKey string
	cacheKey, err = BuildCacheKey(this.KeyPrefix, key)
	if err != nil {
		return errors.Wrap(err, "build cache key error")

	}

	if err != nil {
		return errors.Wrapf(err, "build cache key error ,key is %+v", key)
	}

	if err = this.client.Set(cacheKey, buf, this.DefaultExpireTime); err != nil {
		return errors.Wrap(err, "redis set error")
	}
	return nil
}

func (this RedisStorage) MultiGet(keys []Key, value interface{}) error {
	if len(keys) == 0 {
		return nil
	}
	cacheKeys := make([]string, len(keys))
	for index, key := range keys {
		var cacheKey string
		var err error
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

	valueMap := reflect.ValueOf(value)
	for i, value := range val {
		if value == nil {
			continue
		}
		object := this.newObject()
		// err := this.encoding.Unmarshal([]byte(value.(string)), object)
		err = Unmarshal(this.encoding, []byte(value.(string)), object)
		if err != nil {
			log.Warning("cant't unmarshal json ", keys[i].String(), cacheKeys[i], reflect.TypeOf(object), value)
			continue
		}
		valueMap.SetMapIndex(reflect.ValueOf(keys[i]), reflect.ValueOf(object))
	}
	return nil
}

func (this RedisStorage) MultiSet(valueMap map[Key]interface{}) error {
	if len(valueMap) == 0 {
		return nil
	}
	values := make([]interface{}, 0, 2*len(valueMap))
	for key, value := range valueMap {
		// buf, err := this.encoding.Marshal(value)
		buf, err := Marshal(this.encoding, value)
		if err != nil {
			log.Warningf("cant't unmarshal json ,json string is %+v", value)
			continue
		}
		var cacheKey string
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

func (this RedisStorage) Delete(keyList ...Key) error {
	if len(keyList) == 0 {
		return nil
	}
	var cacheKeyList []string = make([]string, len(keyList))
	var err error
	for storagekeyIdx, key := range keyList {
		var cacheKey string
		cacheKey, err = BuildCacheKey(this.KeyPrefix, key)
		if err != nil {
			log.Warningf("build cache key error ,key is %+v", key)
			continue
		}

		cacheKeyList[storagekeyIdx] = cacheKey
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
