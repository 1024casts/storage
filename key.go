package storage

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dropbox/godropbox/errors"
)

type Key interface {
	String() string
}
type KeyList []Key

type String string

func (this String) String() string {
	return string(this)
}

type Int int

func (this Int) String() string {
	return strconv.Itoa(int(this))
}

type KeyGetter interface {
	GetKey() (key Key)
}
type KeyChangeable interface {
	IsKeyChangeable() bool // 比如数据库主键auto increment，后会导致key 变化，配合  KeyGetter一起使用
}

type EmptyObjectError struct {
	Key string
}

func (this EmptyObjectError) Error() string {
	return fmt.Sprintf("key %s does not exists", this.Key)
}

func IsErrorEmpty(err error) bool {
	if err == nil {
		return false
	}

	switch err.(type) {
	case EmptyObjectError:
		return true
	default:
		return false
	}
}

func IntList2KeyList(intList []int) (keyList []Key) {
	keyList = make([]Key, len(intList))
	for idx, i := range intList {
		keyList[idx] = Int(i)
	}
	return
}

func Int64List2KeyList(intList []int64) (keyList []Key) {
	keyList = make([]Key, len(intList))
	for idx, i := range intList {
		keyList[idx] = Int(int(i))
	}
	return
}

func KeyList2Int64List(keyList []Key) (intList []int64) {
	intList = make([]int64, len(keyList))
	for idx, key := range keyList {
		intList[idx] = int64(int(key.(Int)))
	}
	return
}

func KeyList2IntList(keyList []Key) (intList []int) {
	intList = make([]int, len(keyList))
	for idx, key := range keyList {
		intList[idx] = int(key.(Int))
	}
	return
}

func StringList2KeyList(stringList []string) (keyList []Key) {
	keyList = make([]Key, len(stringList))
	for idx, i := range stringList {
		keyList[idx] = String(i)
	}
	return
}

func KeyList2StringList(keyList []Key) (stringList []string) {
	stringList = make([]string, len(keyList))
	for idx, i := range keyList {
		stringList[idx] = i.String()
	}
	return
}

func BuildCacheKey(keyPrefix string, key Key) (cacheKey string, err error) {
	if key == nil || key.String() == "" {
		return "", errors.New("key should not be nil or to string should not be empty string")
	}
	cacheKey, err = strings.Join([]string{keyPrefix, key.String()}, "_"), nil
	// log.Info("redis_cache_key ", cacheKey)
	return
}

func BuildintCacheKey(keyPrefix string, key int) (cacheKey string) {
	cacheKey = fmt.Sprintf("%s_%d", keyPrefix, key)
	// log.Info("redis_cache_key ", cacheKey)
	return
}

func Buildint64CacheKey(keyPrefix string, key int64) (cacheKey string) {
	cacheKey = fmt.Sprintf("%s_%d", keyPrefix, key)
	// log.Info("redis_cache_key ", cacheKey)
	return
}

func BuildstringCacheKey(keyPrefix string, key string) (cacheKey string) {
	cacheKey = fmt.Sprintf("%s_%s", keyPrefix, key)
	// log.Info("redis_cache_key ", cacheKey)
	return
}

func GetRawKey(key string) (rawKey String) {
	keys := strings.Split(key, "_")
	return String(keys[len(keys)-1])
}

func isInStringSlice(strList []string, ele string) bool {
	for _, value := range strList {
		if ele == value {
			return true
		}
	}
	return false
}
