package storage

import (
	"reflect"

	log "github.com/golang/glog"
	"github.com/1024casts/go-common/context"
)

type Storage interface {
	Get(ctx *context.Context, key Key, value interface{}) error
	Set(ctx *context.Context, key Key, object interface{}) error
	Add(ctx *context.Context, key Key, object interface{}) error
	MultiGet(ctx *context.Context, keys []Key, valuesMap interface{}) error
	MultiSet(ctx *context.Context, values map[Key]interface{}) error
	Delete(ctx *context.Context, key ...Key) error
}

type StorageProxy struct {
	PreferedStorage Storage
	BackupStorage   Storage
}

func NewStorageProxy(prefered, backup Storage) *StorageProxy {
	return &StorageProxy{
		PreferedStorage: prefered,
		BackupStorage:   backup,
	}
}

func (this *StorageProxy) Get(ctx *context.Context, key Key, value interface{}) error {
	err := this.PreferedStorage.Get(ctx, key, value)
	if err != nil && reflect.TypeOf(err).Name() == "EmptyObjectError" {
		err = this.BackupStorage.Get(ctx, key, value)
		if err != nil {
			return err
		}
		err = this.PreferedStorage.Set(ctx, key, value)
		if err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	return nil
}

func (this *StorageProxy) Add(ctx *context.Context, key Key, object interface{}) error {
	// 这段代码先BackupStorage后PreferedStorage
	// 因为数据库有auto increment 的情况，add时，key无意义
	// 需要插入成功后再来获取key
	if object != nil {
		err := this.BackupStorage.Add(ctx, key, object)
		if err != nil {
			return err
		}
		keyChangeableObj, iskeyChangeableI := object.(KeyChangeable)
		keyGetterObj, isKeyGetterI := object.(KeyGetter)
		if iskeyChangeableI && isKeyGetterI {
			if keyChangeableObj.IsKeyChangeable() {
				key = keyGetterObj.GetKey()
			}
		}

		err = this.PreferedStorage.Add(ctx, key, object)
		if err != nil {
			return err
		}
	}
	return nil
}

func (this *StorageProxy) Set(ctx *context.Context, key Key, object interface{}) error {
	if object != nil {
		err := this.PreferedStorage.Set(ctx, key, object)
		if err != nil {
			return err
		}
		err = this.BackupStorage.Set(ctx, key, object)
		if err != nil {
			return err
		}
	}
	return nil
}

func (this *StorageProxy) MultiGet(ctx *context.Context, keys []Key, valuesMap interface{}) error {
	err := this.PreferedStorage.MultiGet(ctx, keys, valuesMap)
	if err != nil {
		log.Warning(err)
		return nil
	}
	missedKeyCount := 0
	valueMapReflect := reflect.ValueOf(valuesMap)
	missedKeys := make([]Key, 0, missedKeyCount)
	for _, key := range keys {
		if !valueMapReflect.MapIndex(reflect.ValueOf(key)).IsValid() {
			missedKeyCount++
			missedKeys = append(missedKeys, key)
		}
	}
	if missedKeyCount > 0 {
		missedMap := make(map[Key]interface{})
		err := this.BackupStorage.MultiGet(ctx, missedKeys, missedMap)
		if err != nil {
			return err
		}
		if len(missedMap) == 0 {
			return nil
		}
		this.PreferedStorage.MultiSet(ctx, missedMap)
		for k, v := range missedMap {
			valueMapReflect.SetMapIndex(reflect.ValueOf(k), reflect.ValueOf(v))
		}
	}
	return nil
}

func (this *StorageProxy) MultiSet(ctx *context.Context, objectMap map[Key]interface{}) error {
	err := this.PreferedStorage.MultiSet(ctx, objectMap)
	if err != nil {
		return err
	}
	err = this.BackupStorage.MultiSet(ctx, objectMap)
	if err != nil {
		return err
	}
	return nil
}

func (this *StorageProxy) Delete(ctx *context.Context, key ...Key) error {
	err := this.BackupStorage.Delete(ctx, key...)
	if err != nil {
		return err
	}
	err = this.PreferedStorage.Delete(ctx, key...)
	if err != nil {
		return err
	}
	return nil
}

func (this *StorageProxy) Incr(ctx *context.Context, key Key, step int64) (newValue int64, err error) {
	result, err := this.PreferedStorage.(CounterStorage).Incr(ctx, key, step)
	if err != nil {
		return result, err
	}
	result, err = this.BackupStorage.(CounterStorage).Incr(ctx, key, step)
	if err != nil {
		return result, err
	}
	return result, err
}

func (this *StorageProxy) Decr(ctx *context.Context, key Key, step int64) (newValue int64, err error) {
	result, err := this.PreferedStorage.(CounterStorage).Decr(ctx, key, step)
	if err != nil {
		return result, err
	}
	result, err = this.BackupStorage.(CounterStorage).Decr(ctx, key, step)
	if err != nil {
		return result, err
	}
	return result, err
}
