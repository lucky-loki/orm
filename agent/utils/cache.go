package utils

import "sync"

type GetMethod func(key interface{}) (interface{}, error)

type Cache struct {
	cache sync.Map
	GetFromOrigin GetMethod
}

// Get 先从缓存中取值，如果不存在再从指定的方法中取值。非线程安全
func (c *Cache) Get(key interface{}) (interface{}, error) {
	v, ok := c.cache.Load(key)
	if ok {
		return v, nil
	}
	vv, err := c.GetFromOrigin(key)
	if err != nil {
		return nil, err
	}
	c.cache.Store(key, vv)
	return vv, nil
}