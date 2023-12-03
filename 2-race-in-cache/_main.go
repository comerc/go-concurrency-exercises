//////////////////////////////////////////////////////////////////////
//
// Given is some code to cache key-value pairs from a database into
// the main memory (to reduce access time). Note that golang's map are
// not entirely thread safe. Multiple readers are fine, but multiple
// writers are not. Change the code to make this thread safe.
//

package main

import (
	"container/list"
	"hash/fnv"
	"sync"
	"testing"
)

// CacheSize determines how big the cache can grow
const CacheSize = 100

// KeyStoreCacheLoader is an interface for the KeyStoreCache
type KeyStoreCacheLoader interface {
	// Load implements a function where the cache should gets it's content from
	Load(string) string
}

type page struct {
	Key   string
	Value string
}

type KeyStoreCache struct {
	shards []shard
	load   func(string) string
}

type shard struct {
	cacheLock sync.RWMutex
	cache     map[string]*list.Element
	pages     list.List
}

func New(load KeyStoreCacheLoader) *KeyStoreCache {
	return NewKeyStoreCache(5, load)
}

func NewKeyStoreCache(numShards int, load KeyStoreCacheLoader) *KeyStoreCache {
	shards := make([]shard, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = shard{
			cacheLock: sync.RWMutex{},
			cache:     make(map[string]*list.Element),
		}
	}
	return &KeyStoreCache{
		shards: shards,
		load:   load.Load,
	}
}

func (k *KeyStoreCache) Get(key string) string {
	shardIndex := hash(key) % uint32(len(k.shards))
	shard := &k.shards[shardIndex]

	shard.cacheLock.Lock()
	defer shard.cacheLock.Unlock()

	if e, ok := shard.cache[key]; ok {
		shard.pages.MoveToFront(e)
		return e.Value.(*page).Value
	}

	// Miss - load from database and save it in cache
	p := page{Key: key, Value: k.load(key)}

	// if cache is full remove the least used item
	if len(shard.cache) >= CacheSize {
		end := shard.pages.Back()
		// remove from map
		delete(shard.cache, end.Value.(*page).Key)
		// remove from list
		shard.pages.Remove(end)
	}

	// add new page to cache
	elem := shard.pages.PushFront(&p)
	shard.cache[key] = elem

	return p.Value
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// Loader implements KeyStoreLoader
type Loader struct {
	DB *MockDB
}

// Load gets the data from the database
func (l *Loader) Load(key string) string {
	val, err := l.DB.Get(key)
	if err != nil {
		panic(err)
	}

	return val
}

func run(t *testing.T) (*KeyStoreCache, *MockDB) {
	loader := Loader{
		DB: GetMockDB(),
	}
	cache := New(&loader)

	RunMockServer(cache, t)

	return cache, loader.DB
}

func main() {
	run(nil)
}
