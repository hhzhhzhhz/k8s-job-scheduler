package utils

import (
	"sync"
	"time"
)

const (
	cacheSize = 10
)

func NewDeduplication() *Deduplication {
	beguile := &Deduplication{
		cache: make(map[string]int64, cacheSize),
	}
	go beguile.async()
	return beguile
}

type Deduplication struct {
	mux   sync.Mutex
	cache map[string]int64
}

func (d *Deduplication) Exist(uid string) bool {
	d.mux.Lock()
	defer d.mux.Unlock()
	_, ok := d.cache[uid]
	if ok {
		return ok
	}
	d.cache[uid] = time.Now().Unix()
	return ok
}

func (d *Deduplication) async() {
	tick := time.Tick(rotationTime)
	for {
		select {
		case <-tick:
			d.clear()
		}
	}
}

func (d *Deduplication) clear() {
	d.mux.Lock()
	defer d.mux.Unlock()
	now := time.Now().Unix()
	var dels []string
	for k, ts := range d.cache {
		if now-ts > int64(rotationTime.Seconds()) {
			dels = append(dels, k)
		}
	}
	for _, del := range dels {
		delete(d.cache, del)
	}
}
