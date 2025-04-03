package lock

import (
	"fmt"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	clientId string // id for each client
	lockId   string // id for each lock
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.clientId = kvtest.RandValue(8)
	lk.lockId = l
	return lk
}

func (lk *Lock) Acquire() {
	key := lk.getLockKey()

	for {
		val, version, err := lk.ck.Get(key)

		if err == rpc.ErrNoKey {
			lk.ck.Put(key, "", 0)
		}

		if err == rpc.OK && val == "" {
			err = lk.ck.Put(key, lk.clientId, version)

			if err == rpc.OK {
				return
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	key := lk.getLockKey()

	val, version, _ := lk.ck.Get(key)

	if val == lk.clientId {
		lk.ck.Put(key, "", version)
	}
}

func (lk *Lock) getLockKey() string {
	return fmt.Sprintf("Lock_&&_{%s}_&&_Lock", lk.lockId)
}
