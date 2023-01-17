package machieid

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

// 测试 N 个线程并发申请 MachineID
func TestConcurrentRetriveMachineID(t *testing.T) {
	rc := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	_, err := ResetMachineIDPool(context.Background(), rc)
	if err != nil {
		t.Fatal(err)
		return
	}

	syncMap := sync.Map{}
	wg := sync.WaitGroup{}

	// count 个线程
	count := 100
	// 成功申请 MachineID 的线程数量
	var successCounter int32
	// 失败申请 MachineID 的线程数量
	var failCounter int32

	var counter int32
	for i := 0; i < count; i++ {
		wg.Add(1)
		t.Run(fmt.Sprintf("worker-%d", i), func(t *testing.T) {
			defer wg.Done()
			t.Parallel()
			id := atomic.AddInt32(&counter, 1)
			mg := NewMachineIDManager(rc, &ProcessInfo{PID: int(id)})
			machineID, err := mg.Retrive(context.Background())

			// 属于正常的业务异常
			if err != nil && err == ErrMachineIDPoolDrained {
				t.Logf("申请 MachineID 失败：池子已经枯竭")
				atomic.AddInt32(&failCounter, 1)
				return
			}

			// 不是正常的业务异常，只有可能是 redis 抛出的异常
			if err != nil && err != ErrMachineIDPoolDrained {
				t.Fatalf("申请 Machine ID 失败：%v", err)
				atomic.AddInt32(&failCounter, 1)
				return
			}

			// 一个 Machine ID 只能被一个线程申请到
			// 如果 syncMap 中已经有值，说明重复颁发了
			_, loaded := syncMap.LoadOrStore(machineID, mg)
			if loaded {
				t.Fatalf("Machine ID 重复颁发了！")
				return
			} else {
				atomic.AddInt32(&successCounter, 1)
			}
		})
	}

	t.Run("统计", func(t *testing.T) {
		t.Parallel()
		wg.Wait()
		t.Logf("MachineID 池子大小：%d", TotalMachineIDAmount())
		t.Logf("申请 MachineID 总共的线程数量：%d", count)
		t.Logf("申请 MachineID 成功的线程数量：%d", successCounter)
		t.Logf("申请 MachineID 失败的线程数量：%d", failCounter)
		if successCounter < int32(TotalMachineIDAmount()) {
			t.Fatalf("结果不符合预期")
		}
	})
}

// 测试单个线程申请 MachineID 及 Extension 延长占用时长的逻辑
func TestExtension(t *testing.T) {
	rc := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	_, err := ResetMachineIDPool(context.Background(), rc)
	if err != nil {
		t.Fatal(err)
		return
	}
	manager := NewMachineIDManager(rc, nil)

	machineID, err := manager.Retrive(context.Background())
	if err != nil {
		t.Fatal(err)
		return
	}

	t.Logf("申请到的 MachineID 为：%d", machineID)

	var check = func() time.Duration {
		localTTL, err := manager.TTL(context.Background(), false)
		if err != nil {
			t.Fatal(err)
			return 0
		}
		t.Logf("local ttl: %s", localTTL.String())
		remoteTTL, err := manager.TTL(context.Background(), true)
		if err != nil {
			t.Fatal(err)
			return 0
		}
		if math.Abs(localTTL.Seconds()-remoteTTL.Seconds()) > float64(time.Second) {
			t.Fatalf("本地 TTL 和远程 TTL 差距大于 1 秒")
		}

		t.Logf("remote ttl: %s", remoteTTL.String())
		return localTTL
	}

	check()
	time.Sleep(time.Second * 5)
	ttl1 := check()

	err = manager.Extension(context.Background())
	if err != nil {
		t.Fatalf("延长 MachineID TTL 失败：%v", err)
		return
	}
	t.Logf("延长 MachineID TTL 成功")
	ttl2 := check()
	assert.Greater(t, ttl2, ttl1)
}
