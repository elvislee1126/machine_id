package machieid

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	ErrMachineIDNotConsistent = fmt.Errorf("local machine id is not consistent with remote")
	ErrUnAllocMachineID       = fmt.Errorf("un alloc machie id")
	ErrMachineIDExpire        = fmt.Errorf("machine id expire")
	ErrMachineIDPoolDrained   = fmt.Errorf("machine id pool is drained")
)

const (
	UnAllocMachineID = -1
	// 可以使用的 MachineID 的左闭区间
	MachineIDMin = 0
	// 可以使用的 MachineID 的右闭区间
	MachineIDMax = 2<<16 - 1
	// 申请回来的 MachineID 的可占用时长，对应 redis 的 TTL
	MachineIDUsageExpiration = time.Minute * 4
	// 延长使用时长的周期，设置为占用时长的约 1 / 3，
	// 保证在一个可使用时长内，可以重试 3 次
	MachineIDUsageExtensionInterval = time.Minute * 1
)

const (
	K8SPodNameEnvKey   = "HOSTNAME"
	K8SPodNameSpaceKey = "GM_K8S_NAMESPACE"
)

type MachineIDManager struct {
	redisCli        *redis.Client
	usageInfo       *MachineIDUsage
	extensionTicker *time.Ticker

	process *ProcessInfo
}

type ProcessInfo struct {
	PodNs   string
	PodName string
	PID     int
}

func NewMachineIDManager(
	redisCli *redis.Client,
	processInfo *ProcessInfo,
) *MachineIDManager {
	if processInfo == nil {
		processInfo = &ProcessInfo{
			PID:     os.Getpid(),
			PodNs:   os.Getenv(K8SPodNameSpaceKey),
			PodName: os.Getenv(K8SPodNameEnvKey),
		}
	}
	m := &MachineIDManager{
		process:  processInfo,
		redisCli: redisCli,
		usageInfo: newMachineIDUsage(
			processInfo,
			UnAllocMachineID,
		),
	}
	return m
}

func MachineIDUsageKey(id int) string {
	return fmt.Sprintf("ceres:id_generator:machine_id_usage:%d", id)
}

// 危险！！清空 redis 所有 machine id 的占用信息
// 清空后，会出现线上实例进程本地状态与 redis 状态不一致的情况
// 最久约 MachineIDUsageExtensionInterval 时间后
// （在该时间段之内，可能会有多个实例进程本地 MachineID 值相同的情况，且 GeneratorID 接口正常工作，但是可能生成重复 ID），
// 调用 CurrentMachineID 获取进程本地 MachineID 会返回 error
func ResetMachineIDPool(
	ctx context.Context,
	redisCli *redis.Client,
) ([]*MachineIDUsage, error) {
	arr := make([]*MachineIDUsage, 0, MachineIDMax+1)
	for i := MachineIDMin; i <= MachineIDMax; i++ {
		key := MachineIDUsageKey(i)
		val, err := redisCli.Get(ctx, key).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			return arr, err
		}
		if val == "" {
			continue
		}
		usage := new(MachineIDUsage)
		err = json.Unmarshal([]byte(val), usage)
		if err != nil {
			return arr, err
		}
		_, err = redisCli.Del(context.Background(), key).Result()
		if err != nil {
			return arr, err
		}
		arr = append(arr, usage)
	}
	return arr, nil
}

// 返回池子大小
func TotalMachineIDAmount() int {
	return MachineIDMax - MachineIDMin + 1
}

// 当前申请到的 MachineID
func (t *MachineIDManager) CurrentMachineID() (int, error) {
	err := t.IsValid()
	if err != nil {
		return UnAllocMachineID, err
	}
	return t.usageInfo.MachineID, nil
}

// 校验本地的 MachineID 是否可以正常使用
func (t *MachineIDManager) IsValid() error {
	if t.usageInfo.MachineID == UnAllocMachineID {
		return ErrUnAllocMachineID
	}
	if t.usageInfo.NotConsistent {
		return ErrMachineIDNotConsistent
	}
	if t.usageInfo.ExpireAt < time.Now().UnixMilli() {
		return ErrMachineIDExpire
	}
	return nil
}

// 申请 MachineID
// 且成功后，开启刷新占用时长的定时任务
func (t *MachineIDManager) RetriveAndGoExtension(
	ctx context.Context,
) (int, error) {
	val, err := t.Retrive(ctx)
	if err == nil {
		t.GoExtension()
	}
	return val, err
}

// 归还 MachineID
// 且成功后，关闭刷新占用时长的定时任务
func (t *MachineIDManager) RevertAndStopExtension(
	ctx context.Context,
) (int, error) {
	id, err := t.Revert(ctx)
	if err == nil {
		t.StopExtension()
	}
	return id, err
}

// 申请/占用 MachineID 一段时间
func (t *MachineIDManager) Retrive(
	ctx context.Context,
) (int, error) {
	_, err := WithRetry(func() (interface{}, error) {
		err := t.retrive(ctx)
		return nil, err
	}, RetryOptions{
		Retry: 3,
		Delay: []time.Duration{time.Second},
	})
	if err != nil {
		return UnAllocMachineID, err
	}
	return t.usageInfo.MachineID, nil
}

// 归还 MachineID
func (t *MachineIDManager) Revert(
	ctx context.Context,
) (int, error) {
	revertedMachineID, err := WithRetry(func() (int, error) {
		return t.revert(ctx)
	}, RetryOptions{
		Retry: 3,
		Delay: []time.Duration{time.Second},
	})
	return revertedMachineID, err
}

// 延长 MachineID 的使用时长
func (t *MachineIDManager) Extension(
	ctx context.Context,
) error {
	_, err := WithRetry(func() (interface{}, error) {
		err := t.extension(ctx)
		return nil, err
	}, RetryOptions{
		Retry: 3,
		Delay: []time.Duration{time.Second},
	})
	return err
}

// 定期延长 MachineID 的使用时长
func (t *MachineIDManager) GoExtension() {
	if t.extensionTicker != nil {
		t.extensionTicker.Stop()
	}
	t.extensionTicker = time.NewTicker(MachineIDUsageExtensionInterval)
	go func() {
		for range t.extensionTicker.C {
			ctx := context.Background()
			t.Extension(ctx)
		}
	}()
}

func (t *MachineIDManager) StopExtension() {
	if t.extensionTicker == nil {
		return
	}
	t.extensionTicker.Stop()
	t.extensionTicker = nil
}

// 查询指定的 MachineID 当前的持有者信息
func (t *MachineIDManager) QueryHolder(
	ctx context.Context,
	machieID int,
	forceRemote bool,
) (*ProcessInfo, error) {
	if !forceRemote && t.usageInfo.MachineID != UnAllocMachineID {
		local := t.usageInfo
		if !local.NotConsistent && local.ExpireAt >= time.Now().UnixMilli() {
			clone := *t.usageInfo.Holder
			return &clone, nil
		}
	}

	key := MachineIDUsageKey(machieID)
	val, err := t.redisCli.Get(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	usage := &MachineIDUsage{}
	err = json.Unmarshal([]byte(val), usage)
	if err != nil {
		return nil, err
	}
	return usage.Holder, nil
}

// 若 time.Duration 为 0 且 error 为 nil，代表没有 TTL
func (t *MachineIDManager) TTL(
	ctx context.Context, forceRemote bool,
) (time.Duration, error) {
	if t.usageInfo.MachineID == UnAllocMachineID {
		return 0, ErrUnAllocMachineID
	}

	if !forceRemote {
		expireAt := time.UnixMilli(t.usageInfo.ExpireAt)
		return expireAt.Sub(time.Now()), nil
	}

	rc := t.redisCli
	key := MachineIDUsageKey(t.usageInfo.MachineID)

	reply, err := rc.TTL(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	// -2 代表 key 不存在
	if reply == -2 {
		return 0, ErrUnAllocMachineID
	}
	// -1 代表 key 没有 ttl
	if reply == -1 {
		return 0, nil
	}
	return reply, nil
}

func (t *MachineIDManager) retrive(
	ctx context.Context,
) error {
	rc := t.redisCli
	for i := MachineIDMin; i <= MachineIDMax; i++ {
		var request *MachineIDUsage
		if t.usageInfo != nil {
			request = t.usageInfo.Clone()
			request.MachineID = i
		} else {
			request = newMachineIDUsage(t.process, i)
		}
		request.RetriveTimeMs = time.Now().UnixMilli()
		request.ExpireAt = request.RetriveTimeMs + MachineIDUsageExpiration.Milliseconds()

		key := MachineIDUsageKey(i)

		ok, err := WithRetry(func() (bool, error) {
			ok, err := rc.SetNX(ctx, key, request.String(), MachineIDUsageExpiration).Result()
			return ok, err
		}, RetryOptions{
			Retry: 3,
			Delay: []time.Duration{time.Second},
		})

		if err != nil {
			return err
		}
		if ok {
			t.usageInfo = request
			return nil
		}
	}

	return ErrMachineIDPoolDrained
}

func (t *MachineIDManager) revert(
	ctx context.Context,
) (int, error) {
	if t.usageInfo.MachineID == UnAllocMachineID {
		return UnAllocMachineID, nil
	}
	request := t.usageInfo
	key := MachineIDUsageKey(request.MachineID)
	_, err := t.redisCli.Del(ctx, key).Result()
	if err != nil {
		return request.MachineID, err
	}
	t.usageInfo = newMachineIDUsage(t.process, UnAllocMachineID)
	return request.MachineID, nil
}

func (t *MachineIDManager) extension(
	ctx context.Context,
) error {
	if t.usageInfo.MachineID == UnAllocMachineID {
		return nil
	}
	request := t.usageInfo.Clone()
	request.ExpireAt = time.Now().Add(MachineIDUsageExpiration).UnixMilli()
	key := MachineIDUsageKey(request.MachineID)

	ok, err := t.redisCli.SetXX(ctx, key, request.String(), MachineIDUsageExpiration).Result()
	if err != nil {
		return err
	}
	if !ok {
		// 代表 redis 中的 key 已经过期，此时不更新进程本地的状态
		// 除非有人手动删除了 redis 中的 key
		// 或者 3 次都刷新失败了
		// 或者 redis 持久化失败了
		// 否则不会进到这个分支，概率极小
		t.usageInfo.NotConsistent = true
	} else {
		// 更新进程本地的状态
		t.usageInfo = request
	}
	return nil
}

type MachineIDUsage struct {
	MachineID     int // 0 也是有效位
	Holder        *ProcessInfo
	RetriveTimeMs int64 // 占用该 MachineID 时的毫秒时间戳
	ExpireAt      int64

	// 进程本地数据是否和远端 redis 一致，
	// 当远端 redis 的 key 不存在时（没有严格校验字段是否完全一致），
	// 该字段为 true
	NotConsistent bool
}

func newMachineIDUsage(
	holder *ProcessInfo,
	machineID int,
) *MachineIDUsage {
	usage := &MachineIDUsage{}
	usage.Holder = holder
	usage.MachineID = machineID
	return usage
}

func (i *MachineIDUsage) String() string {
	buf, _ := json.Marshal(i)
	return string(buf)
}

func (i *MachineIDUsage) Clone() *MachineIDUsage {
	return &MachineIDUsage{
		MachineID: i.MachineID,
		Holder: &ProcessInfo{
			PID:     i.Holder.PID,
			PodNs:   i.Holder.PodNs,
			PodName: i.Holder.PodName,
		},
		RetriveTimeMs: i.RetriveTimeMs,
	}
}
