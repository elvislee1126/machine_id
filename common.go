package machieid

import (
	"time"
)

type Fn[T any] func() (T, error)

const (
	MaxDelaySecond = time.Second * 5
)

type RetryOptions struct {
	// 重试次数
	Retry int

	// 重试间隔时长
	// 若没有指定该参数，默认每次间隔的时长为上一次间隔的时长加一秒（最大不超过 5 秒）
	Delay []time.Duration

	// 重试前执行的钩子函数，被重试函数返回的 error 作为入参
	BeforeRetryHook func(error)
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func WithRetry[T any](fn Fn[T], opts RetryOptions) (T, error) {
	var err error
	var res T
	if len(opts.Delay) <= 0 {
		for idx := 1; idx <= opts.Retry; idx++ {
			delay := time.Second * time.Duration(idx)
			if delay > MaxDelaySecond {
				break
			}
			opts.Delay = append(opts.Delay, delay)
		}
	}
	for counter := 0; counter <= opts.Retry; counter++ {
		if counter > 0 {
			idx := min(len(opts.Delay)-1, counter-1)
			if idx >= 0 {
				delay := opts.Delay[idx]
				time.Sleep(delay)
			}
			if opts.BeforeRetryHook != nil && err != nil {
				opts.BeforeRetryHook(err)
			}
		}
		res, err = fn()
		if err == nil {
			break
		}
	}
	return res, err
}
