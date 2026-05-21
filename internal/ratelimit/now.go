package ratelimit

import "time"

func nowMillisReal() int64 { return time.Now().UnixMilli() }
