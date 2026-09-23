package alert

import "time"

// atTime converts a unix-nanosecond observation time to time.Time.
func atTime(ns int64) time.Time {
	if ns <= 0 {
		return time.Time{}
	}
	return time.Unix(0, ns).UTC()
}
