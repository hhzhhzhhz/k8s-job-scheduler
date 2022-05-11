package utils

import (
	"fmt"
	"runtime"
)

func Caller(skip int) string {
	_, file, line, ok := runtime.Caller(skip)
	if ok {
		short := file
		for i := len(file) - 1; i > 0; i-- {
			if file[i] == '/' {
				short = file[i+1:]
				break
			}
		}
		file = short
		return fmt.Sprintf("%s:%d", file, line)
	}
	return ""
}
