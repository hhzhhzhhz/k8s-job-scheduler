package spec

var (
	JobSpec = map[string]string{
		"labels":                     "",
		"annotations":                "",
		"parallelism":                "",                       //并行数量 >0
		"completions":                "",                       // 成功数量
		"restart_policy":             "OnFailure,Always,Never", // 重启策略 default: Never
		"active_deadline_seconds":    "",                       // job存活周期
		"backoff_limit":              "",                       //
		"ttl_seconds_after_finished": "",                       // 清理时间
		"completion_mode":            "NonIndexed,Indexed",     // default: Indexed
		"envs":                       "",                       // 环境变量
		"log_collect":                "",                       // 是否采集日志
		"args":                       "",                       // 运行参数
		"callback_url":               "",                       // 回调接口
	}
)
