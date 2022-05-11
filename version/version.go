package version

var Version version

func init() {
	Version = version{Version: "v0.0.1-bate.1", Product: "k8s-job-schedule", Website: "https://github.com/hhzhhzhhz/k8s-job-scheduler", License: "Copyright test."}
}

type version struct {
	Version string `json:"version"`
	Product string `json:"author"`
	Website string `json:"website"`
	License string `json:"license"`
}
