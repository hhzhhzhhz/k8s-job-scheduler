package utils

import (
	"fmt"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/constant"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/api/core/v1"
	"strings"
)

const (
	prefixSplit  = "Created pod:"
	JobNameSplit = "spec.containers{"
)

func ParsePodName(event *v1.EventList) string {
	var podName string
	for _, v := range event.Items {
		if v.Reason == constant.SuccessfulCreate.String() {
			if strings.Contains(v.Message, prefixSplit) {
				podlist := strings.Split(v.Message, prefixSplit)
				if len(podlist) == 2 {
					podName = strings.ReplaceAll(podlist[1], " ", "")
					break
				}
			}
		}
	}
	return podName
}

func GetPodName(message string) string {
	var podName string
	podlist := strings.Split(message, prefixSplit)
	if len(podlist) == 2 {
		podName = strings.ReplaceAll(podlist[1], " ", "")
	}
	return podName
}

func GetJobName(fieldPath string) string {
	var podName string
	podlist := strings.Split(fieldPath, JobNameSplit)
	if len(podlist) == 2 {
		podName = strings.ReplaceAll(podlist[1], "}", "")
	}
	return podName
}

func ParseConditions(cs []batchv1.JobCondition) string {
	if cs == nil {
		return ""
	}
	res := ""
	for _, c := range cs {
		res = res + fmt.Sprintf("type=%s status=%s reason=%s message=%s|", c.Type, c.Status, c.Reason, c.Message)
	}
	return res

}

func CheckEvent(labels map[string]string) bool {
	label, ok := labels[JobFrom]
	return ok && label == constant.LabelFrom
}

func ParseEventLabels(labels map[string]string) (jobId, jobName, jobType string) {
	return labels[JobId], labels[JobName], labels[JobType]
}

func NameSpace(str string) string {
	if str == "" {
		return "All"
	}
	return str
}

// ParsePodStatus todo
func ParsePodStatus(pod v1.PodStatus) string {
	return fmt.Sprintf("%s", pod.Phase)
}
