package antman

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
)

const (
	KubeDLPrefix      = "kubedl.io"
	ResourceNvidiaGPU = v1.ResourceName("nvidia.com/gpu")
	// AnnotationGPUVisibleDevices is the visible gpu devices in view of process.
	AnnotationGPUVisibleDevices = "gpus." + KubeDLPrefix + "/visible-devices"

	// for antman
	AntManPrefix = "alibaba.pai.antman"
	// for DL jobs to report DL framework information to local coordinator.
	AnnotationGpuStatusFile = AntManPrefix + "/gpu-status-file"
	// for local coordinator to control the GPU usage of a DL job.
	AnnotationGpuConfigFile = AntManPrefix + "/gpu-config-file"
	// for local coordinator to access file
	LocalCoordinatorCommFolder = "/tmp/antman/"

	// Name is the name of the plugin used in Registry and configurations.
	Name = "Antman"
	// OpportunisticPod is the label name for antman to identify the type of a Pod.
	OpportunisticPod = "opportunistic"
)

type PodInfo struct {
	key     string
	podType string
}

type MyString string

func (ms *MyString) Clone() framework.StateData {
	c := *ms
	return &c
}

type GpuInfo struct {
	index    int
	usedUtil float64
	usedMem  float64
}

func GetPodTypeLabels(pod *v1.Pod) (string, error) {
	opportunistic, exist := pod.Labels[OpportunisticPod]
	if !exist || len(opportunistic) == 0 {
		klog.Infof("Pod %v/%v is a normal pod", pod.Namespace, pod.Name)
		return "", nil
	}
	klog.Infof("Pod %v%v is a pod with opportunistic label set as: %v", pod.Namespace, opportunistic)
	return opportunistic, nil
}
