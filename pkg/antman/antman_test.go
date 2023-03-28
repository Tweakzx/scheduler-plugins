package antman

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/kubernetes/pkg/scheduler/listers"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

// FakeNew is used for test.
func FakeNew(clock util.Clock, stop chan struct{}) (*Antman, error) {
	am := &Antman{
		clock: clock,
	}
	// go wait.Until(am.podGroupInfoGC, time.Duration(am.args.PodGroupGCIntervalSeconds)*time.Second, stop)
	return am, nil
}

func TestLess(t *testing.T) {

}

func TestPreFilter(t *testing.T) {

}

func TestPermit(t *testing.T) {

}

func TestPodGroupClean(t *testing.T) {

}

var _ listers.SharedLister = &fakeSharedLister{}

type fakeSharedLister struct {
	pods []*v1.Pod
}

func (f *fakeSharedLister) Pods() listers.PodLister {
	return f
}

func (f *fakeSharedLister) List(_ labels.Selector) ([]*v1.Pod, error) {
	return f.pods, nil
}

func (f *fakeSharedLister) FilteredList(podFilter listers.PodFilter, selector labels.Selector) ([]*v1.Pod, error) {
	pods := make([]*v1.Pod, 0, len(f.pods))
	for _, pod := range f.pods {
		if podFilter(pod) && selector.Matches(labels.Set(pod.Labels)) {
			pods = append(pods, pod)
		}
	}
	return pods, nil
}

func (f *fakeSharedLister) NodeInfos() listers.NodeInfoLister {
	return nil
}
