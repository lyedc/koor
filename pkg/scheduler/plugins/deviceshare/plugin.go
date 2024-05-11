/*
Copyright 2022 The Koordinator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package deviceshare

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	quotav1 "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/v1/resource"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	apiext "github.com/koordinator-sh/koordinator/apis/extension"
	schedulingv1alpha1 "github.com/koordinator-sh/koordinator/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koordinator/pkg/scheduler/frameworkext"
	"github.com/koordinator-sh/koordinator/pkg/util"
)

const (
	// Name is the name of the plugin used in the plugin registry and configurations.
	Name = "DeviceShare"

	// stateKey is the key in CycleState to pre-computed data.
	stateKey = Name

	// ErrMissingDevice when node does not have Device.
	ErrMissingDevice = "node(s) missing Device"

	// ErrInsufficientDevices when node can't satisfy Pod's requested resource.
	ErrInsufficientDevices = "Insufficient Devices"
)

type Plugin struct {
	handle          framework.Handle
	nodeDeviceCache *nodeDeviceCache
}

var (
	_ framework.PreFilterPlugin = &Plugin{}
	_ framework.FilterPlugin    = &Plugin{}
	_ framework.ReservePlugin   = &Plugin{}
	_ framework.PreBindPlugin   = &Plugin{}
)

type preFilterState struct {
	skip                    bool
	allocationResult        apiext.DeviceAllocations
	convertedDeviceResource corev1.ResourceList
}

func (s *preFilterState) Clone() framework.StateData {
	return s
}

func (g *Plugin) Name() string {
	return Name
}

// PreFilter 在调度前调用，用于对 Pod 进行检查和转换。
// 检查 Pod 是否请求了特殊设备资源（如 GPU、RDMA 或 FPGA），并验证这些请求。
// 如果请求有效，将其转换为调度过程后续阶段可以理解的格式。
//
// 参数:
// - ctx: 用于取消和设置截止时间的上下文。
// - cycleState: 用于在整个调度周期中存储和检索数据的 CycleState 对象。
// - pod: 正在评估的 Pod。
//
// 返回:
// - 状态对象，表示成功或失败。如果失败，提供错误消息。
func (g *Plugin) PreFilter(ctx context.Context, cycleState *framework.CycleState, pod *corev1.Pod) *framework.Status {
    // 初始化预过滤器状态，设置默认值。
	state := &preFilterState{
		skip:                    true,
		convertedDeviceResource: make(corev1.ResourceList),
	}

    // 计算 Pod 请求的总资源。
	podRequest, _ := resource.PodRequestsAndLimits(pod)

    // 遍历已知设备资源类型，检查并处理每种类型。
	for deviceType := range DeviceResourceNames {
		switch deviceType {
		case schedulingv1alpha1.GPU:
			// 如果 Pod 请求 GPU，验证 GPU 请求。
			if !hasDeviceResource(podRequest, deviceType) {
				break
			}
			//  验证GPU的请求
			combination, err := ValidateGPURequest(podRequest)
			if err != nil {

				return framework.NewStatus(framework.Error, err.Error())
			}
			// 将 GPU 请求转换为 quotav1.ResourceList 格式。
			state.convertedDeviceResource = quotav1.Add(
				state.convertedDeviceResource,
				ConvertGPUResource(podRequest, combination),
			)
			// 标记不应跳过此 Pod，因为它具有有效的 GPU 请求。
			state.skip = false
		case schedulingv1alpha1.RDMA, schedulingv1alpha1.FPGA:
			// 如果 Pod 请求 RDMA 或 FPGA，验证通用设备请求。
			if !hasDeviceResource(podRequest, deviceType) {
				break
			}
			if err := validateCommonDeviceRequest(podRequest, deviceType); err != nil {
				// 如果通用设备请求无效，返回错误。
				return framework.NewStatus(framework.Error, err.Error())
			}
			// 将通用设备请求转换为 quotav1.ResourceList 格式。
			state.convertedDeviceResource = quotav1.Add(
				state.convertedDeviceResource,
				convertCommonDeviceResource(podRequest, deviceType),
			)
			// 标记不应跳过此 Pod，因为它具有有效的通用设备请求。
			state.skip = false
		default:
			// 如果遇到不支持的设备类型，记录警告。
			klog.Warningf("设备类型 %v 尚不受支持", deviceType)
		}
	}

    // 将预过滤器状态存储以备后续调度周期使用。
	cycleState.Write(stateKey, state)
	return nil
}


func (g *Plugin) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

func getPreFilterState(cycleState *framework.CycleState) (*preFilterState, *framework.Status) {
	value, err := cycleState.Read(stateKey)
	if err != nil {
		return nil, framework.AsStatus(err)
	}
	state := value.(*preFilterState)
	return state, nil
}

func (g *Plugin) Filter(ctx context.Context, cycleState *framework.CycleState, pod *corev1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	state, status := getPreFilterState(cycleState)
	if !status.IsSuccess() {
		return status
	}
	if state.skip {
		return nil
	}

	if nodeInfo.Node() == nil {
		return framework.NewStatus(framework.Error, "node not found")
	}
    // 获取node上面gpu的资源信息，是从缓存中获取的，缓存中获取的是通过 registerDeviceEventHandler 这个informer获取的
	nodeDeviceInfo := g.nodeDeviceCache.getNodeDevice(nodeInfo.Node().Name)
	if nodeDeviceInfo == nil {
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, ErrMissingDevice)
	}

	podRequest := state.convertedDeviceResource

	nodeDeviceInfo.lock.RLock()
	defer nodeDeviceInfo.lock.RUnlock()
    // 这里在调度器层面就可以直接通过缓存中的device信息，判断是否可以分配device设备给pod，不用像device plugin那样去调用plugin的接口才能知道是否能被分配资源。
	allocateResult, err := nodeDeviceInfo.tryAllocateDevice(podRequest)
	if len(allocateResult) != 0 && err == nil {
		return nil
	}

	return framework.NewStatus(framework.Unschedulable, ErrInsufficientDevices)
}

func (g *Plugin) Reserve(ctx context.Context, cycleState *framework.CycleState, pod *corev1.Pod, nodeName string) *framework.Status {
	state, status := getPreFilterState(cycleState)
	if !status.IsSuccess() {
		return status
	}
	if state.skip {
		return nil
	}

	nodeDeviceInfo := g.nodeDeviceCache.getNodeDevice(nodeName)
	if nodeDeviceInfo == nil {
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, ErrMissingDevice)
	}

	podRequest := state.convertedDeviceResource

	nodeDeviceInfo.lock.Lock()
	defer nodeDeviceInfo.lock.Unlock()

	allocateResult, err := nodeDeviceInfo.tryAllocateDevice(podRequest)
	if err != nil || len(allocateResult) == 0 {
		return framework.NewStatus(framework.Unschedulable, ErrInsufficientDevices)
	}
	// 核减pod使用的gpu的资源的使用。
	nodeDeviceInfo.updateCacheUsed(allocateResult, pod, true)

	state.allocationResult = allocateResult
	return nil
}

func (g *Plugin) Unreserve(ctx context.Context, cycleState *framework.CycleState, pod *corev1.Pod, nodeName string) {
	state, status := getPreFilterState(cycleState)
	if !status.IsSuccess() {
		return
	}
	if state.skip {
		return
	}

	nodeDeviceInfo := g.nodeDeviceCache.getNodeDevice(nodeName)
	if nodeDeviceInfo == nil {
		return
	}

	nodeDeviceInfo.lock.Lock()
	defer nodeDeviceInfo.lock.Unlock()

	nodeDeviceInfo.updateCacheUsed(state.allocationResult, pod, false)

	state.allocationResult = nil
}

func (g *Plugin) PreBind(ctx context.Context, cycleState *framework.CycleState, pod *corev1.Pod, nodeName string) *framework.Status {
	state, status := getPreFilterState(cycleState)
	if !status.IsSuccess() {
		return status
	}
	if state.skip {
		return nil
	}

	allocResult := state.allocationResult
	newPod := pod.DeepCopy()
	// 把gpu信息，注入到annotation中
	if err := apiext.SetDeviceAllocations(newPod, allocResult); err != nil {
		return framework.NewStatus(framework.Error, err.Error())
	}

	// NOTE: APIServer won't allow the following modification. Error: pod updates may not change fields other than
	// `spec.containers[*].image`, `spec.initContainers[*].image`, `spec.activeDeadlineSeconds`,
	// `spec.tolerations` (only additions to existing tolerations) or `spec.terminationGracePeriodSeconds`

	// podRequest := state.convertedDeviceResource
	// if _, ok := allocResult[schedulingv1alpha1.GPU]; ok {
	// 	patchContainerGPUResource(newPod, podRequest)
	// }
   // 合并pod的path，并更新pod的资源情况。
	patchBytes, err := util.GeneratePodPatch(pod, newPod)
	if err != nil {
		return framework.NewStatus(framework.Error, err.Error())
	}
	err = util.RetryOnConflictOrTooManyRequests(func() error {
		_, podErr := g.handle.ClientSet().CoreV1().Pods(pod.Namespace).
			Patch(ctx, pod.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
		return podErr
	})
	if err != nil {
		return framework.NewStatus(framework.Error, err.Error())
	}

	return nil
}

func (g *Plugin) getNodeDeviceSummary(nodeName string) (*NodeDeviceSummary, bool) {
	return g.nodeDeviceCache.getNodeDeviceSummary(nodeName)
}

func (g *Plugin) getAllNodeDeviceSummary() map[string]*NodeDeviceSummary {
	return g.nodeDeviceCache.getAllNodeDeviceSummary()
}

func New(args runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	extendedHandle, ok := handle.(frameworkext.ExtendedHandle)
	if !ok {
		return nil, fmt.Errorf("expect handle to be type frameworkext.ExtendedHandle, got %T", handle)
	}
    // 注册事件方法
	deviceCache := newNodeDeviceCache()
	// 注册device的事件方法
	registerDeviceEventHandler(deviceCache, extendedHandle.KoordinatorSharedInformerFactory())
	// 注册pod的事件方法。
	registerPodEventHandler(deviceCache, handle.SharedInformerFactory())

	return &Plugin{
		handle:          handle,
		nodeDeviceCache: deviceCache,
	}, nil
}
