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

package elasticquota

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	quotav1 "k8s.io/apiserver/pkg/quota/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/v1/resource"
	"k8s.io/kubernetes/pkg/scheduler/util"

	"github.com/koordinator-sh/koordinator/apis/extension"
	"github.com/koordinator-sh/koordinator/pkg/scheduler/plugins/elasticquota/core"
)

const (
	QuotaOverUsedRevokeControllerName = "QuotaOverUsedRevokeController"
)

type QuotaOverUsedGroupMonitor struct {
	groupQuotaManger             *core.GroupQuotaManager
	quotaName                    string
	lastUnderUsedTime            time.Time
	overUsedTriggerEvictDuration time.Duration
}

func NewQuotaOverUsedGroupMonitor(quotaName string, manager *core.GroupQuotaManager, overUsedTriggerEvictDuration time.Duration) *QuotaOverUsedGroupMonitor {
	return &QuotaOverUsedGroupMonitor{
		quotaName:                    quotaName,
		groupQuotaManger:             manager,
		overUsedTriggerEvictDuration: overUsedTriggerEvictDuration,
		lastUnderUsedTime:            time.Now(),
	}
}
// monitor检查配额使用情况，如果当前配额使用量超过了其运行时配额量，
// 并且这种状态持续时间超过了预设的触发驱逐的阈值，则返回true，表示需要触发驱逐机制。
func (monitor *QuotaOverUsedGroupMonitor) monitor() bool {
	quotaInfo := monitor.groupQuotaManger.GetQuotaInfoByName(monitor.quotaName)
	if quotaInfo == nil {
		return false
	}

	runtime := quotaInfo.GetRuntime()
	used := quotaInfo.GetUsed()
	// 检查当前使用量是否小于等于运行时配额量，并获取超出的维度
	isLessEqual, exceedDimensions := quotav1.LessThanOrEqual(used, runtime)

	var overUseContinueDuration time.Duration
	if !isLessEqual {
		// 如果当前使用量大于运行时配额量，则计算持续过度使用的时间
		overUseContinueDuration = time.Since(monitor.lastUnderUsedTime)
		klog.V(5).Infof("Quota used is large than runtime, quotaName:%v, resDimensions:%v, used:%v, "+
			"runtime:%v", monitor.quotaName, exceedDimensions, used, runtime)
	} else {
		monitor.lastUnderUsedTime = time.Now()
	}
	//如果持续过度使用的时长超过了触发驱逐的阈值
	if overUseContinueDuration > monitor.overUsedTriggerEvictDuration {
		klog.V(5).Infof("Quota used continue large than runtime, prepare trigger evict, quotaName:%v,"+
			"overUseContinueDuration:%v, config:%v", monitor.quotaName, overUseContinueDuration,
			monitor.overUsedTriggerEvictDuration)
		monitor.lastUnderUsedTime = time.Now()
		return true
	}
	return false
}

// 这里会多次的尝试回收pod
func (monitor *QuotaOverUsedGroupMonitor) getToRevokePodList(quotaName string) []*v1.Pod {
	quotaInfo := monitor.groupQuotaManger.GetQuotaInfoByName(quotaName)
	if quotaInfo == nil {
		return nil
	}

	runtime := quotaInfo.GetRuntime()
	used := quotaInfo.GetUsed()
	oriUsed := used.DeepCopy()

	// order pod from low priority -> high priority
	// 获取所有已经被调度的pod这个是存在quota的cache中的。
	priPodCache := quotaInfo.GetPodThatIsAssigned()
    // 按照优先级排序，没有优先级的按照启动时间
	sort.Slice(priPodCache, func(i, j int) bool { return !util.MoreImportantPod(priPodCache[i], priPodCache[j]) })

	// first try revoke all until used <= runtime
	// 尝试回收Pod，直到配额使用量不超过运行时限制
	tryAssignBackPodCache := make([]*v1.Pod, 0)

	for _, pod := range priPodCache {
		// 判断runtime和userd的谁打，如果runtime的大，那么久表示还有资源可以分配，跳过。
		// 如果使用量已经不超过运行时限制，则停止回收过程
		if shouldBreak, _ := quotav1.LessThanOrEqual(used, runtime); shouldBreak {
			break
		}
		// 如果已经超过了runtime，那么就加入到要回收的pod中。
		podReq, _ := resource.PodRequestsAndLimits(pod)
		// 从use中减去要回收的pod的资源。然后继续循环，直到used的资源小于runtime。
		used = quotav1.Subtract(used, podReq)
		tryAssignBackPodCache = append(tryAssignBackPodCache, pod)
	}

	// means should evict all
	// 遍历一遍后如果还是超过runtime，那么就返回要回收的pod。
	// 如果遍历完所有Pod后，使用量仍超过运行时限制，则准备回收所有超过部分的Pod
	if lessThanOrEqual, _ := quotav1.LessThanOrEqual(used, runtime); !lessThanOrEqual {
		for _, pod := range tryAssignBackPodCache {
			klog.Infof("pod should be revoked by QuotaOverUsedMonitor, pod:%v, quotaName:%v"+
				"used:%v, runtime:%v", pod.Name, quotaName, oriUsed, runtime)
		}
		return tryAssignBackPodCache
	}

	//try assign back from high->low
	//从高优先级向低优先级尝试将资源分配回已回收的Pod，直到再次超过运行时限制
	realRevokePodCache := make([]*v1.Pod, 0)
	for index := len(tryAssignBackPodCache) - 1; index >= 0; index-- {
		pod := tryAssignBackPodCache[index]
		podRequest, _ := resource.PodRequestsAndLimits(pod)
		used = quotav1.Add(used, podRequest)
		if canAssignBack, _ := quotav1.LessThanOrEqual(used, runtime); !canAssignBack {
			used = quotav1.Subtract(used, podRequest)
			realRevokePodCache = append(realRevokePodCache, pod)
		}
	}
	for _, pod := range realRevokePodCache {
		klog.Infof("pod should be evict by QuotaOverUseGroupMonitor, pod:%v, quotaName:%v,"+
			"used:%v, runtime:%v", pod.Name, quotaName, oriUsed, runtime)
	}
	return realRevokePodCache
}

type QuotaOverUsedRevokeController struct {
	clientSet                    clientset.Interface
	groupQuotaManger             *core.GroupQuotaManager
	monitorsLock                 sync.RWMutex
	monitors                     map[string]*QuotaOverUsedGroupMonitor
	overUsedTriggerEvictDuration time.Duration
	revokePodCycle               time.Duration
	monitorAllQuotas             bool
}

func NewQuotaOverUsedRevokeController(client clientset.Interface, overUsedTriggerEvictDuration, revokePodCycle time.Duration,
	groupQuotaManager *core.GroupQuotaManager, monitorAllQuotas bool) *QuotaOverUsedRevokeController {
	controller := &QuotaOverUsedRevokeController{
		clientSet:                    client,
		groupQuotaManger:             groupQuotaManager,
		overUsedTriggerEvictDuration: overUsedTriggerEvictDuration,
		revokePodCycle:               revokePodCycle,
		monitors:                     make(map[string]*QuotaOverUsedGroupMonitor),
		monitorAllQuotas:             monitorAllQuotas,
	}
	return controller
}

func (controller *QuotaOverUsedRevokeController) Name() string {
	return QuotaOverUsedRevokeControllerName
}

func (controller *QuotaOverUsedRevokeController) Start() {
	go wait.Until(controller.revokePodDueToQuotaOverUsed, controller.revokePodCycle, nil)
	klog.Infof("start elasticQuota QuotaOverUsedRevokeController")
}

// 撤销过度使用资源的pod，也就是收quota中的资源被使用超了。需要删除哪些低优先级的pod，来平衡quota的资源使用情况。
func (controller *QuotaOverUsedRevokeController) revokePodDueToQuotaOverUsed() {
	// 返回要回收的pod
	toRevokePods := controller.monitorAll()
	// 对回收的pod进行驱逐。
	for _, pod := range toRevokePods {
		if err := EvictPod(context.TODO(), controller.clientSet, pod, &metav1.DeleteOptions{}); err != nil {
			klog.Errorf("failed to revoke pod due to quota overused, pod:%v, error:%s",
				pod.Name, err)
			continue
		}
		klog.V(5).Infof("finish revoke pod due to quota overused, pod:%v",
			pod.Name)
	}
}

func (controller *QuotaOverUsedRevokeController) monitorAll() []*v1.Pod {
	// 同步所有的quota信息，为没有加入监控的quota加入监控,也就是加入一个monitor。
	controller.syncQuota()
    // 获取监控的quota
	monitors := controller.getToMonitorQuotas()

	toRevokePods := make([]*v1.Pod, 0)
	for quotaName, monitor := range monitors {
		toRevokePodsTmp := monitor.getToRevokePodList(quotaName)
		toRevokePods = append(toRevokePods, toRevokePodsTmp...)
	}
	return toRevokePods
}

func (controller *QuotaOverUsedRevokeController) syncQuota() {
	controller.monitorsLock.Lock()
	defer controller.monitorsLock.Unlock()

	allQuotaNames := controller.groupQuotaManger.GetAllQuotaNames()

	for quotaName := range allQuotaNames {
		if quotaName == extension.SystemQuotaName || quotaName == extension.RootQuotaName {
			continue
		}
       // 增加一个monitor来监控 quota的使用情况。
		if controller.monitors[quotaName] == nil {
			controller.addQuota(quotaName)
		}
	}
    // 移除已经删除的quota的monitor。
	for quotaName := range controller.monitors {
		if _, exist := allQuotaNames[quotaName]; !exist {
			controller.deleteQuota(quotaName)
		}
	}
}

func (controller *QuotaOverUsedRevokeController) addQuota(quotaName string) {
	controller.monitors[quotaName] = NewQuotaOverUsedGroupMonitor(quotaName, controller.groupQuotaManger, controller.overUsedTriggerEvictDuration)
	klog.V(5).Infof("QuotaOverUseRescheduleController add quota:%v", quotaName)
}

func (controller *QuotaOverUsedRevokeController) deleteQuota(quotaName string) {
	delete(controller.monitors, quotaName)
	klog.V(5).Infof("QuotaOverUseRescheduleController delete quota:%v", quotaName)
}

func (controller *QuotaOverUsedRevokeController) getToMonitorQuotas() map[string]*QuotaOverUsedGroupMonitor {
	if !controller.monitorAllQuotas {
		return nil
	}
	monitors := make(map[string]*QuotaOverUsedGroupMonitor)

	{
		controller.monitorsLock.RLock()
		for key, value := range controller.monitors {
			monitors[key] = value
		}
		controller.monitorsLock.RUnlock()
	}

	result := make(map[string]*QuotaOverUsedGroupMonitor)

	for quotaName, monitor := range monitors {
		shouldTriggerEvict := monitor.monitor()
		if shouldTriggerEvict {
			result[quotaName] = monitor
		}
	}
	return result
}

func EvictPod(ctx context.Context, client clientset.Interface, pod *v1.Pod, deleteOptions *metav1.DeleteOptions) error {
	eviction := &policy.Eviction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
		},
		DeleteOptions: deleteOptions,
	}
	err := client.PolicyV1beta1().Evictions(eviction.Namespace).Evict(ctx, eviction)
	if apierrors.IsTooManyRequests(err) {
		return fmt.Errorf("error when evicting pod (ignoring) %q: %v", pod.Name, err)
	}
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("pod not found when evicting %q: %v", pod.Name, err)
	}
	return err
}
