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

package resmanager

import (
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/pointer"

	apiext "github.com/koordinator-sh/koordinator/apis/extension"
	slov1alpha1 "github.com/koordinator-sh/koordinator/apis/slo/v1alpha1"
	"github.com/koordinator-sh/koordinator/pkg/features"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/executor"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/metriccache"
	"github.com/koordinator-sh/koordinator/pkg/util"
)

const (
	beCPUSatisfactionLowPercentMax   = 60
	beCPUSatisfactionUpperPercentMax = 100
	beCPUUsageThresholdPercent       = 90
)

type CPUEvictor struct {
	resmanager    *resmanager
	lastEvictTime time.Time
}

func NewCPUEvictor(resmanager *resmanager) *CPUEvictor {
	return &CPUEvictor{
		resmanager:    resmanager,
		lastEvictTime: time.Now(),
	}
}

type podEvictCPUInfo struct {
	milliRequest   int64
	milliUsedCores int64
	cpuUsage       float64 // cpuUsage = milliUsedCores / milliRequest
	pod            *corev1.Pod
}

func (c *CPUEvictor) cpuEvict() {
	klog.V(5).Infof("cpu evict process start")

	nodeSLO := c.resmanager.getNodeSLOCopy()
	if disabled, err := isFeatureDisabled(nodeSLO, features.BECPUEvict); err != nil {
		klog.Warningf("cpuEvict failed, cannot check the feature gate, err: %s", err)
		return
	} else if disabled {
		klog.Warningf("cpuEvict skipped, nodeSLO disable the feature gate")
		return
	}

	if time.Since(c.lastEvictTime) < time.Duration(c.resmanager.config.CPUEvictCoolTimeSeconds)*time.Second {
		klog.Warningf("skip CPU evict process, still in evict cool time")
		return
	}
   // be类型的pod的使用阈值
	thresholdConfig := nodeSLO.Spec.ResourceUsedThresholdWithBE
	windowSeconds := c.resmanager.collectResUsedIntervalSeconds * 2
	if thresholdConfig.CPUEvictTimeWindowSeconds != nil && *thresholdConfig.CPUEvictTimeWindowSeconds > c.resmanager.collectResUsedIntervalSeconds {
		windowSeconds = *thresholdConfig.CPUEvictTimeWindowSeconds
	}

	node := c.resmanager.statesInformer.GetNode()
	if node == nil {
		klog.Warningf("cpuEvict failed, got nil node %s", c.resmanager.nodeName)
		return
	}

	cpuCapacity := node.Status.Capacity.Cpu().Value()
	if cpuCapacity <= 0 {
		klog.Warningf("cpuEvict failed, node cpuCapacity not valid,value: %d", cpuCapacity)
		return
	}

	c.evictByResourceSatisfaction(node, thresholdConfig, windowSeconds)
	klog.V(5).Info("cpu evict process finished.")
}

// calculateMilliRelease 计算需要释放的CPU资源量（以毫核为单位）。
//
// 参数:
// - thresholdConfig: 包含CPU驱逐策略的配置，例如CPU使用率阈值。
// - windowSeconds: 用于计算平均CPU使用率的时间窗口（秒）。
//
// 返回:
// - *metriccache.BECPUResourceMetric: 当前的BECPU资源指标，如果计算有效则返回，否则为nil。
// - int64: 需要释放的CPU资源量（毫核），如果不需要释放则为0。
func (c *CPUEvictor) calculateMilliRelease(thresholdConfig *slov1alpha1.ResourceThresholdStrategy, windowSeconds int64) (*metriccache.BECPUResourceMetric, int64) {
	// 步骤1: 根据指定时间窗口内的BECPUResourceMetric计算释放的资源量
	avgBECPUQueryResult := c.resmanager.metricCache.GetBECPUResourceMetric(generateQueryParamsAvg(windowSeconds))
	if !isAvgQueryResultValid(avgBECPUQueryResult, windowSeconds, c.resmanager.collectResUsedIntervalSeconds) {
		return nil, 0
	}

	// 检查平均CPU使用率是否足够高，不足以触发驱逐
	if !isBECPUUsageHighEnough(avgBECPUQueryResult.Metric, thresholdConfig.CPUEvictBEUsageThresholdPercent) {
		klog.Warningf("cpuEvict by ResourceSatisfaction skipped,avg cpuUsage not Enough! metric: %+v", avgBECPUQueryResult.Metric)
		return nil, 0
	}

	// 计算基于平均CPU使用率的释放资源量
	milliRelease := calculateResourceMilliToRelease(avgBECPUQueryResult.Metric, thresholdConfig)
	if milliRelease <= 0 {
		klog.Warningf("cpuEvict by ResourceSatisfaction skipped,releaseByAvg: %d", milliRelease)
		return nil, 0
	}

	// 步骤2: 计算当前时间点的释放资源量
	currentBECPUQueryResult := c.resmanager.metricCache.GetBECPUResourceMetric(generateQueryParamsLast(c.resmanager.collectResUsedIntervalSeconds * 2))
	if !isQueryResultValid(currentBECPUQueryResult) {
		return nil, 0
	}

	// 检查当前CPU使用率是否足够高，不足以触发驱逐
	if !isBECPUUsageHighEnough(currentBECPUQueryResult.Metric, thresholdConfig.CPUEvictBEUsageThresholdPercent) {
		klog.Warningf("cpuEvict by ResourceSatisfaction skipped,current cpuUsage not Enough! metric: %+v", currentBECPUQueryResult.Metric)
		return nil, 0
	}

	// 计算基于当前CPU使用率的释放资源量
	milliReleaseByCurrent := calculateResourceMilliToRelease(currentBECPUQueryResult.Metric, thresholdConfig)
	if milliReleaseByCurrent <= 0 {
		klog.Warningf("cpuEvict by ResourceSatisfaction skipped,releaseByCurrent: %d", milliReleaseByCurrent)
		return nil, 0
	}

	// 步骤3: 释放资源量取平均和当前计算结果中的较小值
	if milliReleaseByCurrent < milliRelease {
		milliRelease = milliReleaseByCurrent
	}
	return currentBECPUQueryResult.Metric, milliRelease
}


// evictByResourceSatisfaction 根据资源满足度条件执行驱逐操作。
// 此函数检查给定节点上的资源满足度配置是否有效，并根据配置计算出需要释放的CPU资源量。
// 如果计算出需要释放CPU资源，则按照特定策略排序选择要驱逐的Pod，并执行驱逐操作。
//
// 参数:
// - node: 指定要进行驱逐操作的节点。
// - thresholdConfig: 资源阈值策略配置，包含触发驱逐的资源满足度条件。
// - windowSeconds: 用于计算资源使用率的时间窗口长度（秒）。
//
// 无返回值。
func (c *CPUEvictor) evictByResourceSatisfaction(node *corev1.Node, thresholdConfig *slov1alpha1.ResourceThresholdStrategy, windowSeconds int64) {
	// 验证资源满足度配置是否有效，无效则直接返回。
	if !isSatisfactionConfigValid(thresholdConfig) {
		return
	}

	// 根据配置和时间窗口计算当前需要释放的CPU资源量（毫核）。
	currentBECPU, milliRelease := c.calculateMilliRelease(thresholdConfig, windowSeconds)
	if milliRelease > 0 {
		// 获取并排序满足条件的Pod信息，准备进行驱逐。
		bePodInfos := c.getPodEvictInfoAndSort(currentBECPU)
		// 执行驱逐操作，释放计算出的CPU资源量。
		c.killAndEvictBEPodsRelease(node, bePodInfos, milliRelease)
	}
}


func (c *CPUEvictor) killAndEvictBEPodsRelease(node *corev1.Node, bePodInfos []*podEvictCPUInfo, cpuNeedMilliRelease int64) {
	message := fmt.Sprintf("killAndEvictBEPodsRelease for node(%s), need realase CPU : %d", c.resmanager.nodeName, cpuNeedMilliRelease)

	cpuMilliReleased := int64(0)
	var killedPods []*corev1.Pod
	for _, bePod := range bePodInfos {
		if cpuMilliReleased >= cpuNeedMilliRelease {
			break
		}

		podKillMsg := fmt.Sprintf("%s, kill pod : %s", message, bePod.pod.Name)
		killContainers(bePod.pod, podKillMsg)

		killedPods = append(killedPods, bePod.pod)
		cpuMilliReleased = cpuMilliReleased + bePod.milliRequest
	}

	c.resmanager.evictPodsIfNotEvicted(killedPods, node, executor.EvictPodByBECPUSatisfaction, message)

	if len(killedPods) > 0 {
		c.lastEvictTime = time.Now()
	}
	klog.V(5).Infof("killAndEvictBEPodsRelease finished!cpuNeedMilliRelease(%d) cpuMilliReleased(%d)", cpuNeedMilliRelease, cpuMilliReleased)
}

func (c *CPUEvictor) getPodEvictInfoAndSort(beMetric *metriccache.BECPUResourceMetric) []*podEvictCPUInfo {
	var bePodInfos []*podEvictCPUInfo

	for _, podMeta := range c.resmanager.statesInformer.GetAllPods() {
		pod := podMeta.Pod
		if apiext.GetPodQoSClass(pod) == apiext.QoSBE {

			bePodInfo := &podEvictCPUInfo{pod: podMeta.Pod}
			podQueryResult := c.resmanager.collectPodMetric(podMeta, generateQueryParamsLast(c.resmanager.collectResUsedIntervalSeconds*2))
			podMetric := podQueryResult.Metric
			if podQueryResult.Error == nil && podMetric != nil {
				bePodInfo.milliUsedCores = podMetric.CPUUsed.CPUUsed.MilliValue()
			}

			milliRequestSum := int64(0)
			for _, container := range pod.Spec.Containers {
				containerCPUReq := util.GetContainerBatchMilliCPURequest(&container)
				if containerCPUReq > 0 {
					milliRequestSum = milliRequestSum + containerCPUReq
				}
			}

			bePodInfo.milliRequest = milliRequestSum
			if bePodInfo.milliRequest > 0 {
				bePodInfo.cpuUsage = float64(bePodInfo.milliUsedCores) / float64(bePodInfo.milliRequest)
			}

			bePodInfos = append(bePodInfos, bePodInfo)
		}
	}

	sort.Slice(bePodInfos, func(i, j int) bool {
		if bePodInfos[i].pod.Spec.Priority == nil || bePodInfos[j].pod.Spec.Priority == nil ||
			*bePodInfos[i].pod.Spec.Priority == *bePodInfos[j].pod.Spec.Priority {
			return bePodInfos[i].cpuUsage > bePodInfos[j].cpuUsage
		}
		return *bePodInfos[i].pod.Spec.Priority < *bePodInfos[j].pod.Spec.Priority
	})
	return bePodInfos
}

func calculateResourceMilliToRelease(metric *metriccache.BECPUResourceMetric, thresholdConfig *slov1alpha1.ResourceThresholdStrategy) int64 {
	if metric.CPURequest.IsZero() {
		klog.Warningf("cpuEvict by ResourceSatisfaction skipped! be pods requests is zero!")
		return 0
	}

	satisfactionRate := float64(metric.CPURealLimit.MilliValue()) / float64(metric.CPURequest.MilliValue())
	if satisfactionRate > float64(*thresholdConfig.CPUEvictBESatisfactionLowerPercent)/100 {
		klog.Warningf("cpuEvict by ResourceSatisfaction skipped! satisfactionRate(%.2f) and lowPercent(%f)", satisfactionRate, float64(*thresholdConfig.CPUEvictBESatisfactionLowerPercent))
		return 0
	}

	rateGap := float64(*thresholdConfig.CPUEvictBESatisfactionUpperPercent)/100 - satisfactionRate
	if rateGap <= 0 {
		klog.Warningf("cpuEvict by ResourceSatisfaction skipped! satisfactionRate(%.2f) > upperPercent(%f)", satisfactionRate, float64(*thresholdConfig.CPUEvictBESatisfactionUpperPercent))
		return 0
	}

	milliRelease := float64(metric.CPURequest.MilliValue()) * rateGap
	return int64(milliRelease)
}

func isBECPUUsageHighEnough(metric *metriccache.BECPUResourceMetric, thresholdPercent *int64) bool {
	if metric.CPURealLimit.IsZero() {
		klog.Warningf("cpuEvict by ResourceSatisfaction skipped! CPURealLimit is zero!")
		return false
	}
	if metric.CPURealLimit.MilliValue() < 1000 {
		return true
	}
	cpuUsage := float64(metric.CPUUsed.MilliValue()) / float64(metric.CPURealLimit.MilliValue())
	if thresholdPercent == nil {
		thresholdPercent = pointer.Int64(beCPUUsageThresholdPercent)
	}
	if cpuUsage < float64(*thresholdPercent)/100 {
		klog.Warningf("cpuEvict by ResourceSatisfaction skipped! cpuUsage(%.2f) and thresholdPercent %d!", cpuUsage, *thresholdPercent)
		return false
	}
	return true
}

func isAvgQueryResultValid(avgQueryResult metriccache.BECPUResourceQueryResult, windowSeconds, collectIntervalSeconds int64) bool {
	if !isQueryResultValid(avgQueryResult) {
		return false
	}
	if avgQueryResult.AggregateInfo == nil {
		klog.Warningf("cpuEvict by ResourceSatisfaction skipped, AggregateInfo is nil!windowSize: %v, collectInterval: %v", windowSeconds, collectIntervalSeconds)
		return false
	}
	if avgQueryResult.AggregateInfo.MetricsCount*collectIntervalSeconds < windowSeconds/3 {
		klog.Warningf("cpuEvict by ResourceSatisfaction skipped, metricsCount(%d) not enough!windowSize: %v, collectInterval: %v", avgQueryResult.AggregateInfo.MetricsCount, windowSeconds, collectIntervalSeconds)
		return false
	}
	return true
}

func isQueryResultValid(queryResult metriccache.BECPUResourceQueryResult) bool {
	if queryResult.Error != nil {
		klog.Warningf("cpuEvict by ResourceSatisfaction skipped,queryResult error: %v", queryResult.Error)
		return false
	}
	if queryResult.Metric == nil {
		klog.Warningf("cpuEvict by ResourceSatisfaction skipped, queryResult metric(%+v) is nil!", queryResult.Metric)
		return false
	}
	return true
}

func isSatisfactionConfigValid(thresholdConfig *slov1alpha1.ResourceThresholdStrategy) bool {
	lowPercent := thresholdConfig.CPUEvictBESatisfactionLowerPercent
	upperPercent := thresholdConfig.CPUEvictBESatisfactionUpperPercent
	if lowPercent == nil && upperPercent == nil {
		klog.Warningf("cpuEvict by ResourceSatisfaction skipped, CPUEvictBESatisfactionLowerPercent and  CPUEvictBESatisfactionUpperPercent not config!")
		return false
	}
	if lowPercent == nil || *lowPercent > beCPUSatisfactionLowPercentMax || *lowPercent <= 0 {
		klog.Warningf("cpuEvict by ResourceSatisfaction skipped, CPUEvictBESatisfactionLowerPercent(%d) is not valid! must (0,%d]", lowPercent, beCPUSatisfactionLowPercentMax)
		return false
	}
	if upperPercent == nil || *upperPercent >= beCPUSatisfactionUpperPercentMax || *upperPercent <= 0 {
		klog.Warningf("cpuEvict by ResourceSatisfaction skipped, CPUEvictBESatisfactionUpperPercent(%d) is not valid,must (0,%d)!", upperPercent, beCPUSatisfactionUpperPercentMax)
		return false
	} else if *upperPercent < *lowPercent {
		klog.Infof("cpuEvict by ResourceSatisfaction skipped, CPUEvictBESatisfactionUpperPercent(%d) < CPUEvictBESatisfactionLowerPercent(%d)!", upperPercent, lowPercent)
		return false
	}
	return true
}
