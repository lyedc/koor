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

package metricsadvisor

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"

	apiext "github.com/koordinator-sh/koordinator/apis/extension"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/metriccache"
	koordletutil "github.com/koordinator-sh/koordinator/pkg/koordlet/util"
	"github.com/koordinator-sh/koordinator/pkg/util"
)

func (c *collector) collectBECPUResourceMetric() {
	klog.V(6).Info("collectBECPUResourceMetric start")
	// 收集的指标： cpuset.cpus  cpu.cfs_quota_us和cpu.cfs_period_us 这三个指标
	realMilliLimit, err := getBECPURealMilliLimit()
	if err != nil {
		klog.Errorf("getBECPURealMilliLimit failed, error: %v", err)
		return
	}
	// 获取 pod中 QoSBE     QoSClass = "BE"  类型的所有的pod的request的值。
	beCPURequest := c.getBECPURequestSum()
	// 获取be类型的pod使用cpu的毫核数据。
	// 通过： cpuacct.usage进行计算，计算的是besteffort类型的pod的cpu使用率。
	beCPUUsageCores, err := c.getBECPUUsageCores()
	if err != nil {
		klog.Errorf("getBECPUUsageCores failed, error: %v", err)
		return
	}

	if beCPUUsageCores == nil {
		klog.Info("beCPUUsageCores is nil")
		return
	}

	beCPUMetric := metriccache.BECPUResourceMetric{
		CPUUsed:      *beCPUUsageCores,
		CPURealLimit: *resource.NewMilliQuantity(int64(realMilliLimit), resource.DecimalSI),
		CPURequest:   beCPURequest,
	}

	collectTime := time.Now()
	err = c.metricCache.InsertBECPUResourceMetric(collectTime, &beCPUMetric)
	if err != nil {
		klog.Errorf("InsertBECPUResourceMetric failed, error: %v", err)
		return
	}
	klog.V(6).Info("collectBECPUResourceMetric finished")
}

func getBECPURealMilliLimit() (int, error) {
	limit := 0
	// 获取best-effort类型的pod的 cpu limit
	// 读取的cpugroup 中的cpuset.cpus的指标，表示的是程序能访问的cpu的，相当于是cpu的绑定关系了。
	// 限定了服务能使用的cpu的核心，
	cpuSet, err := koordletutil.GetBECgroupCurCPUSet()
	if err != nil {
		return 0, err
	}
	// cpuset: [0,1,5,6,7]
	// limit: 5*1000 表示5个核心？？？
	limit = len(cpuSet) * 1000
	// cpu-quota设置配额
	/*
	    读取的cpugroup 中的cpu.cfs_quota_us和cpu.cfs_period_us的指标，表示的是cpu的配额和周期，
	   cpu-quota/cpu-period为实际分配的CPU量
		这个商是小数就表示分配的CPU量不足一个vCPU，
		如果商大于1就表示分配的CPU量超过一个vCPU
	*/
	cfsQuota, err := koordletutil.GetRootCgroupCurCFSQuota(corev1.PodQOSBestEffort)
	if err != nil {
		return 0, err
	}

	// -1 means not suppress by cfs_quota
	if cfsQuota == -1 {
		return limit, nil
	}
	// cpu-period设置是一个评估周期，区间在1ms~1s之间
	cfsPeriod, err := koordletutil.GetRootCgroupCurCFSPeriod(corev1.PodQOSBestEffort)
	if err != nil {
		return 0, err
	}

	limitByCfsQuota := int(cfsQuota * 1000 / cfsPeriod)

	if limitByCfsQuota < limit {
		limit = limitByCfsQuota
	}

	return limit, nil
}

func (c *collector) getBECPURequestSum() resource.Quantity {
	requestSum := int64(0)
	for _, podMeta := range c.statesInformer.GetAllPods() {
		pod := podMeta.Pod
		if apiext.GetPodQoSClass(pod) == apiext.QoSBE {
			podCPUReq := util.GetPodBEMilliCPURequest(pod)
			if podCPUReq > 0 {
				requestSum += podCPUReq
			}
		}
	}
	return *resource.NewMilliQuantity(requestSum, resource.DecimalSI)
}

func (c *collector) getBECPUUsageCores() (*resource.Quantity, error) {
	klog.V(6).Info("getBECPUUsageCores start")

	collectTime := time.Now()
	// 读取这个： cpuacct.usage 83898673587975：
	// 记录合格cgroup中所有的进程(包含子进程）消耗的总cpu时间(纳秒)
	currentCPUUsage, err := koordletutil.GetRootCgroupCPUUsageNanoseconds(corev1.PodQOSBestEffort)
	if err != nil {
		klog.Warningf("failed to collect be cgroup usage, error: %v", err)
		return nil, err
	}

	lastCPUStat := c.context.lastBECPUStat
	c.context.lastBECPUStat = contextRecord{
		cpuUsage: currentCPUUsage,
		ts:       collectTime,
	}

	if lastCPUStat.cpuUsage <= 0 {
		klog.V(6).Infof("ignore the first cpu stat collection")
		return nil, nil
	}

	// NOTICE: do subtraction and division first to avoid overflow
	// 计算cpu的使用之值
	/*
	计算当前CPU使用率与上一次统计时的CPU使用率之差，然后除以当前统计时间与上一次统计时间之差，得到CPU使用率的值。
	将CPU使用率的值乘以1000，再转换为int64类型，得到毫核数
	*/
	cpuUsageValue := float64(currentCPUUsage-lastCPUStat.cpuUsage) / float64(collectTime.Sub(lastCPUStat.ts))
	// 1.0 CPU = 1000 Milli-CPU
	cpuUsageCores := resource.NewMilliQuantity(int64(cpuUsageValue*1000), resource.DecimalSI)
	klog.V(6).Infof("collectBECPUUsageCores finished %.2f", cpuUsageValue)
	return cpuUsageCores, nil
}
