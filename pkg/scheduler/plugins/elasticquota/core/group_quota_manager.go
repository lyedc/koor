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

package core

import (
	"fmt"
	"reflect"
	"sync"

	v1 "k8s.io/api/core/v1"
	quotav1 "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/v1/resource"
	"sigs.k8s.io/scheduler-plugins/pkg/apis/scheduling/v1alpha1"

	"github.com/koordinator-sh/koordinator/apis/extension"
	"github.com/koordinator-sh/koordinator/pkg/util"
)

type GroupQuotaManager struct {
	// hierarchyUpdateLock used for resourceKeys/quotaInfoMap/quotaTreeWrapper change
	hierarchyUpdateLock sync.RWMutex
	// totalResource without systemQuotaGroup and DefaultQuotaGroup's used Quota
	totalResourceExceptSystemAndDefaultUsed v1.ResourceList
	// totalResource with systemQuotaGroup and DefaultQuotaGroup's used Quota
	totalResource v1.ResourceList
	// resourceKeys helps to store runtimeQuotaCalculators' resourceKey
	resourceKeys map[v1.ResourceName]struct{}
	// quotaInfoMap stores all the nodes, it can help get all parents conveniently
	quotaInfoMap map[string]*QuotaInfo
	// runtimeQuotaCalculatorMap helps calculate the subGroups' runtimeQuota in one quotaGroup
	runtimeQuotaCalculatorMap map[string]*RuntimeQuotaCalculator
	// quotaTopoNodeMap only stores the topology of the quota
	quotaTopoNodeMap     map[string]*QuotaTopoNode
	scaleMinQuotaEnabled bool
	// scaleMinQuotaManager is used when overRootResource
	scaleMinQuotaManager *ScaleMinQuotaManager
	once                 sync.Once
}

func NewGroupQuotaManager(systemGroupMax, defaultGroupMax v1.ResourceList) *GroupQuotaManager {
	quotaManager := &GroupQuotaManager{
		totalResourceExceptSystemAndDefaultUsed: v1.ResourceList{},
		totalResource:                           v1.ResourceList{},
		resourceKeys:                            make(map[v1.ResourceName]struct{}),
		quotaInfoMap:                            make(map[string]*QuotaInfo),
		runtimeQuotaCalculatorMap:               make(map[string]*RuntimeQuotaCalculator),
		quotaTopoNodeMap:                        make(map[string]*QuotaTopoNode),
		scaleMinQuotaManager:                    NewScaleMinQuotaManager(),
	}
	quotaManager.quotaInfoMap[extension.SystemQuotaName] = NewQuotaInfo(false, true, extension.SystemQuotaName, extension.RootQuotaName)
	quotaManager.quotaInfoMap[extension.SystemQuotaName].setMaxQuotaNoLock(systemGroupMax)
	quotaManager.quotaInfoMap[extension.DefaultQuotaName] = NewQuotaInfo(false, true, extension.DefaultQuotaName, extension.RootQuotaName)
	quotaManager.quotaInfoMap[extension.DefaultQuotaName].setMaxQuotaNoLock(defaultGroupMax)
	quotaManager.runtimeQuotaCalculatorMap[extension.RootQuotaName] = NewRuntimeQuotaCalculator(extension.RootQuotaName)
	quotaManager.setScaleMinQuotaEnabled(true)
	return quotaManager
}

func (gqm *GroupQuotaManager) setScaleMinQuotaEnabled(flag bool) {
	gqm.hierarchyUpdateLock.Lock()
	defer gqm.hierarchyUpdateLock.Unlock()

	gqm.scaleMinQuotaEnabled = flag
	klog.V(5).Infof("Set ScaleMinQuotaEnabled, flag:%v", gqm.scaleMinQuotaEnabled)
}

func (gqm *GroupQuotaManager) UpdateClusterTotalResource(deltaRes v1.ResourceList) {
	gqm.hierarchyUpdateLock.Lock()
	defer gqm.hierarchyUpdateLock.Unlock()

	klog.V(5).Infof("UpdateClusterResource deltaRes:%v", deltaRes)
	gqm.getQuotaInfoByNameNoLock(extension.DefaultQuotaName).lock.Lock()
	defer gqm.getQuotaInfoByNameNoLock(extension.DefaultQuotaName).lock.Unlock()

	gqm.getQuotaInfoByNameNoLock(extension.SystemQuotaName).lock.Lock()
	defer gqm.getQuotaInfoByNameNoLock(extension.SystemQuotaName).lock.Unlock()

	gqm.updateClusterTotalResourceNoLock(deltaRes)
}

// updateClusterTotalResourceNoLock no need to lock gqm.hierarchyUpdateLock and system/defaultQuotaGroup's lock
func (gqm *GroupQuotaManager) updateClusterTotalResourceNoLock(deltaRes v1.ResourceList) {
	fmt.Println(gqm.totalResource.Cpu().Value())
	fmt.Println(gqm.totalResource.Memory().Value())
	gqm.totalResource = quotav1.Add(gqm.totalResource, deltaRes)

	sysAndDefaultUsed := gqm.quotaInfoMap[extension.DefaultQuotaName].CalculateInfo.Used.DeepCopy()
	fmt.Println(sysAndDefaultUsed.Cpu().Value())
	fmt.Println(sysAndDefaultUsed.Memory().Value())
	sysAndDefaultUsed = quotav1.Add(sysAndDefaultUsed, gqm.quotaInfoMap[extension.SystemQuotaName].CalculateInfo.Used.DeepCopy())
	totalResNoSysOrDefault := quotav1.Subtract(gqm.totalResource, sysAndDefaultUsed)

	diffRes := quotav1.Subtract(totalResNoSysOrDefault, gqm.totalResourceExceptSystemAndDefaultUsed)

	if !quotav1.IsZero(diffRes) {
		gqm.totalResourceExceptSystemAndDefaultUsed = totalResNoSysOrDefault.DeepCopy()
		gqm.runtimeQuotaCalculatorMap[extension.RootQuotaName].setClusterTotalResource(totalResNoSysOrDefault)
		klog.V(5).Infof("UpdateClusterResource finish totalResourceExceptSystemAndDefaultUsed:%v", gqm.totalResourceExceptSystemAndDefaultUsed)
	}
}

func (gqm *GroupQuotaManager) GetClusterTotalResource() v1.ResourceList {
	gqm.hierarchyUpdateLock.RLock()
	defer gqm.hierarchyUpdateLock.RUnlock()

	return gqm.totalResource.DeepCopy()
}

// updateGroupDeltaRequestNoLock no need lock gqm.lock
func (gqm *GroupQuotaManager) updateGroupDeltaRequestNoLock(quotaName string, deltaReq v1.ResourceList) {
	curToAllParInfos := gqm.getCurToAllParentGroupQuotaInfoNoLock(quotaName)
	allQuotaInfoLen := len(curToAllParInfos)
	if allQuotaInfoLen <= 0 {
		return
	}

	defer gqm.scopedLockForQuotaInfo(curToAllParInfos)()
    // // 根据quotaInfo中的limit，计算出最大的request的值，更新到QuotaTree中的quotaNode中。
	gqm.recursiveUpdateGroupTreeWithDeltaRequest(deltaReq, curToAllParInfos)
}

// recursiveUpdateGroupTreeWithDeltaRequest update the quota of a node, also need update all parentNode, the lock operation
// of all quotaInfo is done by gqm. scopedLockForQuotaInfo, so just get treeWrappers' lock when calling treeWrappers' function
// 根据请求的差量来从下到上更新整个tree中的节点上的request的值，quotaNode中存储的值。
// // 根据quotaInfo中的limit，计算出最大的request的值，更新到quotaNode中。
func (gqm *GroupQuotaManager) recursiveUpdateGroupTreeWithDeltaRequest(deltaReq v1.ResourceList, curToAllParInfos []*QuotaInfo) {
	for i := 0; i < len(curToAllParInfos); i++ {
		curQuotaInfo := curToAllParInfos[i]
		oldSubLimitReq := curQuotaInfo.getLimitRequestNoLock()
		// 计算request的值，并更新到quotaInfo的CalculateInfo对象中。
		curQuotaInfo.addRequestNonNegativeNoLock(deltaReq)
		if curQuotaInfo.Name == extension.SystemQuotaName || curQuotaInfo.Name == extension.DefaultQuotaName {
			return
		}
		newSubLimitReq := curQuotaInfo.getLimitRequestNoLock()
		deltaReq = quotav1.Subtract(newSubLimitReq, oldSubLimitReq)
       // 返回的对象的作用：
       // RuntimeQuotaCalculator 结构体的主要功能是针对某个特定的配额信息（treeName），计算子配额组在该配额信息下的运行配额
		directParRuntimeCalculatorPtr := gqm.getRuntimeQuotaCalculatorByNameNoLock(curQuotaInfo.ParentName)
		if directParRuntimeCalculatorPtr == nil {
			klog.Errorf("treeWrapper not exist! quotaName:%v  parentName:%v", curQuotaInfo.Name, curQuotaInfo.ParentName)
			return
		}
		if directParRuntimeCalculatorPtr.needUpdateOneGroupRequest(curQuotaInfo) {
			// 传入的对象是：具体的当前某个字配额的信息。
			directParRuntimeCalculatorPtr.updateOneGroupRequest(curQuotaInfo)
		}
	}
}

// updateGroupDeltaUsedNoLock updates the usedQuota of a node, it also updates all parent nodes
// no need to lock gqm.hierarchyUpdateLock
// 更新节点的use信息，并从更新父节点也就是整个树的情况。
func (gqm *GroupQuotaManager) updateGroupDeltaUsedNoLock(quotaName string, delta v1.ResourceList) {
	curToAllParInfos := gqm.getCurToAllParentGroupQuotaInfoNoLock(quotaName)
	allQuotaInfoLen := len(curToAllParInfos)
	if allQuotaInfoLen <= 0 {
		return
	}

	defer gqm.scopedLockForQuotaInfo(curToAllParInfos)()
	for i := 0; i < allQuotaInfoLen; i++ {
		quotaInfo := curToAllParInfos[i]
		quotaInfo.addUsedNonNegativeNoLock(delta)
	}

	// if systemQuotaGroup or DefaultQuotaGroup's used change, update cluster total resource.
	if quotaName == extension.SystemQuotaName || quotaName == extension.DefaultQuotaName {
		gqm.updateClusterTotalResourceNoLock(v1.ResourceList{})
	}
}

func (gqm *GroupQuotaManager) RefreshRuntime(quotaName string) v1.ResourceList {
	gqm.hierarchyUpdateLock.RLock()
	defer gqm.hierarchyUpdateLock.RUnlock()

	return gqm.RefreshRuntimeNoLock(quotaName)
}

// RefreshRuntimeNoLock 用于刷新指定配额组的运行时资源使用情况，不加锁执行
func (gqm *GroupQuotaManager) RefreshRuntimeNoLock(quotaName string) v1.ResourceList {
	quotaInfo := gqm.getQuotaInfoByNameNoLock(quotaName)
	if quotaInfo == nil {
		return nil
	}

	if quotaName == extension.SystemQuotaName || quotaName == extension.DefaultQuotaName {
		return quotaInfo.getMax()
	}
    // 获取所有的quota的所有的父节点的quotaInfo
	curToAllParInfos := gqm.getCurToAllParentGroupQuotaInfoNoLock(quotaInfo.Name)

	defer gqm.scopedLockForQuotaInfo(curToAllParInfos)()

	totalRes := gqm.totalResourceExceptSystemAndDefaultUsed.DeepCopy()
	for i := len(curToAllParInfos) - 1; i >= 0; i-- {
		quotaInfo = curToAllParInfos[i]
		// 获取父配额组的运行时配额计算器
		parRuntimeQuotaCalculator := gqm.getRuntimeQuotaCalculatorByNameNoLock(quotaInfo.ParentName)
		if parRuntimeQuotaCalculator == nil {
			klog.Errorf("treeWrapper not exist! parentQuotaName:%v", quotaInfo.ParentName)
			return nil
		}
		// 获取当前配额组的运行时配额计算器
		subTreeWrapper := gqm.getRuntimeQuotaCalculatorByNameNoLock(quotaInfo.Name)
		if subTreeWrapper == nil {
			klog.Errorf("treeWrapper not exist! parentQuotaName:%v", quotaInfo.Name)
			return nil
		}

		// 1. execute scaleMin logic with totalRes and update scaledMin if needed
		// 执行最小配额缩放逻辑，并更新缩放后的最小配额
		if gqm.scaleMinQuotaEnabled {
			needScale, newMinQuota := gqm.scaleMinQuotaManager.getScaledMinQuota(
				totalRes, quotaInfo.ParentName, quotaInfo.Name)
			if needScale {
				gqm.updateOneGroupAutoScaleMinQuotaNoLock(quotaInfo, newMinQuota)
			}
		}

		// 2. update parent's runtimeQuota
		// 更新父配额组的运行时配额
		if quotaInfo.RuntimeVersion != parRuntimeQuotaCalculator.getVersion() {
			parRuntimeQuotaCalculator.updateOneGroupRuntimeQuota(quotaInfo)
		}
		newSubGroupsTotalRes := quotaInfo.CalculateInfo.Runtime.DeepCopy()

		// 3. update subGroup's cluster resource  when i >= 1 (still has children)
		// 如果当前配额组还有父配额组，更新父配额组的集群资源总量
		if i >= 1 {
			subTreeWrapper.setClusterTotalResource(newSubGroupsTotalRes)
		}

		// 4. update totalRes
		totalRes = newSubGroupsTotalRes
	}

	return curToAllParInfos[0].getMaskedRuntimeNoLock()
}

// updateOneGroupAutoScaleMinQuotaNoLock no need to lock gqm.lock
func (gqm *GroupQuotaManager) updateOneGroupAutoScaleMinQuotaNoLock(quotaInfo *QuotaInfo, newMinRes v1.ResourceList) {
	if !quotav1.Equals(quotaInfo.CalculateInfo.AutoScaleMin, newMinRes) {
		quotaInfo.setAutoScaleMinQuotaNoLock(newMinRes)
		gqm.runtimeQuotaCalculatorMap[quotaInfo.ParentName].updateOneGroupMinQuota(quotaInfo)
	}
}

func (gqm *GroupQuotaManager) getCurToAllParentGroupQuotaInfoNoLock(quotaName string) []*QuotaInfo {
	curToAllParInfos := make([]*QuotaInfo, 0)
	quotaInfo := gqm.getQuotaInfoByNameNoLock(quotaName)
	if quotaInfo == nil {
		return curToAllParInfos
	}

	for true {
		curToAllParInfos = append(curToAllParInfos, quotaInfo)
		if quotaInfo.ParentName == extension.RootQuotaName {
			break
		}

		quotaInfo = gqm.getQuotaInfoByNameNoLock(quotaInfo.ParentName)
		if quotaInfo == nil {
			return curToAllParInfos
		}
	}

	return curToAllParInfos
}

func (gqm *GroupQuotaManager) GetQuotaInfoByName(quotaName string) *QuotaInfo {
	gqm.hierarchyUpdateLock.RLock()
	defer gqm.hierarchyUpdateLock.RUnlock()

	return gqm.getQuotaInfoByNameNoLock(quotaName)
}

func (gqm *GroupQuotaManager) getQuotaInfoByNameNoLock(quotaName string) *QuotaInfo {
	return gqm.quotaInfoMap[quotaName]
}
// 返回的是：
/*
这个结构体的作用是帮助计算子配额组在对应的配额信息（treeName）下，所有资源维度的运行配额。

基于这段注释，可以推断出 RuntimeQuotaCalculator 结构体的主要功能是针对某个特定的配额信息（treeName），
计算子配额组在该配额信息下的运行配额。这个结构体可能会提供一系列方法来执行这个计算过程，并可能包含与配额相关的信息和逻辑
*/
func (gqm *GroupQuotaManager) getRuntimeQuotaCalculatorByNameNoLock(quotaName string) *RuntimeQuotaCalculator {
	return gqm.runtimeQuotaCalculatorMap[quotaName]
}

func (gqm *GroupQuotaManager) scopedLockForQuotaInfo(quotaList []*QuotaInfo) func() {
	listLen := len(quotaList)
	for i := listLen - 1; i >= 0; i-- {
		quotaList[i].lock.Lock()
	}

	return func() {
		for i := 0; i < listLen; i++ {
			quotaList[i].lock.Unlock()
		}
	}
}

// UpdateQuota 用于更新或删除配额信息。
// 如果isDelete为true，则删除指定的配额信息；
// 如果isDelete为false，则更新指定配额的信息。
// 参数:
// - quota: 指定要更新或删除的配额对象。
// - isDelete: 指示是否要删除配额，true为删除，false为更新。
// 返回值:
// - error: 操作过程中出现的错误，如果没有错误发生则为nil。
func (gqm *GroupQuotaManager) UpdateQuota(quota *v1alpha1.ElasticQuota, isDelete bool) error {
	// 加锁以确保更新过程的线程安全
	gqm.hierarchyUpdateLock.Lock()
	defer gqm.hierarchyUpdateLock.Unlock()

	quotaName := quota.Name
	if isDelete {
		// 如果是要删除配额，首先检查该配额是否存在
		_, exist := gqm.quotaInfoMap[quotaName]
		if !exist {
			return fmt.Errorf("get quota info failed, quotaName:%v", quotaName)
		}
		// 存在则从配额信息映射中删除该配额
		delete(gqm.quotaInfoMap, quotaName)
	} else {
		// 如果不是删除操作，则创建新的配额信息
		newQuotaInfo := NewQuotaInfoFromQuota(quota)
		// 检查配额信息是否已存在，若存在则更新，不存在则添加
		if localQuotaInfo, exist := gqm.quotaInfoMap[quotaName]; exist {
			// 如果配额元数据没有变化，则仅当运行时/使用量/请求量发生变化时才进行更新
			if !localQuotaInfo.isQuotaMetaChange(newQuotaInfo) {
				return nil
			}
			// 更新本地配额信息
			localQuotaInfo.updateQuotaInfoFromRemote(newQuotaInfo)
		} else {
			// 如果配额信息不存在，则直接添加到映射中,调度的时候，主要使用的及时quotaInfo中的信息。
			gqm.quotaInfoMap[quotaName] = newQuotaInfo
		}
	}
	// 不管是添加还是更新配额信息，最后都需更新配额组配置
	gqm.updateQuotaGroupConfigNoLock()

	return nil
}

// 这个方法用于在 GroupQuotaManager 中更新组配额的配置，包括重新构建拓扑结构和重置配额信息，以确保管理器处于一致的状态
func (gqm *GroupQuotaManager) updateQuotaGroupConfigNoLock() {
	// rebuild gqm.quotaTopoNodeMap
	// 这表明该方法负责重新构建组配额管理器中的拓扑结构，以便正确地管理组之间的层次关系
	// 组建顶层的quota也就是一个树形的层级结构，包含了父级和子级、
	// 构建自配额组的拓扑结构。
	// 把quotaInfoMap中的quotaInfo信息构建成树形结构，并更新到gqm.quotaTopoNodeMap中。
	gqm.buildSubParGroupTopoNoLock()
	// reset gqm.runtimeQuotaCalculator
	// 重置所有组的配额。这意味着该方法负责重置所有组的配额信息，通常是在重新构建拓扑结构之后需要执行的操作，以确保所有配额信息处于正确的状态
	gqm.resetAllGroupQuotaNoLock()
}

// buildSubParGroupTopoNoLock reBuild a nodeTree from root, no need to lock gqm.lock
func (gqm *GroupQuotaManager) buildSubParGroupTopoNoLock() {
	// rebuild QuotaTopoNodeMap
	gqm.quotaTopoNodeMap = make(map[string]*QuotaTopoNode)
	// 这里面定义了runtime
	// 这是一个树形的结构。
	rootNode := NewQuotaTopoNode(NewQuotaInfo(false, true, extension.RootQuotaName, extension.RootQuotaName))
	gqm.quotaTopoNodeMap[extension.RootQuotaName] = rootNode

	// add node according to the quotaInfoMap
	for quotaName, quotaInfo := range gqm.quotaInfoMap {
		if quotaName == extension.SystemQuotaName || quotaName == extension.DefaultQuotaName {
			continue
		}
		gqm.quotaTopoNodeMap[quotaName] = NewQuotaTopoNode(quotaInfo)
	}

	// build tree according to the parGroupName
	for _, topoNode := range gqm.quotaTopoNodeMap {
		if topoNode.name == extension.RootQuotaName {
			continue
		}
		parQuotaTopoNode := gqm.quotaTopoNodeMap[topoNode.quotaInfo.ParentName]
		// incase load child before its parent
		// 表示没有父节点，则创建一个临时的父节点
		if parQuotaTopoNode == nil {
			parQuotaTopoNode = NewQuotaTopoNode(&QuotaInfo{
				Name: topoNode.quotaInfo.ParentName,
			})
		}
		// 给叶子节点赋值他的父节点的信息，如果没有父节点的话，这里的父节点就是root节点。
		topoNode.parQuotaTopoNode = parQuotaTopoNode
		// 把自己加入到父节点的叶子节点中。形成一个树形的结构。
		parQuotaTopoNode.addChildGroupQuotaInfo(topoNode)
	}
}

// ResetAllGroupQuotaNoLock no need to lock gqm.lock
func (gqm *GroupQuotaManager) resetAllGroupQuotaNoLock() {
	//childRequestMap 和 childUsedMap，用于存储子配额组的请求和已使用配额
	childRequestMap, childUsedMap := make(quotaResMapType), make(quotaResMapType)
	// quotaTopoNodeMap 存储的是拓扑结构
	for quotaName, topoNode := range gqm.quotaTopoNodeMap {
		if quotaName == extension.RootQuotaName {
			continue
		}
		topoNode.quotaInfo.lock.Lock()
		if !topoNode.quotaInfo.IsParent {
			// 新加入的quota那么这里的request和user都是nil
			childRequestMap[quotaName] = topoNode.quotaInfo.CalculateInfo.Request.DeepCopy()
			childUsedMap[quotaName] = topoNode.quotaInfo.CalculateInfo.Used.DeepCopy()
		}
		// 清除计算后的配额的信息。
		topoNode.quotaInfo.clearForResetNoLock()
		topoNode.quotaInfo.lock.Unlock()
	}

	// clear old runtimeQuotaCalculator
	gqm.runtimeQuotaCalculatorMap = make(map[string]*RuntimeQuotaCalculator)
	// reset runtimeQuotaCalculator
	// RootQuotaName: root,相当于是重置了runtime的计算
	gqm.runtimeQuotaCalculatorMap[extension.RootQuotaName] = NewRuntimeQuotaCalculator(extension.RootQuotaName)
	// 设置最大的资源
	gqm.runtimeQuotaCalculatorMap[extension.RootQuotaName].setClusterTotalResource(gqm.totalResourceExceptSystemAndDefaultUsed)
	rootNode := gqm.quotaTopoNodeMap[extension.RootQuotaName]
	// 递归遍历每个group中的子group重置里卖弄的配额组的信息。保证正确性 通过父节点的runtime方法计算子节点最大，最小等情况，计算的结果被存储在RuntimeQuotaCalculator的quotaTree中。
	gqm.resetAllGroupQuotaRecursiveNoLock(rootNode)
	gqm.updateResourceKeyNoLock()

	// subGroup's topo relation may change; refresh the request/used from bottom to top
	// 从上到下更新节点上的 request和used的信息。。
	for quotaName, topoNode := range gqm.quotaTopoNodeMap {
		if !topoNode.quotaInfo.IsParent {
			// 更新tree结构中的request的值，
			gqm.updateGroupDeltaRequestNoLock(quotaName, childRequestMap[quotaName])
			gqm.updateGroupDeltaUsedNoLock(quotaName, childUsedMap[quotaName])
		}
	}
}

// ResetAllGroupQuotaRecursiveNoLock no need to lock gqm.lock
// 遍历每个配额组的子配额组，并对其进行递归地调用，以确保所有子配额组的配额信息都被正确地重置，这几个组合起来就是通过父节点的runtime方法计算子节点最大，最小等情况，计算的结果被存储在RuntimeQuotaCalculator的quotaTree中。
func (gqm *GroupQuotaManager) resetAllGroupQuotaRecursiveNoLock(rootNode *QuotaTopoNode) {
	// 获取全部的叶子节点的资源信息。
	childGroupQuotaInfos := rootNode.getChildGroupQuotaInfos()
	// subName是qutoa的名字，topoNode是这个quota的节点信息。
	for subName, topoNode := range childGroupQuotaInfos {
		//
		gqm.runtimeQuotaCalculatorMap[subName] = NewRuntimeQuotaCalculator(subName)
		// 更新该子配额组的最大配额
		gqm.updateOneGroupMaxQuotaNoLock(topoNode.quotaInfo)
		// 新该子配额组的最小配额
		gqm.updateMinQuotaNoLock(topoNode.quotaInfo)
		// 更新该子配额组的共享权重
		gqm.updateOneGroupSharedWeightNoLock(topoNode.quotaInfo)
        // 重置该子配额组的所有子配额组的配额信息
		gqm.resetAllGroupQuotaRecursiveNoLock(topoNode)
	}
}

// updateOneGroupMaxQuotaNoLock no need to lock gqm.lock
func (gqm *GroupQuotaManager) updateOneGroupMaxQuotaNoLock(quotaInfo *QuotaInfo) {
	quotaInfo.lock.Lock()
	defer quotaInfo.lock.Unlock()
	// 获取父配额组的运行时配额计算器
	// 这里返回的是NewRuntimeQuotaCalculator(extension.RootQuotaName) 这个对象
	// 这里的runtimeQuota是父节点的信息。quotaInfo是子节点的信息。
	runtimeQuotaCalculator := gqm.getRuntimeQuotaCalculatorByNameNoLock(quotaInfo.ParentName)
	runtimeQuotaCalculator.updateOneGroupMaxQuota(quotaInfo)
}

// updateMinQuotaNoLock no need to lock gqm.lock
func (gqm *GroupQuotaManager) updateMinQuotaNoLock(quotaInfo *QuotaInfo) {
	gqm.updateOneGroupOriginalMinQuotaNoLock(quotaInfo)
	gqm.scaleMinQuotaManager.update(quotaInfo.ParentName, quotaInfo.Name,
		quotaInfo.CalculateInfo.Min, gqm.scaleMinQuotaEnabled)
}

// updateOneGroupOriginalMinQuotaNoLock no need to lock gqm.lock
func (gqm *GroupQuotaManager) updateOneGroupOriginalMinQuotaNoLock(quotaInfo *QuotaInfo) {
	quotaInfo.lock.Lock()
	defer quotaInfo.lock.Unlock()

	quotaInfo.setAutoScaleMinQuotaNoLock(quotaInfo.CalculateInfo.Min)
	gqm.runtimeQuotaCalculatorMap[quotaInfo.ParentName].updateOneGroupMinQuota(quotaInfo)
}

// updateOneGroupSharedWeightNoLock no need to lock gqm.lock
func (gqm *GroupQuotaManager) updateOneGroupSharedWeightNoLock(quotaInfo *QuotaInfo) {
	quotaInfo.lock.Lock()
	defer quotaInfo.lock.Unlock()

	gqm.runtimeQuotaCalculatorMap[quotaInfo.ParentName].updateOneGroupSharedWeight(quotaInfo)
}

func (gqm *GroupQuotaManager) updateResourceKeyNoLock() {
	// collect all dimensions
	resourceKeys := make(map[v1.ResourceName]struct{})
	for quotaName, quotaInfo := range gqm.quotaInfoMap {
		if quotaName == extension.DefaultQuotaName || quotaName == extension.SystemQuotaName {
			continue
		}
		for resName := range quotaInfo.CalculateInfo.Max {
			resourceKeys[resName] = struct{}{}
		}
	}

	if !reflect.DeepEqual(resourceKeys, gqm.resourceKeys) {
		gqm.resourceKeys = resourceKeys
		for _, runtimeQuotaCalculator := range gqm.runtimeQuotaCalculatorMap {
			runtimeQuotaCalculator.updateResourceKeys(resourceKeys)
		}
	}
}

func (gqm *GroupQuotaManager) GetAllQuotaNames() map[string]struct{} {
	quotaInfoMap := make(map[string]struct{})
	gqm.hierarchyUpdateLock.RLock()
	defer gqm.hierarchyUpdateLock.RUnlock()

	for name := range gqm.quotaInfoMap {
		quotaInfoMap[name] = struct{}{}
	}
	return quotaInfoMap
}

func (gqm *GroupQuotaManager) updatePodRequestNoLock(quotaName string, oldPod, newPod *v1.Pod) {
	var oldPodReq, newPodReq v1.ResourceList
	if oldPod != nil {
		oldPodReq, _ = resource.PodRequestsAndLimits(oldPod)
	} else {
		oldPodReq = make(v1.ResourceList)
	}

	if newPod != nil {
		newPodReq, _ = resource.PodRequestsAndLimits(newPod)
		fmt.Println(newPodReq.Cpu().Value())
		fmt.Println(newPodReq.Memory().Value())
	} else {
		newPodReq = make(v1.ResourceList)
	}

	deltaReq := quotav1.Subtract(newPodReq, oldPodReq)
	if quotav1.IsZero(deltaReq) {
		return
	}
	// // 根据quotaInfo中的limit，计算出最大的request的值，更新到quotaNode中。
	gqm.updateGroupDeltaRequestNoLock(quotaName, deltaReq)
}

func (gqm *GroupQuotaManager) updatePodUsedNoLock(quotaName string, oldPod, newPod *v1.Pod) {
	quotaInfo := gqm.getQuotaInfoByNameNoLock(quotaName)
	if quotaInfo == nil {
		return
	}
	if !quotaInfo.GetPodIsAssigned(newPod) && !quotaInfo.GetPodIsAssigned(oldPod) {
		klog.V(5).Infof("updatePodUsed, isAssigned is false, quotaName:%v, podName:%v",
			quotaName, getPodName(oldPod, newPod))
		return
	}

	var oldPodUsed, newPodUsed v1.ResourceList
	if oldPod != nil {
		oldPodUsed, _ = resource.PodRequestsAndLimits(oldPod)
	} else {
		oldPodUsed = make(v1.ResourceList)
	}

	if newPod != nil {
		newPodUsed, _ = resource.PodRequestsAndLimits(newPod)
	} else {
		newPodUsed = make(v1.ResourceList)
	}

	deltaUsed := quotav1.Subtract(newPodUsed, oldPodUsed)
	if quotav1.IsZero(deltaUsed) {
		klog.V(5).Infof("updatePodUsed, deltaUsedIsZero, quotaName:%v, podName:%v, podUsed:%v",
			quotaName, getPodName(oldPod, newPod), newPodUsed)
		return
	}
	gqm.updateGroupDeltaUsedNoLock(quotaName, deltaUsed)
}

func (gqm *GroupQuotaManager) updatePodCacheNoLock(quotaName string, pod *v1.Pod, isAdd bool) {
	quotaInfo := gqm.getQuotaInfoByNameNoLock(quotaName)
	if quotaInfo == nil {
		return
	}

	if isAdd {
		quotaInfo.addPodIfNotPresent(pod)
	} else {
		quotaInfo.removePodIfPresent(pod)
	}
}

func (gqm *GroupQuotaManager) UpdatePodIsAssigned(quotaName string, pod *v1.Pod, isAssigned bool) error {
	gqm.hierarchyUpdateLock.RLock()
	defer gqm.hierarchyUpdateLock.RUnlock()

	return gqm.updatePodIsAssignedNoLock(quotaName, pod, isAssigned)
}

func (gqm *GroupQuotaManager) updatePodIsAssignedNoLock(quotaName string, pod *v1.Pod, isAssigned bool) error {
	quotaInfo := gqm.getQuotaInfoByNameNoLock(quotaName)
	// 在quota的cache标记这个pod已经被调度了。。
	return quotaInfo.UpdatePodIsAssigned(pod, isAssigned)
}

func (gqm *GroupQuotaManager) getPodIsAssignedNoLock(quotaName string, pod *v1.Pod) bool {
	quotaInfo := gqm.getQuotaInfoByNameNoLock(quotaName)
	if quotaInfo == nil {
		return false
	}
	return quotaInfo.GetPodIsAssigned(pod)
}
// out in 一个表示资源的出借，一个表示资源的借用，可以这样理解。
func (gqm *GroupQuotaManager) MigratePod(pod *v1.Pod, out, in string) {
	gqm.hierarchyUpdateLock.Lock()
	defer gqm.hierarchyUpdateLock.Unlock()

	isAssigned := gqm.getPodIsAssignedNoLock(out, pod)
	// 更新quotaNode中存储的request的值，
	gqm.updatePodRequestNoLock(out, pod, nil)
	// 更新 quotaInfo中CalculateInfo中use的值
	gqm.updatePodUsedNoLock(out, pod, nil)
	// 从podcache中移除改pod
	gqm.updatePodCacheNoLock(out, pod, false)

	gqm.updatePodCacheNoLock(in, pod, true)
	gqm.updatePodIsAssignedNoLock(in, pod, isAssigned)
	// 通过计算新pod和旧pod的差额。来更新quotaNode上面的值。也就是整个树上的值。
	// 根据quotaInfo中的limit，计算出最大的request的值，更新到quotaNode中。
	gqm.updatePodRequestNoLock(in, nil, pod)
	// 根据差值，计算quotaInfo中的Use的值，使用量。
	gqm.updatePodUsedNoLock(in, nil, pod)
	klog.V(5).Infof("migrate pod :%v from quota:%v to quota:%v, podPhase:%v", pod.Name, out, in, pod.Status.Phase)
}

func (gqm *GroupQuotaManager) GetQuotaSummary(quotaName string) (*QuotaInfoSummary, bool) {
	gqm.hierarchyUpdateLock.RLock()
	defer gqm.hierarchyUpdateLock.RUnlock()

	quotaInfo := gqm.getQuotaInfoByNameNoLock(quotaName)
	if quotaInfo == nil {
		return nil, false
	}

	quotaSummary := quotaInfo.GetQuotaSummary()
	runtime := gqm.RefreshRuntimeNoLock(quotaName)
	quotaSummary.Runtime = runtime.DeepCopy()
	return quotaSummary, true
}

func (gqm *GroupQuotaManager) GetQuotaSummaries() map[string]*QuotaInfoSummary {
	gqm.hierarchyUpdateLock.RLock()
	defer gqm.hierarchyUpdateLock.RUnlock()

	result := make(map[string]*QuotaInfoSummary)
	for quotaName, quotaInfo := range gqm.quotaInfoMap {
		quotaSummary := quotaInfo.GetQuotaSummary()
		runtime := gqm.RefreshRuntimeNoLock(quotaName)
		quotaSummary.Runtime = runtime.DeepCopy()
		result[quotaName] = quotaSummary
	}

	return result
}

func (gqm *GroupQuotaManager) OnPodAdd(quotaName string, pod *v1.Pod) {
	gqm.hierarchyUpdateLock.RLock()
	defer gqm.hierarchyUpdateLock.RUnlock()
    // 获取对应的资源对象，如果pod中定义的quota不存在就默认使用default的quota，也就是root的。
	quotaInfo := gqm.getQuotaInfoByNameNoLock(quotaName)
	if quotaInfo != nil && quotaInfo.isPodExist(pod) {
		return
	}
   // pod的信息添加到quta的PodCache对象中,表示已经进行了调度。
	gqm.updatePodCacheNoLock(quotaName, pod, true)
	// 更新tree结构中的request的值，quotaNode,下面是node和info的关系。
	// quotaNode stores the corresponding quotaInfo's information in a specific resource dimension.
	// "quotaNode" 负责管理和存储系统中特定类型或维度资源的配额信息
	// 貌似是从info中取值，计算后计入到node当中。
	gqm.updatePodRequestNoLock(quotaName, nil, pod)
	// in case failOver, update pod isAssigned explicitly according to its phase and NodeName.
	if pod.Spec.NodeName != "" && !util.IsPodTerminated(pod) {
		// 在对象 QuotaInfo 中的cache中标记改pod已经被调度。
		gqm.updatePodIsAssignedNoLock(quotaName, pod, true)
		// 跟新QuotaInfo对象。
		gqm.updatePodUsedNoLock(quotaName, nil, pod)
	}
}

func (gqm *GroupQuotaManager) OnPodUpdate(newQuotaName, oldQuotaName string, newPod, oldPod *v1.Pod) {
	gqm.hierarchyUpdateLock.RLock()
	defer gqm.hierarchyUpdateLock.RUnlock()

	if oldQuotaName == newQuotaName {
		gqm.updatePodRequestNoLock(newQuotaName, oldPod, newPod)
		gqm.updatePodUsedNoLock(newQuotaName, oldPod, newPod)
	} else {
		isAssigned := gqm.getPodIsAssignedNoLock(oldQuotaName, oldPod)
		gqm.updatePodRequestNoLock(oldQuotaName, oldPod, nil)
		gqm.updatePodUsedNoLock(oldQuotaName, oldPod, nil)
		gqm.updatePodCacheNoLock(oldQuotaName, oldPod, false)

		gqm.updatePodCacheNoLock(newQuotaName, newPod, true)
		gqm.updatePodIsAssignedNoLock(newQuotaName, newPod, isAssigned)
		gqm.updatePodRequestNoLock(newQuotaName, nil, newPod)
		gqm.updatePodUsedNoLock(newQuotaName, nil, newPod)
	}
}

func (gqm *GroupQuotaManager) OnPodDelete(quotaName string, pod *v1.Pod) {
	gqm.hierarchyUpdateLock.RLock()
	defer gqm.hierarchyUpdateLock.RUnlock()

	gqm.updatePodRequestNoLock(quotaName, pod, nil)
	gqm.updatePodUsedNoLock(quotaName, pod, nil)
	gqm.updatePodCacheNoLock(quotaName, pod, false)
}

func (gqm *GroupQuotaManager) ReservePod(quotaName string, p *v1.Pod) {
	gqm.hierarchyUpdateLock.RLock()
	defer gqm.hierarchyUpdateLock.RUnlock()

	gqm.updatePodIsAssignedNoLock(quotaName, p, true)
	gqm.updatePodUsedNoLock(quotaName, nil, p)
}

func (gqm *GroupQuotaManager) UnreservePod(quotaName string, p *v1.Pod) {
	gqm.hierarchyUpdateLock.RLock()
	defer gqm.hierarchyUpdateLock.RUnlock()

	gqm.updatePodUsedNoLock(quotaName, p, nil)
	gqm.updatePodIsAssignedNoLock(quotaName, p, false)
}

func (gqm *GroupQuotaManager) GetQuotaInformationForSyncHandler(quotaName string) (used, request, runtime v1.ResourceList, err error) {
	gqm.hierarchyUpdateLock.RLock()
	defer gqm.hierarchyUpdateLock.RUnlock()

	quotaInfo := gqm.getQuotaInfoByNameNoLock(quotaName)
	if quotaInfo == nil {
		return nil, nil, nil, fmt.Errorf("groupQuotaManager doesn't have this quota:%v", quotaName)
	}
    // 获取runtime的值
	runtime = gqm.RefreshRuntimeNoLock(quotaName)
	return quotaInfo.GetUsed(), quotaInfo.GetRequest(), runtime, nil
}

func getPodName(oldPod, newPod *v1.Pod) string {
	if oldPod != nil {
		return oldPod.Name
	}
	if newPod != nil {
		return newPod.Name
	}
	return ""
}
