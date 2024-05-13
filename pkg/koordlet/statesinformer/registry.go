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

package statesinformer

func (s *statesInformer) initInformerPlugins() {
	s.states.informerPlugins = map[pluginName]informerPlugin{
		// 根据crd中slo的策略，更新pod的cgroup的值。
		nodeSLOInformerName:    NewNodeSLOInformer(),
		// 计算或者更新node的 cpu topology资源，
		nodeTopoInformerName:   NewNodeTopoInformer(),
		// 收集节点的be类型的Allocatable资源指标，上报prometheus
		nodeInformerName:       NewNodeInformer(),
		// 获取节点上所有的pod信息，并抽取pod的资源指标，上报prometheus,并在本地缓存pod的信息
		podsInformerName:       NewPodsInformer(),
		// 从本地db中获取 nodeMetric，然后上报更新到crd，供调度器使用。
		nodeMetricInformerName: NewNodeMetricInformer(),
	}
}
