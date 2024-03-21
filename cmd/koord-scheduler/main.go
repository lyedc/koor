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

package main

import (
	"math/rand"
	"os"
	"time"

	"k8s.io/component-base/logs"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"

	"github.com/koordinator-sh/koordinator/cmd/koord-scheduler/app"
	"github.com/koordinator-sh/koordinator/pkg/scheduler/frameworkext"
	"github.com/koordinator-sh/koordinator/pkg/scheduler/plugins/batchresource"
	"github.com/koordinator-sh/koordinator/pkg/scheduler/plugins/compatibledefaultpreemption"
	"github.com/koordinator-sh/koordinator/pkg/scheduler/plugins/coscheduling"
	"github.com/koordinator-sh/koordinator/pkg/scheduler/plugins/deviceshare"
	"github.com/koordinator-sh/koordinator/pkg/scheduler/plugins/elasticquota"
	"github.com/koordinator-sh/koordinator/pkg/scheduler/plugins/loadaware"
	"github.com/koordinator-sh/koordinator/pkg/scheduler/plugins/nodenumaresource"
	"github.com/koordinator-sh/koordinator/pkg/scheduler/plugins/reservation"

	// Ensure metric package is initialized
	_ "k8s.io/component-base/metrics/prometheus/clientgo"
	// Ensure scheme package is initialized.
	_ "github.com/koordinator-sh/koordinator/pkg/scheduler/apis/config/scheme"
)

var koordinatorPlugins = map[string]frameworkruntime.PluginFactory{
	//按照koordlet上报的资源，以及node节点的资源限制进行调度过滤。
	loadaware.Name: loadaware.New,
	// 按照node的numa拓扑进行调度。
	nodenumaresource.Name: nodenumaresource.New,
	// 资源预留调度，具体是怎么调度的，怎么生成了一个未知的pod，然后绑定到node上面，并且不展示资源。
	reservation.Name: reservation.New,
	/*
		// 把pod的request替换为 batch类型的资源进行计算以及调度
		// 在pod的webhook的调用中把默认的资源修改为batch类型的,这类资源不会在kubelet中展现，所以说是超分了资源
		// 首先这种类型的pod，会判断node上面有没有足够的batch资源，如果有的话，就使用，如果没有就不能被调度了。
		// 如果node节点上的资源不够了，batch类型的资源怎么回收？ 采用be类型的qos？ 自动驱逐？？
		// 资源是这里来的：  Node(BE).Alloc = Node.Total - Node.Reserved - System.Used - Pod(LS).Used 计算出来的。并且是随着资源
		使用的情况动态变化的。
		NodeResourceReconciler) prepareNodeResource
		resources:
		limits:
		  kubernetes.io/batch-cpu: '500'
		  kubernetes.io/batch-memory: 36Mi
		requests:
		  kubernetes.io/batch-cpu: '500'
		  kubernetes.io/batch-memory: 36Mi
	*/
	batchresource.Name: batchresource.New,
	// GangScheduling调度,也就是podGroup的调度
	coscheduling.Name: coscheduling.New,
	//gpu调度
	deviceshare.Name: deviceshare.New,
	// 弹性配额调度 Elastic Quota
	elasticquota.Name:                elasticquota.New,
	compatibledefaultpreemption.Name: compatibledefaultpreemption.New,
}

// Register custom scheduling hooks for pre-process scheduling context before call plugins.
// e.g. change the nodeInfo and make a copy before calling filter plugins
var schedulingHooks = []frameworkext.SchedulingPhaseHook{
	reservation.NewHook(),
}

func flatten(plugins map[string]frameworkruntime.PluginFactory) []app.Option {
	options := make([]app.Option, 0, len(plugins))
	for name, factoryFn := range plugins {
		options = append(options, app.WithPlugin(name, factoryFn))
	}
	return options
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// Register custom plugins to the scheduler framework.
	// Later they can consist of scheduler profile(s) and hence
	// used by various kinds of workloads.
	command := app.NewSchedulerCommand(
		schedulingHooks,
		flatten(koordinatorPlugins)...,
	)

	logs.InitLogs()
	defer logs.FlushLogs()

	if err := command.Execute(); err != nil {
		os.Exit(1)
	}
}
