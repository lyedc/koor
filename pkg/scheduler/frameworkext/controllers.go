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

package frameworkext

import (
	"k8s.io/klog/v2"

	"github.com/koordinator-sh/koordinator/pkg/descheduler/framework"
)

// 目前有(cs *Coscheduling) NewControllers()
// (g *Plugin) NewControllers() ([]frameworkext.Controller, error)
// 实现了这两个方法，后续如果有需要后台任务运行的controller的话，可以继续实现这个方法。
type ControllerProvider interface {
	NewControllers() ([]Controller, error)
}

type Controller interface {
	Start()
	Name() string
}

type ControllersMap struct {
	controllers map[string]map[string]Controller
}

func NewControllersMap() *ControllersMap {
	return &ControllersMap{
		controllers: make(map[string]map[string]Controller),
	}
}

func (cm *ControllersMap) RegisterControllers(plugin framework.Plugin) {
	controllerProvider, ok := plugin.(ControllerProvider)
	if !ok {
		return
	}
	pluginControllers := cm.controllers[plugin.Name()]
	if len(pluginControllers) > 0 {
		klog.Warningf("Plugin %s already build controllers, skip it", plugin.Name())
		return
	}

	pluginControllers = make(map[string]Controller)
	// 这里是具体的插件的具体注册实现例如 quota，podGroup等
	if controllers, err := controllerProvider.NewControllers(); err == nil {
		for _, controller := range controllers {
			if _, exist := pluginControllers[controller.Name()]; exist {
				klog.Warningf("controller: %v already registered", controller.Name())
				continue
			}
			pluginControllers[controller.Name()] = controller
			klog.Infof("register plugin:%v controller:%v", plugin.Name(), controller.Name())
		}
		cm.controllers[plugin.Name()] = pluginControllers
	}
}

// 启动注册的controller的逻辑。
func (cm *ControllersMap) Start() {
	for _, plugin := range cm.controllers {
		for _, controller := range plugin {
			controller.Start()
		}
	}
}
