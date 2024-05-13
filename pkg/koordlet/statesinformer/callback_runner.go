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

import (
	"k8s.io/klog/v2"
)

type RegisterType int64

const (
	RegisterTypeNodeSLOSpec RegisterType = iota
	RegisterTypeAllPods
	RegisterTypeNodeTopology
)

func (r RegisterType) String() string {
	switch r {
	case RegisterTypeNodeSLOSpec:
		return "RegisterTypeNodeSLOSpec"
	case RegisterTypeAllPods:
		return "RegisterTypeAllPods"
	case RegisterTypeNodeTopology:
		return "RegisterTypeNodeTopology"

	default:
		return "RegisterTypeUnknown"
	}
}

type updateCallback struct {
	name        string
	description string
	fn          UpdateCbFn
}

type UpdateCbCtx struct{}
type UpdateCbFn func(t RegisterType, obj interface{}, pods []*PodMeta)

type callbackRunner struct {
	callbackChans        map[RegisterType]chan UpdateCbCtx
	stateUpdateCallbacks map[RegisterType][]updateCallback
	statesInformer       StatesInformer
}

func NewCallbackRunner() *callbackRunner {
	c := &callbackRunner{}
	c.callbackChans = map[RegisterType]chan UpdateCbCtx{
		RegisterTypeNodeSLOSpec:  make(chan UpdateCbCtx, 1),
		RegisterTypeAllPods:      make(chan UpdateCbCtx, 1),
		RegisterTypeNodeTopology: make(chan UpdateCbCtx, 1),
	}
	c.stateUpdateCallbacks = map[RegisterType][]updateCallback{
		RegisterTypeNodeSLOSpec:  {},
		RegisterTypeAllPods:      {},
		RegisterTypeNodeTopology: {},
	}
	return c
}

func (c *callbackRunner) Setup(s StatesInformer) {
	c.statesInformer = s
}

// 这里的注册的callback是通过 runtimehook 注册进来的.
/*
runtimeHook注册的两个类型的callback
RegisterTypeNodeSLOSpec
RegisterTypeNodeTopology
runttime 中的reconcilier注册了pod的callback
RegisterTypeAllPods
*/
func (s *callbackRunner) RegisterCallbacks(rType RegisterType, name, description string, callbackFn UpdateCbFn) {
	callbacks, legal := s.stateUpdateCallbacks[rType]
	if !legal {
		klog.Fatalf("states informer callback register with type %v is illegal", rType.String())
	}
	for _, c := range callbacks {
		if c.name == name {
			klog.Fatalf("states informer callback register %s with type %v already registered", name, rType.String())
		}
	}
	newCb := updateCallback{
		name:        name,
		description: description,
		fn:          callbackFn,
	}
	s.stateUpdateCallbacks[rType] = append(s.stateUpdateCallbacks[rType], newCb)
	klog.V(1).Infof("states informer callback %s has registered for type %v", name, rType.String())
}

// 目前 3 个informer中资源的更新都会触发 callback的方法, nodeslo  nodetopo  pod
func (s *callbackRunner) SendCallback(objType RegisterType) {
	if _, exist := s.callbackChans[objType]; exist {
		select {
		// 向channel发送消息,这样start方法中的select语句才能执行.
		case s.callbackChans[objType] <- struct{}{}:
			return
		default:
			klog.Infof("last callback runner %v has not finished, ignore this time", objType.String())
		}
	} else {
		klog.Warningf("callback runner %v is not exist", objType.String())
	}
}

// 参数含义: 如果是nodeslo的话, obj表示的是nodeslo的spc对象...
// nodeslo 调用的callback是这个方法:  在rule文件中的UpdateRules

/*
这里callback执行的结果举例,如果是rule类型的callback是对pod的response的值进行修改,相当于是拦截pod的response. 但是怎么下发给真实的cgroup呢.
*/
func (s *callbackRunner) runCallbacks(objType RegisterType, obj interface{}) {
	// 这里注册了 callback才能进行调用,也就是runtimehook要运行后才能进行第一次的调用.
	callbacks, exist := s.stateUpdateCallbacks[objType]
	if !exist {
		klog.Errorf("states informer callbacks type %v not exist", objType.String())
		return
	}
	pods := s.statesInformer.GetAllPods()
	for _, c := range callbacks {
		klog.V(5).Infof("start running callback function %v for type %v", c.name, objType.String())
		// 这里是调用注册到的callback的方法.例如:
		c.fn(objType, obj, pods)
	}
}

func (s *callbackRunner) Start(stopCh <-chan struct{}) {
	for t := range s.callbackChans {
		cbType := t
		go func() {
			for {
				select {
				// 启动后阻塞这里等待chanel触发. 这里当start时候还是一个空的chanel没有数据只能住宿.等待触发sendcallback..
				case cbCtx := <-s.callbackChans[cbType]:
					cbObj := s.getObjByType(cbType, cbCtx)
					if cbObj == nil {
						klog.Warningf("callback runner with type %v is not exist")
					} else {
						s.runCallbacks(cbType, cbObj)
					}
				case <-stopCh:
					klog.Infof("callback runner %v loop is exited", cbType.String())
					return
				}
			}
		}()
	}
}

func (s *callbackRunner) getObjByType(objType RegisterType, cbCtx UpdateCbCtx) interface{} {
	switch objType {
	case RegisterTypeNodeSLOSpec:
		nodeSLO := s.statesInformer.GetNodeSLO()
		if nodeSLO != nil {
			return &nodeSLO.Spec
		}
		return nil
	case RegisterTypeAllPods:
		return &struct{}{}
	case RegisterTypeNodeTopology:
		return s.statesInformer.GetNodeTopo()
	}
	return nil
}
