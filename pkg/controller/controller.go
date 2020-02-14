// Copyright 2020 Antrea Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package networkpolicy provides NetworkPolicyController implementation to manage
// and synchronize the Pods and Namespaces affected by Network Policies and enforce
// their rules.

package controller

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	networkinginformers "k8s.io/client-go/informers/networking/v1"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	networkinglisters "k8s.io/client-go/listers/networking/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"

	"github.com/antoninbas/antrea-k8s-to-ddlog/pkg/ddlog"
)

const (
	// Interval of synchronizing status from apiserver.
	syncPeriod = 0
	// How long to wait before retrying the processing of a change.
	minRetryDelay = 1 * time.Second
	maxRetryDelay = 300 * time.Second
)

// NetworkPolicyController is responsible for synchronizing the Namespaces and Pods
// affected by a Network Policy.
type Controller struct {
	kubeClient  clientset.Interface
	podInformer coreinformers.PodInformer

	// podLister is able to list/get Pods and is populated by the shared informer passed to
	// NewNetworkPolicyController.
	podLister corelisters.PodLister

	// podListerSynced is a function which returns true if the Pod shared informer has been synced at least once.
	podListerSynced cache.InformerSynced

	namespaceInformer coreinformers.NamespaceInformer

	// namespaceLister is able to list/get Namespaces and is populated by the shared informer passed to
	// NewNetworkPolicyController.
	namespaceLister corelisters.NamespaceLister

	// namespaceListerSynced is a function which returns true if the Namespace shared informer has been synced at least once.
	namespaceListerSynced cache.InformerSynced

	networkPolicyInformer networkinginformers.NetworkPolicyInformer

	// networkPolicyLister is able to list/get Network Policies and is populated by the shared informer passed to
	// NewNetworkPolicyController.
	networkPolicyLister networkinglisters.NetworkPolicyLister

	// networkPolicyListerSynced is a function which returns true if the Network Policy shared informer has been synced at least once.
	networkPolicyListerSynced cache.InformerSynced

	queue workqueue.RateLimitingInterface

	ddlogProgram *ddlog.Program
}

type queueObjType int

const (
	queueObjPod queueObjType = iota
	queueObjNamespace
	queueObjNetworkPolicy
)

type queueObj struct {
	objType queueObjType
	key     string
}

// NewController returns a new *Controller.
func NewController(
	kubeClient clientset.Interface,
	podInformer coreinformers.PodInformer,
	namespaceInformer coreinformers.NamespaceInformer,
	networkPolicyInformer networkinginformers.NetworkPolicyInformer,
	ddlogProgram *ddlog.Program,
) *Controller {
	c := &Controller{
		kubeClient:                kubeClient,
		podInformer:               podInformer,
		podLister:                 podInformer.Lister(),
		podListerSynced:           podInformer.Informer().HasSynced,
		namespaceInformer:         namespaceInformer,
		namespaceLister:           namespaceInformer.Lister(),
		namespaceListerSynced:     namespaceInformer.Informer().HasSynced,
		networkPolicyInformer:     networkPolicyInformer,
		networkPolicyLister:       networkPolicyInformer.Lister(),
		networkPolicyListerSynced: networkPolicyInformer.Informer().HasSynced,
		ddlogProgram:              ddlogProgram,
		queue:                     workqueue.NewNamedRateLimitingQueue(workqueue.NewItemExponentialFailureRateLimiter(minRetryDelay, maxRetryDelay), "items"),
	}
	// Add handlers for Pod events.
	podInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { c.enqueue(queueObjPod, obj) },
			UpdateFunc: func(oldObj, curObj interface{}) { c.enqueue(queueObjPod, curObj) },
			DeleteFunc: func(obj interface{}) { c.enqueue(queueObjPod, obj) },
		},
		syncPeriod,
	)
	// Add handlers for Namespace events.
	namespaceInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { c.enqueue(queueObjNamespace, obj) },
			UpdateFunc: func(oldObj, curObj interface{}) { c.enqueue(queueObjNamespace, curObj) },
			DeleteFunc: func(obj interface{}) { c.enqueue(queueObjNamespace, obj) },
		},
		syncPeriod,
	)
	// Add handlers for NetworkPolicy events.
	networkPolicyInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { c.enqueue(queueObjNetworkPolicy, obj) },
			UpdateFunc: func(oldObj, curObj interface{}) { c.enqueue(queueObjNetworkPolicy, curObj) },
			DeleteFunc: func(obj interface{}) { c.enqueue(queueObjNetworkPolicy, obj) },
		},
		syncPeriod,
	)
	return c
}

func (c *Controller) enqueue(objType queueObjType, obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		klog.Errorf("Error when generating key for object: %v", err)
		return
	}
	queueObj := queueObj{objType, key}
	c.queue.Add(queueObj)
}

func (c *Controller) Run(stopCh <-chan struct{}) {
	defer c.queue.ShutDown()

	klog.Info("Starting controller")
	defer klog.Info("Shutting down controller")

	klog.Info("Waiting for caches to sync for controller")
	if !cache.WaitForCacheSync(stopCh, c.podListerSynced, c.namespaceListerSynced, c.networkPolicyListerSynced) {
		klog.Error("Unable to sync caches for controller")
		return
	}
	klog.Info("Caches are synced for controller")

	// all events are processed by the same worker, since DDLog does not support concurrent
	// transactions
	go wait.Until(c.worker, time.Second, stopCh)
	<-stopCh
}

func (c *Controller) worker() {
	for c.processNextEvent() {
	}
}

func (c *Controller) processNextEvent() bool {
	obj, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(obj)
	qObj := obj.(queueObj)
	var err error
	switch qObj.objType {
	case queueObjPod:
		err = c.processPod(qObj)
	case queueObjNamespace:
		err = c.processNamespace(qObj)
	case queueObjNetworkPolicy:
		err = c.processNetworkPolicy(qObj)
	}
	if err != nil {
		klog.Errorf("Error when processing event: %v", err)
		c.queue.AddRateLimited(obj)
		return true
	}
	c.queue.Forget(obj)
	return true
}

func (c *Controller) processPod(obj queueObj) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(obj.key)
	if err != nil {
		return fmt.Errorf("error when extracting Pod namespace and name: %v", err)
	}
	pod, err := c.podLister.Pods(namespace).Get(name)
	var cmd ddlog.Command
	if err != nil { // deletion
		r := ddlog.RecordPodKey(namespace, name)
		klog.Infof("DELETE POD: %s", r.Dump())
		cmd = ddlog.NewDeleteKeyCommand(ddlog.PodTableID, r)
	} else {
		r := ddlog.RecordPod(pod)
		klog.Infof("INSERT POD: %s", r.Dump())
		cmd = ddlog.NewInsertCommand(ddlog.PodTableID, r)
	}
	if err := c.ddlogProgram.ApplyUpdatesAsTransaction(cmd); err != nil {
		return fmt.Errorf("could not apply Pod update: %v", err)
	}
	return nil
}

func (c *Controller) processNamespace(obj queueObj) error {
	namespace, err := c.namespaceLister.Get(obj.key)
	var cmd ddlog.Command
	if err != nil { // deletion
		r := ddlog.RecordNamespaceKey(obj.key)
		klog.Infof("DELETE NAMESPACE: %s", r.Dump())
		cmd = ddlog.NewDeleteValCommand(ddlog.NamespaceTableID, r)
	} else {
		r := ddlog.RecordNamespace(namespace)
		klog.Infof("INSERT NAMESPACE: %s", r.Dump())
		cmd = ddlog.NewInsertCommand(ddlog.NamespaceTableID, r)
	}
	if err := c.ddlogProgram.ApplyUpdatesAsTransaction(cmd); err != nil {
		return fmt.Errorf("could not apply Namespace update: %v", err)
	}
	return nil
}

func (c *Controller) processNetworkPolicy(obj queueObj) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(obj.key)
	if err != nil {
		return fmt.Errorf("error when extracting NetworkPolicy namespace and name: %v", err)
	}
	networkPolicy, err := c.networkPolicyLister.NetworkPolicies(namespace).Get(name)
	var cmd ddlog.Command
	if err != nil { // deletion
		r := ddlog.RecordNetworkPolicyKey(namespace, name)
		klog.Infof("DELETE NETWORKPOLICY: %s", r.Dump())
		cmd = ddlog.NewDeleteKeyCommand(ddlog.NetworkPolicyTableID, r)
	} else {
		r := ddlog.RecordNetworkPolicy(networkPolicy)
		klog.Infof("INSERT NETWORKPOLICY: %s", r.Dump())
		cmd = ddlog.NewInsertCommand(ddlog.NetworkPolicyTableID, r)
	}
	if err := c.ddlogProgram.ApplyUpdatesAsTransaction(cmd); err != nil {
		return fmt.Errorf("could not apply NetworkPolicy update: %v", err)
	}
	return nil
}
