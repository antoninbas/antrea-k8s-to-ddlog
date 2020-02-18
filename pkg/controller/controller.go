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
	"context"
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

	defaultInputWorkers = 1

	maxUpdatesPerTransaction = 32
	maxTransactionDelay      = 100 * time.Millisecond
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

	podQueue workqueue.RateLimitingInterface

	namespaceQueue workqueue.RateLimitingInterface

	networkPolicyQueue workqueue.RateLimitingInterface

	ddlogProgram *ddlog.Program

	ddlogUpdatesCh chan ddlog.Command
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
		podQueue:                  workqueue.NewNamedRateLimitingQueue(workqueue.NewItemExponentialFailureRateLimiter(minRetryDelay, maxRetryDelay), "pods"),
		namespaceQueue:            workqueue.NewNamedRateLimitingQueue(workqueue.NewItemExponentialFailureRateLimiter(minRetryDelay, maxRetryDelay), "namespaces"),
		networkPolicyQueue:        workqueue.NewNamedRateLimitingQueue(workqueue.NewItemExponentialFailureRateLimiter(minRetryDelay, maxRetryDelay), "networkPolicies"),
		ddlogUpdatesCh:            make(chan ddlog.Command, maxUpdatesPerTransaction),
	}
	// Add handlers for Pod events.
	podInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    c.enqueuePod,
			UpdateFunc: func(oldObj, curObj interface{}) { c.enqueuePod(curObj) },
			DeleteFunc: c.enqueuePod,
		},
		syncPeriod,
	)
	// Add handlers for Namespace events.
	namespaceInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    c.enqueueNamespace,
			UpdateFunc: func(oldObj, curObj interface{}) { c.enqueueNamespace(curObj) },
			DeleteFunc: c.enqueueNamespace,
		},
		syncPeriod,
	)
	// Add handlers for NetworkPolicy events.
	networkPolicyInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    c.enqueueNetworkPolicy,
			UpdateFunc: func(oldObj, curObj interface{}) { c.enqueueNetworkPolicy(curObj) },
			DeleteFunc: c.enqueueNetworkPolicy,
		},
		syncPeriod,
	)
	return c
}

func (c *Controller) enqueuePod(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		klog.Errorf("Error when generating key for Pod: %v", err)
		return
	}
	c.podQueue.Add(key)
}

func (c *Controller) enqueueNamespace(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		klog.Errorf("Error when generating key for Namespace: %v", err)
		return
	}
	c.namespaceQueue.Add(key)
}

func (c *Controller) enqueueNetworkPolicy(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		klog.Errorf("Error when generating key for NetworkPolicy: %v", err)
		return
	}
	c.networkPolicyQueue.Add(key)
}

func (c *Controller) Run(stopCh <-chan struct{}) {
	defer c.podQueue.ShutDown()
	defer c.namespaceQueue.ShutDown()
	defer c.networkPolicyQueue.ShutDown()

	klog.Info("Starting controller")
	defer klog.Info("Shutting down controller")

	klog.Info("Waiting for caches to sync for controller")
	if !cache.WaitForCacheSync(stopCh, c.podListerSynced, c.namespaceListerSynced, c.networkPolicyListerSynced) {
		klog.Error("Unable to sync caches for controller")
		return
	}
	klog.Info("Caches are synced for controller")

	// one worker is in charge of all the transactions since DDLog does not support concurrent
	// transactions
	go c.generateTransactions(stopCh)

	for i := 0; i < defaultInputWorkers; i++ {
		go wait.Until(c.podWorker, time.Second, stopCh)
		go wait.Until(c.namespaceWorker, time.Second, stopCh)
		go wait.Until(c.networkPolicyWorker, time.Second, stopCh)
	}

	<-stopCh
}

// We assume that there cannot be transient issues with DDLog transactions, and so there is no point
// in retrying.
func (c *Controller) generateTransactions(stopCh <-chan struct{}) {
	transactionSize := 0
	parentCxt, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()

	ctx := parentCxt
	var cancel context.CancelFunc

	commitTransaction := func() {
		transactionSize = 0
		ctx = parentCxt
		if err := c.ddlogProgram.CommitTransaction(); err != nil {
			klog.Errorf("Error when committing DDLog transaction: %v", err)
		}
	}

	handleCommand := func(cmd ddlog.Command) {
		klog.V(2).Infof("Handling command")
		if transactionSize == 0 {
			// start transaction
			if err := c.ddlogProgram.StartTransaction(); err != nil {
				klog.Errorf("Error when starting DDLog transaction: %v", err)
				return
			}
			ctx, cancel = context.WithTimeout(parentCxt, maxTransactionDelay)
		}
		// add to transaction
		if err := c.ddlogProgram.ApplyUpdates(cmd); err != nil {
			klog.Errorf("Error when applying updates with DDLog: %v", err)
			return
		}
		transactionSize++
		if transactionSize >= maxUpdatesPerTransaction {
			cancel()
			commitTransaction()
		}
	}

	for {
		select {
		case cmd := <-c.ddlogUpdatesCh:
			handleCommand(cmd)
		case <-ctx.Done():
			commitTransaction()
		case <-stopCh:
			return
		}
	}
}

func (c *Controller) podWorker() {
	for c.processNextPod() {
	}
}

func (c *Controller) namespaceWorker() {
	for c.processNextNamespace() {
	}
}

func (c *Controller) networkPolicyWorker() {
	for c.processNextNetworkPolicy() {
	}
}

func (c *Controller) processNextPod() bool {
	obj, quit := c.podQueue.Get()
	if quit {
		return false
	}
	defer c.podQueue.Done(obj)
	key := obj.(string)
	if err := c.processPod(key); err != nil {
		klog.Errorf("Error when processing Pod '%s': %v", key, err)
		c.podQueue.AddRateLimited(obj)
		return true
	}
	c.podQueue.Forget(obj)
	return true
}

func (c *Controller) processPod(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
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
		klog.Infof("UPDATE POD: %s", r.Dump())
		cmd = ddlog.NewInsertOrUpdateCommand(ddlog.PodTableID, r)
	}
	c.ddlogUpdatesCh <- cmd
	return nil
}

func (c *Controller) processNextNamespace() bool {
	obj, quit := c.namespaceQueue.Get()
	if quit {
		return false
	}
	defer c.namespaceQueue.Done(obj)
	key := obj.(string)
	if err := c.processNamespace(key); err != nil {
		klog.Errorf("Error when processing Namespace '%s': %v", key, err)
		c.namespaceQueue.AddRateLimited(obj)
		return true
	}
	c.namespaceQueue.Forget(obj)
	return true
}

func (c *Controller) processNamespace(key string) error {
	namespace, err := c.namespaceLister.Get(key)
	var cmd ddlog.Command
	if err != nil { // deletion
		r := ddlog.RecordNamespaceKey(key)
		klog.Infof("DELETE NAMESPACE: %s", r.Dump())
		cmd = ddlog.NewDeleteKeyCommand(ddlog.NamespaceTableID, r)
	} else {
		r := ddlog.RecordNamespace(namespace)
		klog.Infof("UPDATE NAMESPACE: %s", r.Dump())
		cmd = ddlog.NewInsertOrUpdateCommand(ddlog.NamespaceTableID, r)
	}
	c.ddlogUpdatesCh <- cmd
	return nil
}

func (c *Controller) processNextNetworkPolicy() bool {
	obj, quit := c.networkPolicyQueue.Get()
	if quit {
		return false
	}
	defer c.networkPolicyQueue.Done(obj)
	key := obj.(string)
	if err := c.processNetworkPolicy(key); err != nil {
		klog.Errorf("Error when processing NetworkPolicy '%s': %v", key, err)
		c.networkPolicyQueue.AddRateLimited(obj)
		return true
	}
	c.networkPolicyQueue.Forget(obj)
	return true
}

func (c *Controller) processNetworkPolicy(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
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
		klog.Infof("UPDATE NETWORKPOLICY: %s", r.Dump())
		cmd = ddlog.NewInsertOrUpdateCommand(ddlog.NetworkPolicyTableID, r)
	}
	c.ddlogUpdatesCh <- cmd
	return nil
}
