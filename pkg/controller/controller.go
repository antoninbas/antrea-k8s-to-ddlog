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
	"time"

	v1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	networkinginformers "k8s.io/client-go/informers/networking/v1"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	networkinglisters "k8s.io/client-go/listers/networking/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"

	"github.com/antoninbas/antrea-k8s-to-ddlog/pkg/ddlog"
)

const (
	// Interval of synchronizing status from apiserver.
	syncPeriod = 6000 * time.Second
	// How long to wait before retrying the processing of a NetworkPolicy change.
	minRetryDelay = 5 * time.Second
	maxRetryDelay = 300 * time.Second
	// Default number of workers processing a NetworkPolicy change.
	defaultWorkers = 4
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

	ddlogProgram *ddlog.Program
}

// NewController returns a new *Controller.
func NewController(
	kubeClient clientset.Interface,
	podInformer coreinformers.PodInformer,
	namespaceInformer coreinformers.NamespaceInformer,
	networkPolicyInformer networkinginformers.NetworkPolicyInformer,
	ddlogProgram *ddlog.Program,
) *Controller {
	n := &Controller{
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
	}
	// Add handlers for Pod events.
	podInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    n.addPod,
			UpdateFunc: n.updatePod,
			DeleteFunc: n.deletePod,
		},
		syncPeriod,
	)
	// Add handlers for Namespace events.
	namespaceInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    n.addNamespace,
			UpdateFunc: n.updateNamespace,
			DeleteFunc: n.deleteNamespace,
		},
		syncPeriod,
	)
	// Add handlers for NetworkPolicy events.
	networkPolicyInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    n.addNetworkPolicy,
			UpdateFunc: n.updateNetworkPolicy,
			DeleteFunc: n.deleteNetworkPolicy,
		},
		syncPeriod,
	)
	return n
}

func (n *Controller) addNetworkPolicy(obj interface{}) {
	np := obj.(*networkingv1.NetworkPolicy)
	r := ddlog.RecordNetworkPolicy(np)
	klog.Infof("INSERT NETWORK POLICY: %v", r.Dump())
	cmd := ddlog.NewInsertCommand(ddlog.NetworkPolicyTableID, r)
	if err := n.ddlogProgram.ApplyUpdatesAsTransaction(cmd); err != nil {
		klog.Errorf("Error when applying command to add Network Policy: %v", err)
	}
}

func (n *Controller) updateNetworkPolicy(old, cur interface{}) {
	np := cur.(*networkingv1.NetworkPolicy)
	r := ddlog.RecordNetworkPolicy(np)
	klog.Infof("INSERT NETWORK POLICY: %v", r.Dump())
	cmd := ddlog.NewInsertCommand(ddlog.NetworkPolicyTableID, r)
	if err := n.ddlogProgram.ApplyUpdatesAsTransaction(cmd); err != nil {
		klog.Errorf("Error when applying command to modify Network Policy: %v", err)
	}
}

func (n *Controller) deleteNetworkPolicy(old interface{}) {
	np := old.(*networkingv1.NetworkPolicy)
	r := ddlog.RecordNetworkPolicy(np)
	klog.Infof("DELETE NETWORK POLICY: %v", r.Dump())
	cmd := ddlog.NewDeleteValCommand(ddlog.NetworkPolicyTableID, r)
	if err := n.ddlogProgram.ApplyUpdatesAsTransaction(cmd); err != nil {
		klog.Errorf("Error when applying command to delete Network Policy: %v", err)
	}
}

func (n *Controller) addPod(obj interface{}) {
	pod := obj.(*v1.Pod)
	r := ddlog.RecordPod(pod)
	klog.Infof("INSERT POD: %v", r.Dump())
	cmd := ddlog.NewInsertCommand(ddlog.PodTableID, r)
	if err := n.ddlogProgram.ApplyUpdatesAsTransaction(cmd); err != nil {
		klog.Errorf("Error when applying command to add Pod: %v", err)
	}
}

func (n *Controller) updatePod(oldObj, curObj interface{}) {
	pod := curObj.(*v1.Pod)
	r := ddlog.RecordPod(pod)
	klog.Infof("INSERT POD: %v", r.Dump())
	cmd := ddlog.NewInsertCommand(ddlog.PodTableID, r)
	if err := n.ddlogProgram.ApplyUpdatesAsTransaction(cmd); err != nil {
		klog.Errorf("Error when applying command to update Pod: %v", err)
	}
}

func (n *Controller) deletePod(old interface{}) {
	pod := old.(*v1.Pod)
	r := ddlog.RecordPod(pod)
	klog.Infof("DELETE POD: %v", r.Dump())
	cmd := ddlog.NewDeleteValCommand(ddlog.PodTableID, r)
	if err := n.ddlogProgram.ApplyUpdatesAsTransaction(cmd); err != nil {
		klog.Errorf("Error when applying command to delete Pod: %v", err)
	}
}

func (n *Controller) addNamespace(obj interface{}) {
	namespace := obj.(*v1.Namespace)
	r := ddlog.RecordNamespace(namespace)
	klog.Infof("INSERT NAMESPACE: %v", r.Dump())
	cmd := ddlog.NewInsertCommand(ddlog.NamespaceTableID, r)
	if err := n.ddlogProgram.ApplyUpdatesAsTransaction(cmd); err != nil {
		klog.Errorf("Error when applying command to add Namespace: %v", err)
	}
}

func (n *Controller) updateNamespace(oldObj, curObj interface{}) {
	namespace := curObj.(*v1.Namespace)
	r := ddlog.RecordNamespace(namespace)
	klog.Infof("INSERT NAMESPACE: %v", r.Dump())
	cmd := ddlog.NewInsertCommand(ddlog.NamespaceTableID, r)
	if err := n.ddlogProgram.ApplyUpdatesAsTransaction(cmd); err != nil {
		klog.Errorf("Error when applying command to update Namespace: %v", err)
	}
}

func (n *Controller) deleteNamespace(old interface{}) {
	namespace := old.(*v1.Namespace)
	r := ddlog.RecordNamespace(namespace)
	klog.Infof("INSERT NAMESPACE: %v", r.Dump())
	cmd := ddlog.NewDeleteValCommand(ddlog.NamespaceTableID, r)
	if err := n.ddlogProgram.ApplyUpdatesAsTransaction(cmd); err != nil {
		klog.Errorf("Error when applying command to delete Namespace: %v", err)
	}
}
