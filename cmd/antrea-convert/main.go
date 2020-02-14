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

package main

import (
	"flag"
	"os"
	"path/filepath"
	"time"

	"github.com/antoninbas/antrea-k8s-to-ddlog/pkg/controller"
	"github.com/antoninbas/antrea-k8s-to-ddlog/pkg/ddlog"
	"github.com/antoninbas/antrea-k8s-to-ddlog/pkg/signals"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/component-base/logs"
	"k8s.io/klog"
)

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}

func main() {
	logs.InitLogs()
	defer logs.FlushLogs()

	// ns := &v1.Namespace{
	// 	ObjectMeta: metav1.ObjectMeta{
	// 		Name:   "testNamespace",
	// 		UID:    "testUID",
	// 		Labels: map[string]string{"app": "nginx"},
	// 	},
	// }
	// r := ddlog.RecordNamespace(ns)
	// defer r.Free()
	// fmt.Println(r.Dump())

	recordCommands := flag.String("record-commands", "", "Provide a file name where to record commands sent to DDLog")
	dumpChanges := flag.String("dump-changes", "", "Provide a file name where to dump record changes")

	var kubeconfig *string
	if home := homeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	ddlogProgram, err := ddlog.NewProgram(1, *dumpChanges)
	if err != nil {
		klog.Fatalf("Error when creating DDLog program: %v", err)
	}
	defer func() {
		klog.Infof("Stopping DDLog program")
		if err := ddlogProgram.Stop(); err != nil {
			klog.Errorf("Error when stopping DDLog program: %v", err)
		}
	}()

	if *recordCommands != "" {
		ddlogProgram.StartRecordingCommands(*recordCommands)
	}

	informerFactory := informers.NewSharedInformerFactory(clientset, time.Second*30)
	podInformer := informerFactory.Core().V1().Pods()
	namespaceInformer := informerFactory.Core().V1().Namespaces()
	networkPolicyInformer := informerFactory.Networking().V1().NetworkPolicies()

	c := controller.NewController(
		clientset,
		podInformer,
		namespaceInformer,
		networkPolicyInformer,
		ddlogProgram,
	)

	stopCh := signals.RegisterSignalHandlers()

	informerFactory.Start(stopCh)

	go c.Run(stopCh)

	<-stopCh

	klog.Infof("Exiting")
}
