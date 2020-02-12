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
	"fmt"

	"github.com/antoninbas/antrea-k8s-to-ddlog/pkg/ddlog"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func test() {
	name := "k8spolicy.Pod"
	id := ddlog.GetTableId(name)
	fmt.Printf("Id for table '%s' is: %d\n", name, id)
}

func main() {
	test()

	ns := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "testNamespace",
			UID:    "testUID",
			Labels: map[string]string{"app": "nginx"},
		},
	}
	r := ddlog.RecordNamespace(ns)
	defer r.Free()
	fmt.Println(r.Dump())
}
