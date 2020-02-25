package ddlogk8s

import (
	"testing"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func BenchmarkRecordNamespace(b *testing.B) {
	ns := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "testNamespace",
			UID:    "testUID",
			Labels: map[string]string{"app": "nginx"},
		},
	}
	for i := 0; i < b.N; i++ {
		r := NewRecordNamespace(ns)
		// r.Dump()
		r.Free()
	}
}
