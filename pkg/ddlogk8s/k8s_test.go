package ddlogk8s

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/vmware/differential-datalog/go/pkg/ddlog"
)

func TestOutputTables(t *testing.T) {
	assert.Equal(t, "AppliedToGroup", ddlog.GetTableName(AppliedToGroupTableID))
}

func TestRecordNamespace(t *testing.T) {
	ns := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "testNamespace",
			UID:    "testNamespaceUID",
			Labels: map[string]string{"app": "nginx"},
		},
	}

	r := NewRecordNamespace(ns)
	defer r.Free()
	ns2, err := RecordToNamespace(r)
	assert.Nil(t, err)
	assert.Equal(t, ns.String(), ns2.String())
}

func TestPod(t *testing.T) {
	p := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testPod",
			Namespace: "testNamespace",
			UID:       "testPodUID",
			Labels:    map[string]string{"app": "nginx", "priority": "99"},
		},
		Spec: v1.PodSpec{
			NodeName: "node-1",
		},
		Status: v1.PodStatus{
			PodIP: "10.10.0.1",
		},
	}

	r := NewRecordPod(p)
	defer r.Free()
	p2, err := RecordToPod(r)
	assert.Nil(t, err)
	assert.Equal(t, p.String(), p2.String())
}

// TODO: add more tests to cover different NP specification types
func TestNetworkPolicy(t *testing.T) {
	newProtocol := func(protocol v1.Protocol) *v1.Protocol {
		return &protocol
	}
	newPort := func(port int) *intstr.IntOrString {
		p := intstr.FromInt(port)
		return &p
	}
	// spec:
	//   podSelector:
	//     matchLabels:
	//       app: web-server
	//   policyTypes:
	//   - Ingress
	//   ingress:
	//   - from:
	//     - podSelector:
	//         matchLabels:
	//           app: web-client
	//     ports:
	//     - protocol: TCP
	//       port: 80

	np := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testNetworkPolicy",
			Namespace: "testNamespace",
			UID:       "testNetworkPolicyUID",
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "web-server"},
			},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{
				networkingv1.NetworkPolicyIngressRule{
					Ports: []networkingv1.NetworkPolicyPort{
						networkingv1.NetworkPolicyPort{
							Protocol: newProtocol(v1.ProtocolTCP),
							Port:     newPort(80),
						},
					},
					From: []networkingv1.NetworkPolicyPeer{
						networkingv1.NetworkPolicyPeer{
							PodSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{"app": "web-client"},
							},
						},
					},
				},
			},
		},
	}

	r := NewRecordNetworkPolicy(np)
	defer r.Free()
	np2, err := RecordToNetworkPolicy(r)
	assert.Nil(t, err)
	assert.Equal(t, np.String(), np2.String())
}

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
