package ddlogk8s

import (
	"k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/vmware/differential-datalog/go/pkg/ddlog"
)

var (
	PodTableID           = ddlog.GetTableID("k8spolicy.Pod")
	NamespaceTableID     = ddlog.GetTableID("k8spolicy.Namespace")
	NetworkPolicyTableID = ddlog.GetTableID("k8spolicy.NetworkPolicy")

	// these are never freed
	NetworkPolicyConstructor               = ddlog.NewCString("k8spolicy.NetworkPolicy")
	NetworkPolicyPortConstructor           = ddlog.NewCString("k8spolicy.NetworkPolicyPort")
	NetworkPolicyPeerConstructor           = ddlog.NewCString("k8spolicy.NetworkPolicyPeer")
	NetworkPolicyIngressRuleConstructor    = ddlog.NewCString("k8spolicy.NetworkPolicyIngressRule")
	NetworkPolicyEgressRuleConstructor     = ddlog.NewCString("k8spolicy.NetworkPolicyEgressRule")
	PolicyTypeIngressConstructor           = ddlog.NewCString("k8spolicy.PolicyTypeIngress")
	PolicyTypeEgressConstructor            = ddlog.NewCString("k8spolicy.PolicyTypeEgress")
	NamespaceConstructor                   = ddlog.NewCString("k8spolicy.Namespace")
	PodSpecConstructor                     = ddlog.NewCString("k8spolicy.PodSpec")
	PodStatusConstructor                   = ddlog.NewCString("k8spolicy.PodStatus")
	PodConstructor                         = ddlog.NewCString("k8spolicy.Pod")
	LabelSelectorOpInConstructor           = ddlog.NewCString("k8spolicy.LabelSelectorOpIn")
	LabelSelectorOpNotInConstructor        = ddlog.NewCString("k8spolicy.LabelSelectorOpNotIn")
	LabelSelectorOpExistsConstructor       = ddlog.NewCString("k8spolicy.LabelSelectorOpExists")
	LabelSelectorOpDoesNotExistConstructor = ddlog.NewCString("k8spolicy.LabelSelectorOpDoesNotExist")
	LabelSelectorRequirementConstructor    = ddlog.NewCString("k8spolicy.LabelSelectorRequirementConstructor")
	LabelSelectorConstructor               = ddlog.NewCString("k8spolicy.LabelSelector")
	IPBlockConstructor                     = ddlog.NewCString("k8spolicy.IPBlock")
	UIDConstructor                         = ddlog.NewCString("k8spolicy.UID")
)

func NewRecordUID(UID types.UID) ddlog.Record {
	return ddlog.NewRecordStructStatic(UIDConstructor, ddlog.NewRecordString(string(UID)))
}

func NewRecordLabels(labels map[string]string) ddlog.Record {
	rLabels := ddlog.NewRecordMap() // empty map
	for k, v := range labels {
		rLabels.Push(ddlog.NewRecordString(k), ddlog.NewRecordString(v))
	}
	return rLabels
}

func NewRecordNamespace(ns *v1.Namespace) ddlog.Record {
	rName := ddlog.NewRecordString(ns.Name)
	rUID := NewRecordUID(ns.UID)
	rLabels := NewRecordLabels(ns.Labels)
	return ddlog.NewRecordStructStatic(NamespaceConstructor, rName, rUID, rLabels)
}

func NewRecordNamespaceKey(namespace string) ddlog.Record {
	return ddlog.NewRecordString(namespace)
}

func NewRecordPodSpec(spec *v1.PodSpec) ddlog.Record {
	return ddlog.NewRecordStructStatic(PodSpecConstructor, ddlog.NewRecordString(spec.NodeName))
}

func NewRecordPodStatus(status *v1.PodStatus) ddlog.Record {
	return ddlog.NewRecordStructStatic(PodStatusConstructor, ddlog.NewRecordString(status.PodIP))
}

func NewRecordPod(pod *v1.Pod) ddlog.Record {
	rName := ddlog.NewRecordString(pod.Name)
	rNamespace := ddlog.NewRecordString(pod.Namespace)
	rUID := NewRecordUID(pod.UID)
	rLabels := NewRecordLabels(pod.Labels)
	rSpec := NewRecordPodSpec(&pod.Spec)
	rStatus := NewRecordPodStatus(&pod.Status)
	return ddlog.NewRecordStructStatic(PodConstructor, rName, rNamespace, rUID, rLabels, rSpec, rStatus)
}

func NewRecordPodKey(namespace, name string) ddlog.Record {
	rNamespace := ddlog.NewRecordString(namespace)
	rName := ddlog.NewRecordString(name)
	return ddlog.NewRecordPair(rNamespace, rName)
}

func NewRecordIntOrString(v *intstr.IntOrString) ddlog.Record {
	if v.Type == intstr.Int {
		return ddlog.NewRecordLeft(ddlog.NewRecordI32(v.IntVal))
	} else if v.Type == intstr.String {
		return ddlog.NewRecordRight(ddlog.NewRecordString(v.StrVal))
	}
	// should not happen
	return ddlog.NewRecordNull()
}

func NewRecordLabelSelectorRequirement(req *metav1.LabelSelectorRequirement) ddlog.Record {
	rKey := ddlog.NewRecordString(req.Key)

	var rOperator ddlog.Record
	switch req.Operator {
	case metav1.LabelSelectorOpIn:
		rOperator = ddlog.NewRecordStructStatic(LabelSelectorOpInConstructor)
	case metav1.LabelSelectorOpNotIn:
		rOperator = ddlog.NewRecordStructStatic(LabelSelectorOpNotInConstructor)
	case metav1.LabelSelectorOpExists:
		rOperator = ddlog.NewRecordStructStatic(LabelSelectorOpExistsConstructor)
	case metav1.LabelSelectorOpDoesNotExist:
		rOperator = ddlog.NewRecordStructStatic(LabelSelectorOpDoesNotExistConstructor)
	}

	rValues := ddlog.NewRecordVector()
	for _, value := range req.Values {
		rValues.Push(ddlog.NewRecordString(value))
	}

	return ddlog.NewRecordStructStatic(LabelSelectorRequirementConstructor, rKey, rOperator, rValues)
}

func NewRecordLabelSelector(labelSelector *metav1.LabelSelector) ddlog.Record {
	rMatchLabels := NewRecordLabels(labelSelector.MatchLabels)
	rMatchExpressions := ddlog.NewRecordVector()
	for _, req := range labelSelector.MatchExpressions {
		rMatchExpressions.Push(NewRecordLabelSelectorRequirement(&req))
	}
	return ddlog.NewRecordStructStatic(LabelSelectorConstructor, rMatchLabels, rMatchExpressions)
}

func NewRecordNetworkPolicyPort(policyPort *networkingv1.NetworkPolicyPort) ddlog.Record {
	var rProto ddlog.Record
	if policyPort.Protocol == nil {
		rProto = ddlog.NewRecordNone()
	} else {
		rProto = ddlog.NewRecordSome(ddlog.NewRecordString(string(*policyPort.Protocol)))
	}

	var rPort ddlog.Record
	if policyPort.Port == nil {
		rPort = ddlog.NewRecordNone()
	} else {
		rPort = ddlog.NewRecordSome(NewRecordIntOrString(policyPort.Port))
	}

	return ddlog.NewRecordStructStatic(NetworkPolicyPortConstructor, rProto, rPort)
}

func NewRecordIPBlock(ipBlock *networkingv1.IPBlock) ddlog.Record {
	rCIDR := ddlog.NewRecordString(ipBlock.CIDR)
	rExcept := ddlog.NewRecordVector()
	for _, except := range ipBlock.Except {
		rExcept.Push(ddlog.NewRecordString(except))
	}
	return ddlog.NewRecordStructStatic(IPBlockConstructor, rCIDR, rExcept)
}

func NewRecordNetworkPolicyPeer(policyPeer *networkingv1.NetworkPolicyPeer) ddlog.Record {
	var rPodSelector ddlog.Record
	if policyPeer.PodSelector == nil {
		rPodSelector = ddlog.NewRecordNone()
	} else {
		rPodSelector = ddlog.NewRecordSome(NewRecordLabelSelector(policyPeer.PodSelector))
	}

	var rNamespaceSelector ddlog.Record
	if policyPeer.NamespaceSelector == nil {
		rNamespaceSelector = ddlog.NewRecordNone()
	} else {
		rNamespaceSelector = ddlog.NewRecordSome(NewRecordLabelSelector(policyPeer.NamespaceSelector))
	}

	var rIPBlock ddlog.Record
	if policyPeer.IPBlock == nil {
		rIPBlock = ddlog.NewRecordNone()
	} else {
		rIPBlock = ddlog.NewRecordSome(NewRecordIPBlock(policyPeer.IPBlock))
	}

	return ddlog.NewRecordStructStatic(NetworkPolicyPeerConstructor, rPodSelector, rNamespaceSelector, rIPBlock)
}

func NewRecordNetworkPolicyIngressRule(rule *networkingv1.NetworkPolicyIngressRule) ddlog.Record {
	rPorts := ddlog.NewRecordVector()
	for _, port := range rule.Ports {
		rPorts.Push(NewRecordNetworkPolicyPort(&port))
	}

	rFrom := ddlog.NewRecordVector()
	for _, from := range rule.From {
		rFrom.Push(NewRecordNetworkPolicyPeer(&from))
	}

	return ddlog.NewRecordStructStatic(NetworkPolicyIngressRuleConstructor, rPorts, rFrom)
}

func NewRecordNetworkPolicyEgressRule(rule *networkingv1.NetworkPolicyEgressRule) ddlog.Record {
	rPorts := ddlog.NewRecordVector()
	for _, port := range rule.Ports {
		rPorts.Push(NewRecordNetworkPolicyPort(&port))
	}

	rTo := ddlog.NewRecordVector()
	for _, to := range rule.To {
		rTo.Push(NewRecordNetworkPolicyPeer(&to))
	}

	return ddlog.NewRecordStructStatic(NetworkPolicyEgressRuleConstructor, rPorts, rTo)
}

func NewRecordNetworkPolicySpec(spec *networkingv1.NetworkPolicySpec) ddlog.Record {
	rPodSelector := NewRecordLabelSelector(&spec.PodSelector)

	rIngress := ddlog.NewRecordVector()
	for _, rule := range spec.Ingress {
		rIngress.Push(NewRecordNetworkPolicyIngressRule(&rule))
	}

	rEgress := ddlog.NewRecordVector()
	for _, rule := range spec.Egress {
		rEgress.Push(NewRecordNetworkPolicyEgressRule(&rule))
	}

	rPolicyTypes := ddlog.NewRecordVector()
	for _, pType := range spec.PolicyTypes {
		if pType == networkingv1.PolicyTypeIngress {
			rPolicyTypes.Push(ddlog.NewRecordStructStatic(PolicyTypeIngressConstructor))
		} else if pType == networkingv1.PolicyTypeEgress {
			rPolicyTypes.Push(ddlog.NewRecordStructStatic(PolicyTypeEgressConstructor))
		}
	}

	return ddlog.NewRecordStruct("k8spolicy.NetworkPolicySpec", rPodSelector, rIngress, rEgress, rPolicyTypes)
}

func NewRecordNetworkPolicy(np *networkingv1.NetworkPolicy) ddlog.Record {
	rName := ddlog.NewRecordString(np.Name)
	rNamespace := ddlog.NewRecordString(np.Namespace)
	rUID := NewRecordUID(np.UID)
	rSpec := NewRecordNetworkPolicySpec(&np.Spec)
	return ddlog.NewRecordStructStatic(NetworkPolicyConstructor, rName, rNamespace, rUID, rSpec)
}

func NewRecordNetworkPolicyKey(namespace, name string) ddlog.Record {
	rNamespace := ddlog.NewRecordString(namespace)
	rName := ddlog.NewRecordString(name)
	return ddlog.NewRecordPair(rNamespace, rName)
}
