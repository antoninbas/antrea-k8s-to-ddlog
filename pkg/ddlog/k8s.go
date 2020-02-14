package ddlog

import (
	"k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

var (
	PodTableID           = GetTableID("k8spolicy.Pod")
	NamespaceTableID     = GetTableID("k8spolicy.Namespace")
	NetworkPolicyTableID = GetTableID("k8spolicy.NetworkPolicy")

	// these are never freed
	NetworkPolicyConstructor               = NewCString("k8spolicy.NetworkPolicy")
	NetworkPolicyPortConstructor           = NewCString("k8spolicy.NetworkPolicyPort")
	NetworkPolicyPeerConstructor           = NewCString("k8spolicy.NetworkPolicyPeer")
	NetworkPolicyIngressRuleConstructor    = NewCString("k8spolicy.NetworkPolicyIngressRule")
	NetworkPolicyEgressRuleConstructor     = NewCString("k8spolicy.NetworkPolicyEgressRule")
	PolicyTypeIngressConstructor           = NewCString("k8spolicy.PolicyTypeIngress")
	PolicyTypeEgressConstructor            = NewCString("k8spolicy.PolicyTypeEgress")
	NamespaceConstructor                   = NewCString("k8spolicy.Namespace")
	PodSpecConstructor                     = NewCString("k8spolicy.PodSpec")
	PodStatusConstructor                   = NewCString("k8spolicy.PodStatus")
	PodConstructor                         = NewCString("k8spolicy.Pod")
	LabelSelectorOpInConstructor           = NewCString("k8spolicy.LabelSelectorOpIn")
	LabelSelectorOpNotInConstructor        = NewCString("k8spolicy.LabelSelectorOpNotIn")
	LabelSelectorOpExistsConstructor       = NewCString("k8spolicy.LabelSelectorOpExists")
	LabelSelectorOpDoesNotExistConstructor = NewCString("k8spolicy.LabelSelectorOpDoesNotExist")
	LabelSelectorRequirementConstructor    = NewCString("k8spolicy.LabelSelectorRequirementConstructor")
	LabelSelectorConstructor               = NewCString("k8spolicy.LabelSelector")
	IPBlockConstructor                     = NewCString("k8spolicy.IPBlock")
)

func RecordUID(UID types.UID) Record {
	return RecordStruct("k8spolicy.UID", RecordString(string(UID)))
}

func RecordLabels(labels map[string]string) Record {
	rLabels := RecordMap() // empty map
	for k, v := range labels {
		RecordMapPush(rLabels, RecordString(k), RecordString(v))
	}
	return rLabels
}

func RecordNamespace(ns *v1.Namespace) Record {
	rName := RecordString(ns.Name)
	rUID := RecordUID(ns.UID)
	rLabels := RecordLabels(ns.Labels)
	return RecordStructStatic(NamespaceConstructor, rName, rUID, rLabels)
}

func RecordNamespaceKey(namespace string) Record {
	return RecordString(namespace)
}

func RecordPodSpec(spec *v1.PodSpec) Record {
	return RecordStructStatic(PodSpecConstructor, RecordString(spec.NodeName))
}

func RecordPodStatus(status *v1.PodStatus) Record {
	return RecordStructStatic(PodStatusConstructor, RecordString(status.PodIP))
}

func RecordPod(pod *v1.Pod) Record {
	rName := RecordString(pod.Name)
	rNamespace := RecordString(pod.Namespace)
	rUID := RecordUID(pod.UID)
	rLabels := RecordLabels(pod.Labels)
	rSpec := RecordPodSpec(&pod.Spec)
	rStatus := RecordPodStatus(&pod.Status)
	return RecordStructStatic(PodConstructor, rName, rNamespace, rUID, rLabels, rSpec, rStatus)
}

func RecordPodKey(namespace, name string) Record {
	rNamespace := RecordString(namespace)
	rName := RecordString(name)
	return RecordPair(rNamespace, rName)
}

func RecordIntOrString(v *intstr.IntOrString) Record {
	if v.Type == intstr.Int {
		return RecordLeft(RecordI32(v.IntVal))
	} else if v.Type == intstr.String {
		return RecordRight(RecordString(v.StrVal))
	}
	// should not happen
	return RecordNull()
}

func RecordLabelSelectorRequirement(req *metav1.LabelSelectorRequirement) Record {
	rKey := RecordString(req.Key)

	var rOperator Record
	switch req.Operator {
	case metav1.LabelSelectorOpIn:
		rOperator = RecordStructStatic(LabelSelectorOpInConstructor)
	case metav1.LabelSelectorOpNotIn:
		rOperator = RecordStructStatic(LabelSelectorOpNotInConstructor)
	case metav1.LabelSelectorOpExists:
		rOperator = RecordStructStatic(LabelSelectorOpExistsConstructor)
	case metav1.LabelSelectorOpDoesNotExist:
		rOperator = RecordStructStatic(LabelSelectorOpDoesNotExistConstructor)
	}

	rValues := RecordVector()
	for _, value := range req.Values {
		RecordVectorPush(rValues, RecordString(value))
	}

	return RecordStructStatic(LabelSelectorRequirementConstructor, rKey, rOperator, rValues)
}

func RecordLabelSelector(labelSelector *metav1.LabelSelector) Record {
	rMatchLabels := RecordLabels(labelSelector.MatchLabels)
	rMatchExpressions := RecordVector()
	for _, req := range labelSelector.MatchExpressions {
		RecordVectorPush(rMatchExpressions, RecordLabelSelectorRequirement(&req))
	}
	return RecordStructStatic(LabelSelectorConstructor, rMatchLabels, rMatchExpressions)
}

func RecordNetworkPolicyPort(policyPort *networkingv1.NetworkPolicyPort) Record {
	var rProto Record
	if policyPort.Protocol == nil {
		rProto = RecordNone()
	} else {
		rProto = RecordSome(RecordString(string(*policyPort.Protocol)))
	}

	var rPort Record
	if policyPort.Port == nil {
		rPort = RecordNone()
	} else {
		rPort = RecordSome(RecordIntOrString(policyPort.Port))
	}

	return RecordStructStatic(NetworkPolicyPortConstructor, rProto, rPort)
}

func RecordIPBlock(ipBlock *networkingv1.IPBlock) Record {
	rCIDR := RecordString(ipBlock.CIDR)
	rExcept := RecordVector()
	for _, except := range ipBlock.Except {
		RecordVectorPush(rExcept, RecordString(except))
	}
	return RecordStructStatic(IPBlockConstructor, rCIDR, rExcept)
}

func RecordNetworkPolicyPeer(policyPeer *networkingv1.NetworkPolicyPeer) Record {
	var rPodSelector Record
	if policyPeer.PodSelector == nil {
		rPodSelector = RecordNone()
	} else {
		rPodSelector = RecordSome(RecordLabelSelector(policyPeer.PodSelector))
	}

	var rNamespaceSelector Record
	if policyPeer.NamespaceSelector == nil {
		rNamespaceSelector = RecordNone()
	} else {
		rNamespaceSelector = RecordSome(RecordLabelSelector(policyPeer.NamespaceSelector))
	}

	var rIPBlock Record
	if policyPeer.IPBlock == nil {
		rIPBlock = RecordNone()
	} else {
		rIPBlock = RecordSome(RecordIPBlock(policyPeer.IPBlock))
	}

	return RecordStructStatic(NetworkPolicyPeerConstructor, rPodSelector, rNamespaceSelector, rIPBlock)
}

func RecordNetworkPolicyIngressRule(rule *networkingv1.NetworkPolicyIngressRule) Record {
	rPorts := RecordVector()
	for _, port := range rule.Ports {
		RecordVectorPush(rPorts, RecordNetworkPolicyPort(&port))
	}

	rFrom := RecordVector()
	for _, from := range rule.From {
		RecordVectorPush(rFrom, RecordNetworkPolicyPeer(&from))
	}

	return RecordStructStatic(NetworkPolicyIngressRuleConstructor, rPorts, rFrom)
}

func RecordNetworkPolicyEgressRule(rule *networkingv1.NetworkPolicyEgressRule) Record {
	rPorts := RecordVector()
	for _, port := range rule.Ports {
		RecordVectorPush(rPorts, RecordNetworkPolicyPort(&port))
	}

	rTo := RecordVector()
	for _, to := range rule.To {
		RecordVectorPush(rTo, RecordNetworkPolicyPeer(&to))
	}

	return RecordStructStatic(NetworkPolicyEgressRuleConstructor, rPorts, rTo)
}

func RecordNetworkPolicySpec(spec *networkingv1.NetworkPolicySpec) Record {
	rPodSelector := RecordLabelSelector(&spec.PodSelector)

	rIngress := RecordVector()
	for _, rule := range spec.Ingress {
		RecordVectorPush(rIngress, RecordNetworkPolicyIngressRule(&rule))
	}

	rEgress := RecordVector()
	for _, rule := range spec.Egress {
		RecordVectorPush(rEgress, RecordNetworkPolicyEgressRule(&rule))
	}

	rPolicyTypes := RecordVector()
	for _, pType := range spec.PolicyTypes {
		if pType == networkingv1.PolicyTypeIngress {
			RecordVectorPush(rPolicyTypes, RecordStructStatic(PolicyTypeIngressConstructor))
		} else if pType == networkingv1.PolicyTypeEgress {
			RecordVectorPush(rPolicyTypes, RecordStructStatic(PolicyTypeEgressConstructor))
		}
	}

	return RecordStruct("k8spolicy.NetworkPolicySpec", rPodSelector, rIngress, rEgress, rPolicyTypes)
}

func RecordNetworkPolicy(np *networkingv1.NetworkPolicy) Record {
	rName := RecordString(np.Name)
	rNamespace := RecordString(np.Namespace)
	rUID := RecordUID(np.UID)
	rSpec := RecordNetworkPolicySpec(&np.Spec)
	return RecordStructStatic(NetworkPolicyConstructor, rName, rNamespace, rUID, rSpec)
}

func RecordNetworkPolicyKey(namespace, name string) Record {
	rNamespace := RecordString(namespace)
	rName := RecordString(name)
	return RecordPair(rNamespace, rName)
}
