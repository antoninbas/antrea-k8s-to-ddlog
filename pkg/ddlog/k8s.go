package ddlog

import (
	"k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
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
	return RecordStruct("k8spolicy.Namespace", rName, rUID, rLabels)
}

func RecordPodSpec(spec *v1.PodSpec) Record {
	return RecordStruct("k8spolicy.PodSpec", RecordString(spec.NodeName))
}

func RecordPodStatus(status *v1.PodStatus) Record {
	return RecordStruct("k8spolicy.PodStatus", RecordString(status.PodIP))
}

func RecordPod(pod *v1.Pod) Record {
	rName := RecordString(pod.Name)
	rNamespace := RecordString(pod.Namespace)
	rUID := RecordUID(pod.UID)
	rLabels := RecordLabels(pod.Labels)
	rSpec := RecordPodSpec(&pod.Spec)
	rStatus := RecordPodStatus(&pod.Status)
	return RecordStruct("k8spolicy.Pod", rName, rNamespace, rUID, rLabels, rSpec, rStatus)
}

func RecordIntOrString(v *intstr.IntOrString) Record {
	if v.Type == intstr.Int {
		return RecordEither(RecordI32(v.IntVal), RecordNull())
	} else if v.Type == intstr.String {
		return RecordEither(RecordNull(), RecordString(v.StrVal))
	}
	// should not happen
	return RecordNull()
}

func RecordLabelSelectorRequirement(req *metav1.LabelSelectorRequirement) Record {
	rKey := RecordString(req.Key)

	var rOperator Record
	switch req.Operator {
	case metav1.LabelSelectorOpIn:
		rOperator = RecordStruct("k8spolicy.LabelSelectorOpIn")
	case metav1.LabelSelectorOpNotIn:
		rOperator = RecordStruct("k8spolicy.LabelSelectorOpNotIn")
	case metav1.LabelSelectorOpExists:
		rOperator = RecordStruct("k8spolicy.LabelSelectorOpExists")
	case metav1.LabelSelectorOpDoesNotExist:
		rOperator = RecordStruct("k8spolicy.LabelSelectorOpDoesNotExist")
	}

	rValues := RecordVector()
	for _, value := range req.Values {
		RecordVectorPush(rValues, RecordString(value))
	}

	return RecordStruct("k8spolicy.LabelSelectorRequirement", rKey, rOperator, rValues)
}

func RecordLabelSelector(labelSelector *metav1.LabelSelector) Record {
	rMatchLabels := RecordLabels(labelSelector.MatchLabels)
	rMatchExpressions := RecordVector()
	for _, req := range labelSelector.MatchExpressions {
		RecordVectorPush(rMatchExpressions, RecordLabelSelectorRequirement(&req))
	}
	return RecordStruct("k8spolicy.LabelSelector", rMatchLabels, rMatchExpressions)
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

	return RecordStruct("k8spolicy.NetworkPolicy", rProto, rPort)
}

func RecordIPBlock(ipBlock *networkingv1.IPBlock) Record {
	rCIDR := RecordString(ipBlock.CIDR)
	rExcept := RecordVector()
	for _, except := range ipBlock.Except {
		RecordVectorPush(rExcept, RecordString(except))
	}
	return RecordStruct("k8spolicy.IPBlock", rCIDR, rExcept)
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

	return RecordStruct("k8spolicies.NetworkPolicyPeer", rPodSelector, rNamespaceSelector, rIPBlock)
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

	return RecordStruct("k8spolicy.NetworkPolicyIngressRule", rPorts, rFrom)
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

	return RecordStruct("k8spolicy.NetworkPolicyEgressRule", rPorts, rTo)
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
			RecordVectorPush(rPolicyTypes, RecordStruct("k8spolicy.PolicyTypeIngress"))
		} else if pType == networkingv1.PolicyTypeEgress {
			RecordVectorPush(rPolicyTypes, RecordStruct("k8spolicy.PolicyTypeEgress"))
		}
	}

	return RecordStruct("k8spolicy.NetworkPolicySpec", rPodSelector, rIngress, rEgress, rPolicyTypes)
}

func RecordNetworkPolicy(np *networkingv1.NetworkPolicy) Record {
	rName := RecordString(np.Name)
	rNamespace := RecordString(np.Namespace)
	rUID := RecordUID(np.UID)
	rSpec := RecordNetworkPolicySpec(&np.Spec)
	return RecordStruct("k8spolicy.NetworkPolicy", rName, rNamespace, rUID, rSpec)
}
