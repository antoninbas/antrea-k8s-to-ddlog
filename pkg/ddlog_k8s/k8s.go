package ddlog_k8s

import (
	"k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/antoninbas/antrea-k8s-to-ddlog/pkg/ddlog"
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

func RecordUID(UID types.UID) ddlog.Record {
	return ddlog.RecordStructStatic(UIDConstructor, ddlog.RecordString(string(UID)))
}

func RecordLabels(labels map[string]string) ddlog.Record {
	rLabels := ddlog.RecordMap() // empty map
	for k, v := range labels {
		ddlog.RecordMapPush(rLabels, ddlog.RecordString(k), ddlog.RecordString(v))
	}
	return rLabels
}

func RecordNamespace(ns *v1.Namespace) ddlog.Record {
	rName := ddlog.RecordString(ns.Name)
	rUID := RecordUID(ns.UID)
	rLabels := RecordLabels(ns.Labels)
	return ddlog.RecordStructStatic(NamespaceConstructor, rName, rUID, rLabels)
}

func RecordNamespaceKey(namespace string) ddlog.Record {
	return ddlog.RecordString(namespace)
}

func RecordPodSpec(spec *v1.PodSpec) ddlog.Record {
	return ddlog.RecordStructStatic(PodSpecConstructor, ddlog.RecordString(spec.NodeName))
}

func RecordPodStatus(status *v1.PodStatus) ddlog.Record {
	return ddlog.RecordStructStatic(PodStatusConstructor, ddlog.RecordString(status.PodIP))
}

func RecordPod(pod *v1.Pod) ddlog.Record {
	rName := ddlog.RecordString(pod.Name)
	rNamespace := ddlog.RecordString(pod.Namespace)
	rUID := RecordUID(pod.UID)
	rLabels := RecordLabels(pod.Labels)
	rSpec := RecordPodSpec(&pod.Spec)
	rStatus := RecordPodStatus(&pod.Status)
	return ddlog.RecordStructStatic(PodConstructor, rName, rNamespace, rUID, rLabels, rSpec, rStatus)
}

func RecordPodKey(namespace, name string) ddlog.Record {
	rNamespace := ddlog.RecordString(namespace)
	rName := ddlog.RecordString(name)
	return ddlog.RecordPair(rNamespace, rName)
}

func RecordIntOrString(v *intstr.IntOrString) ddlog.Record {
	if v.Type == intstr.Int {
		return ddlog.RecordLeft(ddlog.RecordI32(v.IntVal))
	} else if v.Type == intstr.String {
		return ddlog.RecordRight(ddlog.RecordString(v.StrVal))
	}
	// should not happen
	return ddlog.RecordNull()
}

func RecordLabelSelectorRequirement(req *metav1.LabelSelectorRequirement) ddlog.Record {
	rKey := ddlog.RecordString(req.Key)

	var rOperator ddlog.Record
	switch req.Operator {
	case metav1.LabelSelectorOpIn:
		rOperator = ddlog.RecordStructStatic(LabelSelectorOpInConstructor)
	case metav1.LabelSelectorOpNotIn:
		rOperator = ddlog.RecordStructStatic(LabelSelectorOpNotInConstructor)
	case metav1.LabelSelectorOpExists:
		rOperator = ddlog.RecordStructStatic(LabelSelectorOpExistsConstructor)
	case metav1.LabelSelectorOpDoesNotExist:
		rOperator = ddlog.RecordStructStatic(LabelSelectorOpDoesNotExistConstructor)
	}

	rValues := ddlog.RecordVector()
	for _, value := range req.Values {
		ddlog.RecordVectorPush(rValues, ddlog.RecordString(value))
	}

	return ddlog.RecordStructStatic(LabelSelectorRequirementConstructor, rKey, rOperator, rValues)
}

func RecordLabelSelector(labelSelector *metav1.LabelSelector) ddlog.Record {
	rMatchLabels := RecordLabels(labelSelector.MatchLabels)
	rMatchExpressions := ddlog.RecordVector()
	for _, req := range labelSelector.MatchExpressions {
		ddlog.RecordVectorPush(rMatchExpressions, RecordLabelSelectorRequirement(&req))
	}
	return ddlog.RecordStructStatic(LabelSelectorConstructor, rMatchLabels, rMatchExpressions)
}

func RecordNetworkPolicyPort(policyPort *networkingv1.NetworkPolicyPort) ddlog.Record {
	var rProto ddlog.Record
	if policyPort.Protocol == nil {
		rProto = ddlog.RecordNone()
	} else {
		rProto = ddlog.RecordSome(ddlog.RecordString(string(*policyPort.Protocol)))
	}

	var rPort ddlog.Record
	if policyPort.Port == nil {
		rPort = ddlog.RecordNone()
	} else {
		rPort = ddlog.RecordSome(RecordIntOrString(policyPort.Port))
	}

	return ddlog.RecordStructStatic(NetworkPolicyPortConstructor, rProto, rPort)
}

func RecordIPBlock(ipBlock *networkingv1.IPBlock) ddlog.Record {
	rCIDR := ddlog.RecordString(ipBlock.CIDR)
	rExcept := ddlog.RecordVector()
	for _, except := range ipBlock.Except {
		ddlog.RecordVectorPush(rExcept, ddlog.RecordString(except))
	}
	return ddlog.RecordStructStatic(IPBlockConstructor, rCIDR, rExcept)
}

func RecordNetworkPolicyPeer(policyPeer *networkingv1.NetworkPolicyPeer) ddlog.Record {
	var rPodSelector ddlog.Record
	if policyPeer.PodSelector == nil {
		rPodSelector = ddlog.RecordNone()
	} else {
		rPodSelector = ddlog.RecordSome(RecordLabelSelector(policyPeer.PodSelector))
	}

	var rNamespaceSelector ddlog.Record
	if policyPeer.NamespaceSelector == nil {
		rNamespaceSelector = ddlog.RecordNone()
	} else {
		rNamespaceSelector = ddlog.RecordSome(RecordLabelSelector(policyPeer.NamespaceSelector))
	}

	var rIPBlock ddlog.Record
	if policyPeer.IPBlock == nil {
		rIPBlock = ddlog.RecordNone()
	} else {
		rIPBlock = ddlog.RecordSome(RecordIPBlock(policyPeer.IPBlock))
	}

	return ddlog.RecordStructStatic(NetworkPolicyPeerConstructor, rPodSelector, rNamespaceSelector, rIPBlock)
}

func RecordNetworkPolicyIngressRule(rule *networkingv1.NetworkPolicyIngressRule) ddlog.Record {
	rPorts := ddlog.RecordVector()
	for _, port := range rule.Ports {
		ddlog.RecordVectorPush(rPorts, RecordNetworkPolicyPort(&port))
	}

	rFrom := ddlog.RecordVector()
	for _, from := range rule.From {
		ddlog.RecordVectorPush(rFrom, RecordNetworkPolicyPeer(&from))
	}

	return ddlog.RecordStructStatic(NetworkPolicyIngressRuleConstructor, rPorts, rFrom)
}

func RecordNetworkPolicyEgressRule(rule *networkingv1.NetworkPolicyEgressRule) ddlog.Record {
	rPorts := ddlog.RecordVector()
	for _, port := range rule.Ports {
		ddlog.RecordVectorPush(rPorts, RecordNetworkPolicyPort(&port))
	}

	rTo := ddlog.RecordVector()
	for _, to := range rule.To {
		ddlog.RecordVectorPush(rTo, RecordNetworkPolicyPeer(&to))
	}

	return ddlog.RecordStructStatic(NetworkPolicyEgressRuleConstructor, rPorts, rTo)
}

func RecordNetworkPolicySpec(spec *networkingv1.NetworkPolicySpec) ddlog.Record {
	rPodSelector := RecordLabelSelector(&spec.PodSelector)

	rIngress := ddlog.RecordVector()
	for _, rule := range spec.Ingress {
		ddlog.RecordVectorPush(rIngress, RecordNetworkPolicyIngressRule(&rule))
	}

	rEgress := ddlog.RecordVector()
	for _, rule := range spec.Egress {
		ddlog.RecordVectorPush(rEgress, RecordNetworkPolicyEgressRule(&rule))
	}

	rPolicyTypes := ddlog.RecordVector()
	for _, pType := range spec.PolicyTypes {
		if pType == networkingv1.PolicyTypeIngress {
			ddlog.RecordVectorPush(rPolicyTypes, ddlog.RecordStructStatic(PolicyTypeIngressConstructor))
		} else if pType == networkingv1.PolicyTypeEgress {
			ddlog.RecordVectorPush(rPolicyTypes, ddlog.RecordStructStatic(PolicyTypeEgressConstructor))
		}
	}

	return ddlog.RecordStruct("k8spolicy.NetworkPolicySpec", rPodSelector, rIngress, rEgress, rPolicyTypes)
}

func RecordNetworkPolicy(np *networkingv1.NetworkPolicy) ddlog.Record {
	rName := ddlog.RecordString(np.Name)
	rNamespace := ddlog.RecordString(np.Namespace)
	rUID := RecordUID(np.UID)
	rSpec := RecordNetworkPolicySpec(&np.Spec)
	return ddlog.RecordStructStatic(NetworkPolicyConstructor, rName, rNamespace, rUID, rSpec)
}

func RecordNetworkPolicyKey(namespace, name string) ddlog.Record {
	rNamespace := ddlog.RecordString(namespace)
	rName := ddlog.RecordString(name)
	return ddlog.RecordPair(rNamespace, rName)
}
