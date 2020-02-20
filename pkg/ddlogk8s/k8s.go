package ddlogk8s

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
	return ddlog.NewRecordStructStatic(UIDConstructor, ddlog.NewRecordString(string(UID)))
}

func RecordLabels(labels map[string]string) ddlog.Record {
	rLabels := ddlog.NewRecordMap() // empty map
	for k, v := range labels {
		// ddlog.NewRecordMapPush(rLabels, ddlog.NewRecordString(k), ddlog.NewRecordString(v))
		rLabels.Push(ddlog.NewRecordString(k), ddlog.NewRecordString(v))
	}
	return rLabels
}

func RecordNamespace(ns *v1.Namespace) ddlog.Record {
	rName := ddlog.NewRecordString(ns.Name)
	rUID := RecordUID(ns.UID)
	rLabels := RecordLabels(ns.Labels)
	return ddlog.NewRecordStructStatic(NamespaceConstructor, rName, rUID, rLabels)
}

func RecordNamespaceKey(namespace string) ddlog.Record {
	return ddlog.NewRecordString(namespace)
}

func RecordPodSpec(spec *v1.PodSpec) ddlog.Record {
	return ddlog.NewRecordStructStatic(PodSpecConstructor, ddlog.NewRecordString(spec.NodeName))
}

func RecordPodStatus(status *v1.PodStatus) ddlog.Record {
	return ddlog.NewRecordStructStatic(PodStatusConstructor, ddlog.NewRecordString(status.PodIP))
}

func RecordPod(pod *v1.Pod) ddlog.Record {
	rName := ddlog.NewRecordString(pod.Name)
	rNamespace := ddlog.NewRecordString(pod.Namespace)
	rUID := RecordUID(pod.UID)
	rLabels := RecordLabels(pod.Labels)
	rSpec := RecordPodSpec(&pod.Spec)
	rStatus := RecordPodStatus(&pod.Status)
	return ddlog.NewRecordStructStatic(PodConstructor, rName, rNamespace, rUID, rLabels, rSpec, rStatus)
}

func RecordPodKey(namespace, name string) ddlog.Record {
	rNamespace := ddlog.NewRecordString(namespace)
	rName := ddlog.NewRecordString(name)
	return ddlog.NewRecordPair(rNamespace, rName)
}

func RecordIntOrString(v *intstr.IntOrString) ddlog.Record {
	if v.Type == intstr.Int {
		return ddlog.NewRecordLeft(ddlog.NewRecordI32(v.IntVal))
	} else if v.Type == intstr.String {
		return ddlog.NewRecordRight(ddlog.NewRecordString(v.StrVal))
	}
	// should not happen
	return ddlog.NewRecordNull()
}

func RecordLabelSelectorRequirement(req *metav1.LabelSelectorRequirement) ddlog.Record {
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

func RecordLabelSelector(labelSelector *metav1.LabelSelector) ddlog.Record {
	rMatchLabels := RecordLabels(labelSelector.MatchLabels)
	rMatchExpressions := ddlog.NewRecordVector()
	for _, req := range labelSelector.MatchExpressions {
		rMatchExpressions.Push(RecordLabelSelectorRequirement(&req))
	}
	return ddlog.NewRecordStructStatic(LabelSelectorConstructor, rMatchLabels, rMatchExpressions)
}

func RecordNetworkPolicyPort(policyPort *networkingv1.NetworkPolicyPort) ddlog.Record {
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
		rPort = ddlog.NewRecordSome(RecordIntOrString(policyPort.Port))
	}

	return ddlog.NewRecordStructStatic(NetworkPolicyPortConstructor, rProto, rPort)
}

func RecordIPBlock(ipBlock *networkingv1.IPBlock) ddlog.Record {
	rCIDR := ddlog.NewRecordString(ipBlock.CIDR)
	rExcept := ddlog.NewRecordVector()
	for _, except := range ipBlock.Except {
		rExcept.Push(ddlog.NewRecordString(except))
	}
	return ddlog.NewRecordStructStatic(IPBlockConstructor, rCIDR, rExcept)
}

func RecordNetworkPolicyPeer(policyPeer *networkingv1.NetworkPolicyPeer) ddlog.Record {
	var rPodSelector ddlog.Record
	if policyPeer.PodSelector == nil {
		rPodSelector = ddlog.NewRecordNone()
	} else {
		rPodSelector = ddlog.NewRecordSome(RecordLabelSelector(policyPeer.PodSelector))
	}

	var rNamespaceSelector ddlog.Record
	if policyPeer.NamespaceSelector == nil {
		rNamespaceSelector = ddlog.NewRecordNone()
	} else {
		rNamespaceSelector = ddlog.NewRecordSome(RecordLabelSelector(policyPeer.NamespaceSelector))
	}

	var rIPBlock ddlog.Record
	if policyPeer.IPBlock == nil {
		rIPBlock = ddlog.NewRecordNone()
	} else {
		rIPBlock = ddlog.NewRecordSome(RecordIPBlock(policyPeer.IPBlock))
	}

	return ddlog.NewRecordStructStatic(NetworkPolicyPeerConstructor, rPodSelector, rNamespaceSelector, rIPBlock)
}

func RecordNetworkPolicyIngressRule(rule *networkingv1.NetworkPolicyIngressRule) ddlog.Record {
	rPorts := ddlog.NewRecordVector()
	for _, port := range rule.Ports {
		rPorts.Push(RecordNetworkPolicyPort(&port))
	}

	rFrom := ddlog.NewRecordVector()
	for _, from := range rule.From {
		rFrom.Push(RecordNetworkPolicyPeer(&from))
	}

	return ddlog.NewRecordStructStatic(NetworkPolicyIngressRuleConstructor, rPorts, rFrom)
}

func RecordNetworkPolicyEgressRule(rule *networkingv1.NetworkPolicyEgressRule) ddlog.Record {
	rPorts := ddlog.NewRecordVector()
	for _, port := range rule.Ports {
		rPorts.Push(RecordNetworkPolicyPort(&port))
	}

	rTo := ddlog.NewRecordVector()
	for _, to := range rule.To {
		rTo.Push(RecordNetworkPolicyPeer(&to))
	}

	return ddlog.NewRecordStructStatic(NetworkPolicyEgressRuleConstructor, rPorts, rTo)
}

func RecordNetworkPolicySpec(spec *networkingv1.NetworkPolicySpec) ddlog.Record {
	rPodSelector := RecordLabelSelector(&spec.PodSelector)

	rIngress := ddlog.NewRecordVector()
	for _, rule := range spec.Ingress {
		rIngress.Push(RecordNetworkPolicyIngressRule(&rule))
	}

	rEgress := ddlog.NewRecordVector()
	for _, rule := range spec.Egress {
		rEgress.Push(RecordNetworkPolicyEgressRule(&rule))
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

func RecordNetworkPolicy(np *networkingv1.NetworkPolicy) ddlog.Record {
	rName := ddlog.NewRecordString(np.Name)
	rNamespace := ddlog.NewRecordString(np.Namespace)
	rUID := RecordUID(np.UID)
	rSpec := RecordNetworkPolicySpec(&np.Spec)
	return ddlog.NewRecordStructStatic(NetworkPolicyConstructor, rName, rNamespace, rUID, rSpec)
}

func RecordNetworkPolicyKey(namespace, name string) ddlog.Record {
	rNamespace := ddlog.NewRecordString(namespace)
	rName := ddlog.NewRecordString(name)
	return ddlog.NewRecordPair(rNamespace, rName)
}
