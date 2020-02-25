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

	AppliedToGroupTableID           = ddlog.GetTableID("AppliedToGroup")
	AppliedToGroupPodsByNodeTableID = ddlog.GetTableID("AppliedToGroupPodsByNode")
	AppliedToGroupSpanTableID       = ddlog.GetTableID("AppliedToGroupSpan")

	AddressGroupTableID        = ddlog.GetTableID("AddressGroup")
	AddressGroupAddressTableID = ddlog.GetTableID("AddressGroupAddress")
	AddressGroupSpanTableID    = ddlog.GetTableID("AddressGroupSpan")

	NetworkPolicyOutTableID     = ddlog.GetTableID("NetworkPolicy")
	NetworkPolicyOutSpanTableID = ddlog.GetTableID("NetworkPolicySpan")

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

func RecordToUID(record ddlog.Record) types.UID {
	return types.UID(record.AsStruct().At(0).ToString())
}

func NewRecordLabels(labels map[string]string) ddlog.Record {
	rLabels := ddlog.NewRecordMap() // empty map
	for k, v := range labels {
		rLabels.Push(ddlog.NewRecordString(k), ddlog.NewRecordString(v))
	}
	return rLabels
}

func RecordToLabels(record ddlog.Record) map[string]string {
	rLabels := record.AsMap()
	labels := make(map[string]string, rLabels.Size())
	for i := 0; i < rLabels.Size(); i++ {
		rKey, rValue := rLabels.At(i)
		labels[rKey.ToString()] = rValue.ToString()
	}
	return labels
}

func NewRecordNamespace(ns *v1.Namespace) ddlog.Record {
	rName := ddlog.NewRecordString(ns.Name)
	rUID := NewRecordUID(ns.UID)
	rLabels := NewRecordLabels(ns.Labels)
	return ddlog.NewRecordStructStatic(NamespaceConstructor, rName, rUID, rLabels)
}

func RecordToNamespace(record ddlog.Record) (*v1.Namespace, error) {
	rNamespace, err := record.AsStructSafe()
	if err != nil {
		return nil, err
	}
	rName := rNamespace.At(0)
	rUID := rNamespace.At(1)
	rLabels := rNamespace.At(2)
	return &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   rName.ToString(),
			UID:    RecordToUID(rUID),
			Labels: RecordToLabels(rLabels),
		},
	}, err
}

func NewRecordNamespaceKey(namespace string) ddlog.Record {
	return ddlog.NewRecordString(namespace)
}

func NewRecordPodSpec(spec *v1.PodSpec) ddlog.Record {
	return ddlog.NewRecordStructStatic(PodSpecConstructor, ddlog.NewRecordString(spec.NodeName))
}

func RecordToPodSpec(record ddlog.Record) *v1.PodSpec {
	r := record.AsStruct()
	rNodeName := r.At(0)
	return &v1.PodSpec{
		NodeName: rNodeName.ToString(),
	}
}

func NewRecordPodStatus(status *v1.PodStatus) ddlog.Record {
	return ddlog.NewRecordStructStatic(PodStatusConstructor, ddlog.NewRecordString(status.PodIP))
}

func RecordToPodStatus(record ddlog.Record) *v1.PodStatus {
	r := record.AsStruct()
	rPodIP := r.At(0)
	return &v1.PodStatus{
		PodIP: rPodIP.ToString(),
	}
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

func RecordToPod(record ddlog.Record) (*v1.Pod, error) {
	rPod, err := record.AsStructSafe()
	if err != nil {
		return nil, err
	}
	rName := rPod.At(0)
	rNamespace := rPod.At(1)
	rUID := rPod.At(2)
	rLabels := rPod.At(3)
	rSpec := rPod.At(4)
	rStatus := rPod.At(5)
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rName.ToString(),
			Namespace: rNamespace.ToString(),
			UID:       RecordToUID(rUID),
			Labels:    RecordToLabels(rLabels),
		},
		Spec:   *RecordToPodSpec(rSpec),
		Status: *RecordToPodStatus(rStatus),
	}, nil
}

func NewRecordPodKey(namespace, name string) ddlog.Record {
	rNamespace := ddlog.NewRecordString(namespace)
	rName := ddlog.NewRecordString(name)
	return ddlog.NewRecordPair(rNamespace, rName)
}

func NewRecordIntOrString(v *intstr.IntOrString) ddlog.Record {
	switch v.Type {
	case intstr.Int:
		return ddlog.NewRecordLeft(ddlog.NewRecordI32(v.IntVal))
	case intstr.String:
		return ddlog.NewRecordRight(ddlog.NewRecordString(v.StrVal))
	}
	// should not happen
	return ddlog.NewRecordNull()
}

func RecordToIntOrString(record ddlog.Record) intstr.IntOrString {
	r := record.AsStruct()
	switch r.Name() {
	case "std.Left":
		return intstr.FromInt(int(r.At(0).ToI32()))
	case "std.Right":
		return intstr.FromString(r.At(0).ToString())
	}
	return intstr.FromInt(0)
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

func RecordToLabelSelectorRequirement(record ddlog.Record) *metav1.LabelSelectorRequirement {
	r := record.AsStruct()
	rKey := r.At(0)
	rOperator := r.At(1).AsStruct()
	rValues := r.At(2).AsVector()

	var operator metav1.LabelSelectorOperator
	switch rOperator.Name() {
	case "k8spolicy.LabelSelectorOpIn":
		operator = metav1.LabelSelectorOpIn
	case "k8spolicy.LabelSelectorOpNotIn":
		operator = metav1.LabelSelectorOpNotIn
	case "k8spolicy.LabelSelectorOpExists":
		operator = metav1.LabelSelectorOpExists
	case "k8spolicy.LabelSelectorOpDoesNotExist":
		operator = metav1.LabelSelectorOpDoesNotExist
	}

	values := make([]string, rValues.Size())
	for i := 0; i < rValues.Size(); i++ {
		values[i] = rValues.At(i).ToString()
	}

	return &metav1.LabelSelectorRequirement{
		Key:      rKey.ToString(),
		Operator: operator,
		Values:   values,
	}
}

func NewRecordLabelSelector(labelSelector *metav1.LabelSelector) ddlog.Record {
	rMatchLabels := NewRecordLabels(labelSelector.MatchLabels)
	rMatchExpressions := ddlog.NewRecordVector()
	for _, req := range labelSelector.MatchExpressions {
		rMatchExpressions.Push(NewRecordLabelSelectorRequirement(&req))
	}
	return ddlog.NewRecordStructStatic(LabelSelectorConstructor, rMatchLabels, rMatchExpressions)
}

func RecordToLabelSelector(record ddlog.Record) *metav1.LabelSelector {
	r := record.AsStruct()
	rMatchLabels := r.At(0)
	rMatchExpressions := r.At(1).AsVector()

	matchExpressions := make([]metav1.LabelSelectorRequirement, rMatchExpressions.Size())

	for i := 0; i < rMatchExpressions.Size(); i++ {
		matchExpressions[i] = *RecordToLabelSelectorRequirement(rMatchExpressions.At(i))
	}

	return &metav1.LabelSelector{
		MatchLabels:      RecordToLabels(rMatchLabels),
		MatchExpressions: matchExpressions,
	}
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

func RecordToNetworkPolicyPort(record ddlog.Record) *networkingv1.NetworkPolicyPort {
	r := record.AsStruct()
	rProto := r.At(0).AsStruct()
	rPort := r.At(1).AsStruct()

	var proto *v1.Protocol
	if rProto.Name() == "std.Some" {
		p := v1.Protocol(rProto.At(0).ToString())
		proto = &p
	}

	var port *intstr.IntOrString
	if rPort.Name() == "std.Some" {
		p := RecordToIntOrString(rPort.At(0))
		port = &p
	}

	return &networkingv1.NetworkPolicyPort{
		Protocol: proto,
		Port:     port,
	}
}

func NewRecordIPBlock(ipBlock *networkingv1.IPBlock) ddlog.Record {
	rCIDR := ddlog.NewRecordString(ipBlock.CIDR)
	rExcept := ddlog.NewRecordVector()
	for _, except := range ipBlock.Except {
		rExcept.Push(ddlog.NewRecordString(except))
	}
	return ddlog.NewRecordStructStatic(IPBlockConstructor, rCIDR, rExcept)
}

func RecordToIPBlock(record ddlog.Record) *networkingv1.IPBlock {
	r := record.AsStruct()
	rCIDR := r.At(0)
	rExcept := r.At(1).AsVector()

	except := make([]string, rExcept.Size())
	for i := 0; i < rExcept.Size(); i++ {
		except[i] = rExcept.At(i).ToString()
	}

	return &networkingv1.IPBlock{
		CIDR:   rCIDR.ToString(),
		Except: except,
	}
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

func RecordToNetworkPolicyPeer(record ddlog.Record) *networkingv1.NetworkPolicyPeer {
	r := record.AsStruct()
	rPodSelector := r.At(0).AsStruct()
	rNamespaceSelector := r.At(1).AsStruct()
	rIPBlock := r.At(2).AsStruct()

	var podSelector *metav1.LabelSelector
	if rPodSelector.Name() == "std.Some" {
		podSelector = RecordToLabelSelector(rPodSelector.At(0))
	}

	var namespaceSelector *metav1.LabelSelector
	if rNamespaceSelector.Name() == "std.Some" {
		namespaceSelector = RecordToLabelSelector(rNamespaceSelector.At(0))
	}

	var ipBlock *networkingv1.IPBlock
	if rIPBlock.Name() == "std.Some" {
		ipBlock = RecordToIPBlock(rIPBlock.At(0))
	}

	return &networkingv1.NetworkPolicyPeer{
		PodSelector:       podSelector,
		NamespaceSelector: namespaceSelector,
		IPBlock:           ipBlock,
	}
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

func RecordToNetworkPolicyIngressRule(record ddlog.Record) *networkingv1.NetworkPolicyIngressRule {
	r := record.AsStruct()
	rPorts := r.At(0).AsVector()
	rFrom := r.At(1).AsVector()

	ports := make([]networkingv1.NetworkPolicyPort, rPorts.Size())
	for i := 0; i < rPorts.Size(); i++ {
		ports[i] = *RecordToNetworkPolicyPort(rPorts.At(i))
	}
	from := make([]networkingv1.NetworkPolicyPeer, rFrom.Size())
	for i := 0; i < rFrom.Size(); i++ {
		from[i] = *RecordToNetworkPolicyPeer(rFrom.At(i))
	}

	return &networkingv1.NetworkPolicyIngressRule{
		Ports: ports,
		From:  from,
	}
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

func RecordToNetworkPolicyEgressRule(record ddlog.Record) *networkingv1.NetworkPolicyEgressRule {
	r := record.AsStruct()
	rPorts := r.At(0).AsVector()
	rTo := r.At(1).AsVector()

	ports := make([]networkingv1.NetworkPolicyPort, rPorts.Size())
	for i := 0; i < rPorts.Size(); i++ {
		ports[i] = *RecordToNetworkPolicyPort(rPorts.At(i))
	}
	to := make([]networkingv1.NetworkPolicyPeer, rTo.Size())
	for i := 0; i < rTo.Size(); i++ {
		to[i] = *RecordToNetworkPolicyPeer(rTo.At(i))
	}

	return &networkingv1.NetworkPolicyEgressRule{
		Ports: ports,
		To:    to,
	}
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

func RecordToNetworkPolicySpec(record ddlog.Record) *networkingv1.NetworkPolicySpec {
	r := record.AsStruct()
	rPodSelector := r.At(0)
	rIngress := r.At(1).AsVector()
	rEgress := r.At(2).AsVector()
	rPolicyTypes := r.At(3).AsVector()

	ingress := make([]networkingv1.NetworkPolicyIngressRule, rIngress.Size())
	for i := 0; i < rIngress.Size(); i++ {
		ingress[i] = *RecordToNetworkPolicyIngressRule(rIngress.At(i))
	}

	egress := make([]networkingv1.NetworkPolicyEgressRule, rEgress.Size())
	for i := 0; i < rEgress.Size(); i++ {
		egress[i] = *RecordToNetworkPolicyEgressRule(rEgress.At(i))
	}

	policyTypes := make([]networkingv1.PolicyType, rPolicyTypes.Size())
	for i := 0; i < rPolicyTypes.Size(); i++ {
		switch rPolicyTypes.At(i).AsStruct().Name() {
		case "k8spolicy.PolicyTypeIngress":
			policyTypes[i] = networkingv1.PolicyTypeIngress
		case "k8spolicy.PolicyTypeEgress":
			policyTypes[i] = networkingv1.PolicyTypeEgress
		}
	}

	return &networkingv1.NetworkPolicySpec{
		PodSelector: *RecordToLabelSelector(rPodSelector),
		Ingress:     ingress,
		Egress:      egress,
		PolicyTypes: policyTypes,
	}
}

func NewRecordNetworkPolicy(np *networkingv1.NetworkPolicy) ddlog.Record {
	rName := ddlog.NewRecordString(np.Name)
	rNamespace := ddlog.NewRecordString(np.Namespace)
	rUID := NewRecordUID(np.UID)
	rSpec := NewRecordNetworkPolicySpec(&np.Spec)
	return ddlog.NewRecordStructStatic(NetworkPolicyConstructor, rName, rNamespace, rUID, rSpec)
}

func RecordToNetworkPolicy(record ddlog.Record) (*networkingv1.NetworkPolicy, error) {
	rNetworkPolicy, err := record.AsStructSafe()
	if err != nil {
		return nil, err
	}
	rName := rNetworkPolicy.At(0)
	rNamespace := rNetworkPolicy.At(1)
	rUID := rNetworkPolicy.At(2)
	rSpec := rNetworkPolicy.At(3)
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rName.ToString(),
			Namespace: rNamespace.ToString(),
			UID:       RecordToUID(rUID),
		},
		Spec: *RecordToNetworkPolicySpec(rSpec),
	}, nil
}

func NewRecordNetworkPolicyKey(namespace, name string) ddlog.Record {
	rNamespace := ddlog.NewRecordString(namespace)
	rName := ddlog.NewRecordString(name)
	return ddlog.NewRecordPair(rNamespace, rName)
}
