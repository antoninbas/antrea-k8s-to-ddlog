package ddlog

/*
#cgo LDFLAGS: -L${SRCDIR}/libs -lnetworkpolicy_controller_ddlog
#include "ddlog.h"
#include <stdlib.h>
#include <assert.h>

ddlog_record **makeRecordArray(size_t s) {
    return malloc(s * sizeof(ddlog_record *));
}

void addRecordToArray(ddlog_record **ra, size_t idx, ddlog_record *r) {
    ra[idx] = r;
}

void freeRecordArray(ddlog_record **ra) {
    free(ra);
}

// a wrapper around ddlog_string_with_length which takes a Go string as parameter.
ddlog_record *ddlogString(_GoString_ s) {
    return ddlog_string_with_length(_GoStringPtr(s), _GoStringLen(s));
}
*/
import "C"

import (
	"fmt"
	"unsafe"
)

var (
	StdSomeConstructor = NewCString("std.Some")
	StdNoneConstructor = NewCString("std.None")

	StdLeftConstructor  = NewCString("std.Left")
	StdRightConstructor = NewCString("std.Right")
)

// CString is a wrapper around a C string. This is useful when you want to pre-allocate a "static"
// string once and use it multiple times, as it avoids multiple calls to C.CString / copies.
type CString struct {
	ptr *C.char
}

// NewCString creates a new CString. It invokes C.CString which allocates a C string in the C heap
// using malloc and copies the contents of the Go string to that location. Because this is a "C
// pointer", it is not subject to the restrictions of Go pointers
// (https://golang.org/cmd/cgo/#hdr-Passing_pointers). It is the caller's responsibility to release
// the allocated memory by calling Free.
func NewCString(s string) CString {
	return CString{C.CString(s)}
}

// Free releases the memory allocated in the C heap for the underlying C string. Do not use teh
// Cstring instance after calling this method.
func (cs CString) Free() {
	C.free(unsafe.Pointer(cs.ptr))
}

type Record interface {
	ptr() unsafe.Pointer

	Free()
	Dump() string

	IsNull() bool
	IsBool() bool
	IsInt() bool
	IsString() bool
	IsTuple() bool
	IsVector() bool
	IsMap() bool
	IsSet() bool
	IsStruct() bool

	IntBits() uint

	ToBool() bool
	ToBoolSafe() (bool, error)
	ToU64() uint64
	ToU64Safe() (uint64, error)
	ToU32() uint32
	ToU32Safe() (uint32, error)
	ToI64() int64
	ToI64Safe() (int64, error)
	ToI32() int32
	ToI32Safe() (int32, error)
	ToString() string
	ToStringSafe() (string, error)

	AsTuple() RecordTuple
	AsTupleSafe() (RecordTuple, error)
	AsVector() RecordVector
	AsVectorSafe() (RecordVector, error)
	AsMap() RecordMap
	AsMapSafe() (RecordMap, error)
	AsSet() RecordSet
	AsSetSafe() (RecordSet, error)
	AsStruct() RecordStruct
	AsStructSafe() (RecordStruct, error)
}

type RecordTuple interface {
	Record
	Push(rValue Record)
	At(idx uint) Record
	Size() uint
}

type RecordVector interface {
	Record
	Push(rValue Record)
	At(idx uint) Record
	Size() uint
}

type RecordMap interface {
	Record
	Push(rKey, rValue Record)
	KeyAt(idx uint) Record
	ValueAt(idx uint) Record
	At(idx uint) (Record, Record)
	Size() uint
}

type RecordSet interface {
	Record
	Push(rValue Record)
	At(idx uint) Record
	Size() uint
}

type RecordStruct interface {
	Record
	At(idx uint) Record
}

type record struct {
	recordPtr unsafe.Pointer
}

type recordTuple struct {
	record
}

type recordVector struct {
	record
}

type recordMap struct {
	record
}

type recordSet struct {
	record
}

type recordStruct struct {
	record
}

func (r *record) ptr() unsafe.Pointer {
	return r.recordPtr
}

// Dump returns a string representation of a record.
func (r *record) Dump() string {
	cs := C.ddlog_dump_record(r.ptr())
	defer C.ddlog_string_free(cs)
	return C.GoString(cs)
}

// Free releases the memory associated with a given record. Do not call this method if ownership of
// the record has already been transferred to DDlog (e.g. by adding the record to a command).
func (r *record) Free() {
	C.ddlog_free(r.ptr())
}

func (r *record) IsNull() bool {
	return r.ptr() == unsafe.Pointer(nil)
}

func (r *record) IsBool() bool {
	return bool(C.ddlog_is_bool(r.ptr()))
}

func (r *record) IsInt() bool {
	return bool(C.ddlog_is_int(r.ptr()))
}

func (r *record) IsString() bool {
	return bool(C.ddlog_is_string(r.ptr()))
}

func (r *record) IsTuple() bool {
	return bool(C.ddlog_is_tuple(r.ptr()))
}

func (r *record) IsVector() bool {
	return bool(C.ddlog_is_vector(r.ptr()))
}

func (r *record) IsMap() bool {
	return bool(C.ddlog_is_map(r.ptr()))
}

func (r *record) IsSet() bool {
	return bool(C.ddlog_is_set(r.ptr()))
}

func (r *record) IsStruct() bool {
	return bool(C.ddlog_is_struct(r.ptr()))
}

func (r *record) IntBits() uint {
	return uint(C.ddlog_int_bits(r.ptr()))
}

func (r *record) ToBool() bool {
	return bool(C.ddlog_get_bool(r.ptr()))
}

func (r *record) ToBoolSafe() (bool, error) {
	if !r.IsBool() {
		return false, fmt.Errorf("record is not a bool")
	}
	return bool(C.ddlog_get_bool(r.ptr())), nil
}

func (r *record) ToU64() uint64 {
	return uint64(C.ddlog_get_u64(r.ptr()))
}

func (r *record) ToU64Safe() (uint64, error) {
	if !r.IsInt() {
		return 0, fmt.Errorf("record is not an integer")
	}
	if r.IntBits() > 64 {
		return 0, fmt.Errorf("integer record cannot be represented with 64 bits")
	}
	return uint64(C.ddlog_get_u64(r.ptr())), nil
}

func (r *record) ToU32() uint32 {
	return uint32(C.ddlog_get_u64(r.ptr()))
}

func (r *record) ToU32Safe() (uint32, error) {
	if !r.IsInt() {
		return 0, fmt.Errorf("record is not an integer")
	}
	if r.IntBits() > 32 {
		return 0, fmt.Errorf("integer record cannot be represented with 32 bits")
	}
	return uint32(C.ddlog_get_u64(r.ptr())), nil
}

func (r *record) ToI64() int64 {
	return int64(C.ddlog_get_u64(r.ptr()))
}

func (r *record) ToI64Safe() (int64, error) {
	if !r.IsInt() {
		return 0, fmt.Errorf("record is not an integer")
	}
	if r.IntBits() > 64 {
		return 0, fmt.Errorf("integer record cannot be represented with 64 bits")
	}
	return int64(C.ddlog_get_i64(r.ptr())), nil
}

func (r *record) ToI32() int32 {
	return int32(C.ddlog_get_i64(r.ptr()))
}

func (r *record) ToI32Safe() (int32, error) {
	if !r.IsInt() {
		return 0, fmt.Errorf("record is not an integer")
	}
	if r.IntBits() > 32 {
		return 0, fmt.Errorf("integer record cannot be represented with 32 bits")
	}
	return int32(C.ddlog_get_i64(r.ptr())), nil
}

func (r *record) ToString() string {
	var len C.size_t
	cs := C.ddlog_get_str_with_length(r.ptr(), &len)
	return C.GoStringN(cs, C.int(len))
}

func (r *record) ToStringSafe() (string, error) {
	var len C.size_t
	cs := C.ddlog_get_str_with_length(r.ptr(), &len)
	if unsafe.Pointer(cs) == unsafe.Pointer(nil) {
		return "", fmt.Errorf("record is not a string")
	}
	return C.GoStringN(cs, C.int(len)), nil
}

func (r *record) AsTuple() RecordTuple {
	return &recordTuple{*r}
}

func (r *record) AsTupleSafe() (RecordTuple, error) {
	if !r.IsTuple() {
		return nil, fmt.Errorf("record is not a tuple")
	}
	return &recordTuple{*r}, nil
}

func (r *record) AsVector() RecordVector {
	return &recordVector{*r}
}

func (r *record) AsVectorSafe() (RecordVector, error) {
	if !r.IsVector() {
		return nil, fmt.Errorf("record is not a vector")
	}
	return &recordVector{*r}, nil
}

func (r *record) AsMap() RecordMap {
	return &recordMap{*r}
}

func (r *record) AsMapSafe() (RecordMap, error) {
	if !r.IsMap() {
		return nil, fmt.Errorf("record is not a map")
	}
	return &recordMap{*r}, nil
}

func (r *record) AsSet() RecordSet {
	return &recordSet{*r}
}

func (r *record) AsSetSafe() (RecordSet, error) {
	if !r.IsSet() {
		return nil, fmt.Errorf("record is not a set")
	}
	return &recordSet{*r}, nil
}

func (r *record) AsStruct() RecordStruct {
	return &recordStruct{*r}
}

func (r *record) AsStructSafe() (RecordStruct, error) {
	if !r.IsStruct() {
		return nil, fmt.Errorf("record is not a struct")
	}
	return &recordStruct{*r}, nil
}

// NewRecordBool creates a boolean record.
func NewRecordBool(v bool) Record {
	r := C.ddlog_bool(C.bool(v))
	return &record{r}
}

// NewRecordU64 creates a record for an unsigned integer value. Can be used to populate any DDlog
// field of type `bit<N>`, `N<=64`.
func NewRecordU64(v uint64) Record {
	r := C.ddlog_u64(C.uint64_t(v))
	return &record{r}
}

// NewRecordU32 creates a record for an unsigned integer value. Can be used to populate any DDlog
// field of type `bit<N>`, `N<=32`.
func NewRecordU32(v uint32) Record {
	return NewRecordU64(uint64(v))
}

// NewRecordI64 creates a record for a signed integer value. Can be used to populate any DDlog field
// of type `signed<N>`, `N<=64`.
func NewRecordI64(v int64) Record {
	r := C.ddlog_i64(C.int64_t(v))
	return &record{r}
}

// NewRecordI32 creates a record for a signed integer value. Can be used to populate any DDlog field
// of type `signed<N>`, `N<=32`.
func NewRecordI32(v int32) Record {
	return NewRecordI64(int64(v))
}

// NewRecordString creates a record for a string.
func NewRecordString(v string) Record {
	r := C.ddlogString(v)
	return &record{r}
}

// NewRecordStruct creates a struct record with specified constructor name and arguments.
func NewRecordStruct(constructor string, records ...Record) RecordStruct {
	cs := C.CString(constructor)
	defer C.free(unsafe.Pointer(cs))
	recordArray := C.makeRecordArray(C.size_t(len(records)))
	defer C.freeRecordArray(recordArray)
	for idx, record := range records {
		C.addRecordToArray(recordArray, C.size_t(idx), record.ptr())
	}
	r := C.ddlog_struct(cs, recordArray, C.size_t(len(records)))
	return &recordStruct{record{r}}
}

// NewRecordStructStatic creates a struct record with specified constructor name and
// arguments. Unlike NewRecordStruct, this function takes a CString for the constructor to avoid
// making an extra copy of the constructor string when it is "static" (known ahead of time).
func NewRecordStructStatic(constructor CString, records ...Record) RecordStruct {
	recordArray := C.makeRecordArray(C.size_t(len(records)))
	defer C.freeRecordArray(recordArray)
	for idx, record := range records {
		C.addRecordToArray(recordArray, C.size_t(idx), record.ptr())
	}
	r := C.ddlog_struct_static_cons(constructor.ptr, recordArray, C.size_t(len(records)))
	return &recordStruct{record{r}}
}

func (rStruct *recordStruct) At(idx uint) Record {
	r := C.ddlog_get_struct_field(rStruct.ptr(), C.size_t(idx))
	return &record{r}
}

// NewRecordTuple creates a tuple record with specified fields.
func NewRecordTuple(records ...Record) RecordTuple {
	// avoid unecessary C calls if we are creating an empty vector
	if len(records) == 0 {
		r := C.ddlog_tuple(nil, 0)
		return &recordTuple{record{r}}
	}
	recordArray := C.makeRecordArray(C.size_t(len(records)))
	defer C.freeRecordArray(recordArray)
	for idx, record := range records {
		C.addRecordToArray(recordArray, C.size_t(idx), record.ptr())
	}
	r := C.ddlog_tuple(recordArray, C.size_t(len(records)))
	return &recordTuple{record{r}}
}

func (rTuple *recordTuple) Push(rValue Record) {
	C.ddlog_tuple_push(rTuple.ptr(), rValue.ptr())
}

func (rTuple *recordTuple) At(idx uint) Record {
	r := C.ddlog_get_tuple_field(rTuple.ptr(), C.size_t(idx))
	return &record{r}
}

func (rTuple *recordTuple) Size() uint {
	return uint(C.ddlog_get_tuple_size(rTuple.ptr()))
}

// NewRecordPair is a convenience way to create a 2-tuple. Such tuples are useful when constructing
// maps out of key-value pairs.
func NewRecordPair(r1, r2 Record) RecordTuple {
	r := C.ddlog_pair(r1.ptr(), r2.ptr())
	return &recordTuple{record{r}}
}

// NewRecordMap creates a map record with specified key-value pairs.
func NewRecordMap(records ...Record) RecordMap {
	// avoid unecessary C calls if we are creating an empty map
	if len(records) == 0 {
		r := C.ddlog_map(nil, 0)
		return &recordMap{record{r}}
	}
	recordArray := C.makeRecordArray(C.size_t(len(records)))
	defer C.freeRecordArray(recordArray)
	for idx, record := range records {
		C.addRecordToArray(recordArray, C.size_t(idx), record.ptr())
	}
	r := C.ddlog_map(recordArray, C.size_t(len(records)))
	return &recordMap{record{r}}
}

// RecordMapPush appends a key-value pair to a map.
// func RecordMapPush(rMap, rKey, rValue Record) {
// 	C.ddlog_map_push(rMap.ptr(), rKey.ptr(), rValue.ptr())
// }

func (rMap *recordMap) Push(rKey, rValue Record) {
	C.ddlog_map_push(rMap.ptr(), rKey.ptr(), rValue.ptr())
}

func (rMap *recordMap) KeyAt(idx uint) Record {
	r := C.ddlog_get_map_key(rMap.ptr(), C.size_t(idx))
	return &record{r}
}

func (rMap *recordMap) ValueAt(idx uint) Record {
	r := C.ddlog_get_map_val(rMap.ptr(), C.size_t(idx))
	return &record{r}
}

func (rMap *recordMap) At(idx uint) (Record, Record) {
	return rMap.KeyAt(idx), rMap.ValueAt(idx)
}

func (rMap *recordMap) Size() uint {
	return uint(C.ddlog_get_map_size(rMap.ptr()))
}

// NewRecordVector creates a vector record with specified elements.
func NewRecordVector(records ...Record) RecordVector {
	// avoid unecessary C calls if we are creating an empty vector
	if len(records) == 0 {
		r := C.ddlog_vector(nil, 0)
		return &recordVector{record{r}}
	}
	recordArray := C.makeRecordArray(C.size_t(len(records)))
	defer C.freeRecordArray(recordArray)
	for idx, record := range records {
		C.addRecordToArray(recordArray, C.size_t(idx), record.ptr())
	}
	r := C.ddlog_vector(recordArray, C.size_t(len(records)))
	return &recordVector{record{r}}
}

// Push appends an element to a vector.
func (rVec *recordVector) Push(rValue Record) {
	C.ddlog_vector_push(rVec.ptr(), rValue.ptr())
}

func (rVec *recordVector) At(idx uint) Record {
	r := C.ddlog_get_vector_elem(rVec.ptr(), C.size_t(idx))
	return &record{r}
}

func (rVec *recordVector) Size() uint {
	return uint(C.ddlog_get_vector_size(rVec.ptr()))
}

// NewRecordSet creates a set record with specified elements.
func NewRecordSet(records ...Record) RecordSet {
	// avoid unecessary C calls if we are creating an empty set
	if len(records) == 0 {
		r := C.ddlog_set(nil, 0)
		return &recordSet{record{r}}
	}
	recordArray := C.makeRecordArray(C.size_t(len(records)))
	defer C.freeRecordArray(recordArray)
	for idx, record := range records {
		C.addRecordToArray(recordArray, C.size_t(idx), record.ptr())
	}
	r := C.ddlog_set(recordArray, C.size_t(len(records)))
	return &recordSet{record{r}}
}

// Push appends an element to a set.
func (rSet *recordSet) Push(rValue Record) {
	C.ddlog_set_push(rSet.ptr(), rValue.ptr())
}

func (rSet *recordSet) At(idx uint) Record {
	r := C.ddlog_get_set_elem(rSet.ptr(), C.size_t(idx))
	return &record{r}
}

func (rSet *recordSet) Size() uint {
	return uint(C.ddlog_get_set_size(rSet.ptr()))
}

// NewRecordSome is a convenience wrapper around NewRecordStructStatic for the std.Some
// constructor.
func NewRecordSome(r Record) Record {
	return NewRecordStructStatic(StdSomeConstructor, r)
}

// NewRecordNone is a convenience wrapper around NewRecordStructStatic for the std.None
// constructor.
func NewRecordNone() Record {
	return NewRecordStructStatic(StdNoneConstructor)
}

// NewRecordNull returns a NULL record, which can be used as a placeholder for an invalid record.
func NewRecordNull() Record {
	return &record{nil}
}

// NewRecordLeft is a convenience wrapper around NewRecordStructStatic for the std.Left
// constructor.
func NewRecordLeft(r Record) Record {
	return NewRecordStructStatic(StdLeftConstructor, r)
}

// NewRecordRight is a convenience wrapper around NewRecordStructStatic for the std.Right
// constructor.
func NewRecordRight(r Record) Record {
	return NewRecordStructStatic(StdRightConstructor, r)
}
