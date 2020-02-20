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

import "unsafe"

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

// Record is a Go wrapper for a DDlog record (ddlog_record *).
type Record struct {
	ptr unsafe.Pointer
}

// RecordBool creates a boolean record.
func RecordBool(v bool) Record {
	r := C.ddlog_bool(C.bool(v))
	return Record{r}
}

// RecordU64 creates a record for an unsigned integer value. Can be used to populate any DDlog field
// of type `bit<N>`, `N<=64`.
func RecordU64(v uint64) Record {
	r := C.ddlog_u64(C.uint64_t(v))
	return Record{r}
}

// RecordU32 creates a record for an unsigned integer value. Can be used to populate any DDlog field
// of type `bit<N>`, `N<=32`.
func RecordU32(v uint32) Record {
	return RecordU64(uint64(v))
}

// RecordI64 creates a record for a signed integer value. Can be used to populate any DDlog field of
// type `signed<N>`, `N<=64`.
func RecordI64(v int64) Record {
	r := C.ddlog_i64(C.int64_t(v))
	return Record{r}
}

// RecordI32 creates a record for a signed integer value. Can be used to populate any DDlog field of
// type `signed<N>`, `N<=32`.
func RecordI32(v int32) Record {
	return RecordI64(int64(v))
}

// RecordString creates a record for a string.
func RecordString(v string) Record {
	r := C.ddlogString(v)
	return Record{r}
}

// RecordStruct creates a struct record with specified constructor name and arguments.
func RecordStruct(constructor string, records ...Record) Record {
	cs := C.CString(constructor)
	defer C.free(unsafe.Pointer(cs))
	recordArray := C.makeRecordArray(C.size_t(len(records)))
	defer C.freeRecordArray(recordArray)
	for idx, record := range records {
		C.addRecordToArray(recordArray, C.size_t(idx), record.ptr)
	}
	r := C.ddlog_struct(cs, recordArray, C.size_t(len(records)))
	return Record{r}
}

// RecordStructStatic creates a struct record with specified constructor name and arguments. Unlike
// RecordStruct, this function takes a CString for the constructor to avoid making an extra copy of
// the constructor string when it is "static" (known ahead of time).
func RecordStructStatic(constructor CString, records ...Record) Record {
	recordArray := C.makeRecordArray(C.size_t(len(records)))
	defer C.freeRecordArray(recordArray)
	for idx, record := range records {
		C.addRecordToArray(recordArray, C.size_t(idx), record.ptr)
	}
	r := C.ddlog_struct_static_cons(constructor.ptr, recordArray, C.size_t(len(records)))
	return Record{r}
}

// RecordTuple creates a tuple record with specified fields.
func RecordTuple(records ...Record) Record {
	// avoid unecessary C calls if we are creating an empty vector
	if len(records) == 0 {
		r := C.ddlog_vector(nil, 0)
		return Record{r}
	}
	recordArray := C.makeRecordArray(C.size_t(len(records)))
	defer C.freeRecordArray(recordArray)
	for idx, record := range records {
		C.addRecordToArray(recordArray, C.size_t(idx), record.ptr)
	}
	r := C.ddlog_tuple(recordArray, C.size_t(len(records)))
	return Record{r}
}

// RecordTuplePush provides an alternative way (from RecordTuple) to construct tuples, by adding
// fields one-by-one.
func RecordTuplePush(rTuple, rValue Record) {
	C.ddlog_tuple_push(rTuple.ptr, rValue.ptr)
}

// RecordPair is a convenience way to create a 2-tuple. Such tuples are useful when constructing
// maps out of key-value pairs.
func RecordPair(r1, r2 Record) Record {
	r := C.ddlog_pair(r1.ptr, r2.ptr)
	return Record{r}
}

// RecordMap creates a map record with specified key-value pairs.
func RecordMap(records ...Record) Record {
	// avoid unecessary C calls if we are creating an empty map
	if len(records) == 0 {
		r := C.ddlog_vector(nil, 0)
		return Record{r}
	}
	recordArray := C.makeRecordArray(C.size_t(len(records)))
	defer C.freeRecordArray(recordArray)
	for idx, record := range records {
		C.addRecordToArray(recordArray, C.size_t(idx), record.ptr)
	}
	r := C.ddlog_map(recordArray, C.size_t(len(records)))
	return Record{r}
}

// RecordMapPush appends a key-value pair to a map.
func RecordMapPush(rMap, rKey, rValue Record) {
	C.ddlog_map_push(rMap.ptr, rKey.ptr, rValue.ptr)
}

// RecordVector creates a vector record with specified elements.
func RecordVector(records ...Record) Record {
	// avoid unecessary C calls if we are creating an empty vector
	if len(records) == 0 {
		r := C.ddlog_vector(nil, 0)
		return Record{r}
	}
	recordArray := C.makeRecordArray(C.size_t(len(records)))
	defer C.freeRecordArray(recordArray)
	for idx, record := range records {
		C.addRecordToArray(recordArray, C.size_t(idx), record.ptr)
	}
	r := C.ddlog_vector(recordArray, C.size_t(len(records)))
	return Record{r}
}

// RecordVectorPush appends an element to a vector.
func RecordVectorPush(rVec, rValue Record) {
	C.ddlog_vector_push(rVec.ptr, rValue.ptr)
}

// RecordSome is a convenience wrapper around RecordStructStatic for the std.Some constructor.
func RecordSome(r Record) Record {
	return RecordStructStatic(StdSomeConstructor, r)
}

// RecordNone is a convenience wrapper around RecordStructStatic for the std.None constructor.
func RecordNone() Record {
	return RecordStructStatic(StdNoneConstructor)
}

// RecordNull returns a NULL record, which can be used as a placeholder for an invalid record.
func RecordNull() Record {
	return Record{nil}
}

// RecordLeft is a convenience wrapper around RecordStructStatic for the std.Left constructor.
func RecordLeft(r Record) Record {
	return RecordStructStatic(StdLeftConstructor, r)
}

// RecordRight is a convenience wrapper around RecordStructStatic for the std.Right constructor.
func RecordRight(r Record) Record {
	return RecordStructStatic(StdRightConstructor, r)
}

// Dump returns a string representation of a record.
func (r *Record) Dump() string {
	cs := C.ddlog_dump_record(r.ptr)
	defer C.ddlog_string_free(cs)
	return C.GoString(cs)
}

// Free releases the memory associated with a given record. Do not call this method if ownership of
// the record has already been transferred to DDlog (e.g. by adding the record to a command).
func (r *Record) Free() {
	C.ddlog_free(r.ptr)
}
