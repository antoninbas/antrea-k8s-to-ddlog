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

type CString struct {
	ptr *C.char
}

func NewCString(s string) CString {
	return CString{C.CString(s)}
}

func (cs CString) Free() {
	C.free(unsafe.Pointer(cs.ptr))
}

type Record struct {
	ptr unsafe.Pointer
}

func RecordBool(v bool) Record {
	r := C.ddlog_bool(C.bool(v))
	return Record{r}
}

func RecordU64(v uint64) Record {
	r := C.ddlog_u64(C.uint64_t(v))
	return Record{r}
}

func RecordU32(v uint32) Record {
	return RecordU64(uint64(v))
}

func RecordI64(v int64) Record {
	r := C.ddlog_i64(C.int64_t(v))
	return Record{r}
}

func RecordI32(v int32) Record {
	return RecordI64(int64(v))
}

func RecordString(v string) Record {
	// cs := C.CString(v)
	// defer C.free(unsafe.Pointer(cs))
	// r := C.ddlog_string(cs)
	// avoid an extra copy
	r := C.ddlogString(v)
	return Record{r}
}

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

func RecordStructStatic(constructor CString, records ...Record) Record {
	recordArray := C.makeRecordArray(C.size_t(len(records)))
	defer C.freeRecordArray(recordArray)
	for idx, record := range records {
		C.addRecordToArray(recordArray, C.size_t(idx), record.ptr)
	}
	r := C.ddlog_struct_static_cons(constructor.ptr, recordArray, C.size_t(len(records)))
	return Record{r}
}

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

func RecordTuplePush(rTuple, rValue Record) {
	C.ddlog_tuple_push(rTuple.ptr, rValue.ptr)
}

func RecordPair(r1, r2 Record) Record {
	r := C.ddlog_pair(r1.ptr, r2.ptr)
	return Record{r}
}

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

func RecordMapPush(rMap, rKey, rValue Record) {
	C.ddlog_map_push(rMap.ptr, rKey.ptr, rValue.ptr)
}

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

func RecordVectorPush(rVec, rValue Record) {
	C.ddlog_vector_push(rVec.ptr, rValue.ptr)
}

func RecordSome(r Record) Record {
	return RecordStructStatic(StdSomeConstructor, r)
}

func RecordNone() Record {
	return RecordStructStatic(StdNoneConstructor)
}

func RecordNull() Record {
	return Record{nil}
}

func RecordLeft(r Record) Record {
	return RecordStructStatic(StdLeftConstructor, r)
}

func RecordRight(r Record) Record {
	return RecordStructStatic(StdRightConstructor, r)
}

func (r *Record) Dump() string {
	cs := C.ddlog_dump_record(r.ptr)
	defer C.ddlog_string_free(cs)
	return C.GoString(cs)
}

func (r *Record) Free() {
	C.ddlog_free(r.ptr)
}
