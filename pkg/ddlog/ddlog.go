package ddlog

/*
#cgo LDFLAGS: -L${SRCDIR}/libs -lnetworkpolicy_controller_ddlog
#include "ddlog.h"
#include <stdlib.h>

ddlog_record **makeRecordArray(size_t s) {
    return malloc(s * sizeof(ddlog_record *));
}

void addRecordToArray(ddlog_record **ra, size_t idx, ddlog_record *r) {
    ra[idx] = (ddlog_record *)r;
}

void freeRecordArray(ddlog_record **ra) {
    free(ra);
}
*/
import "C"

import "unsafe"

func GetTableId(name string) uint64 {
	cs := C.CString(name)
	defer C.free(unsafe.Pointer(cs))
	return uint64(C.ddlog_get_table_id(cs))
}

type Record struct {
	ptr unsafe.Pointer
}

func RecordBool(v bool) Record {
	r := C.ddlog_bool(C.bool(v))
	return Record{r}
}

func RecordU64(v uint64) Record {
	// won't work on 32 bit arch
	r := C.ddlog_u64(C.ulong(v))
	return Record{r}
}

func RecordU32(v uint32) Record {
	return RecordU64(uint64(v))
}

func RecordI64(v int64) Record {
	r := C.ddlog_i64(C.long(v))
	return Record{r}
}

func RecordI32(v int32) Record {
	return RecordI64(int64(v))
}

func RecordString(v string) Record {
	cs := C.CString(v)
	defer C.free(unsafe.Pointer(cs))
	r := C.ddlog_string(cs)
	return Record{r}
}

func RecordStruct(constructor string, records ...Record) Record {
	cs := C.CString(constructor)
	defer C.free(unsafe.Pointer(cs))
	recordArray := C.makeRecordArray(C.ulong(len(records)))
	defer C.freeRecordArray(recordArray)
	for idx, record := range records {
		C.addRecordToArray(recordArray, C.ulong(idx), record.ptr)
	}
	r := C.ddlog_struct(cs, recordArray, C.ulong(len(records)))
	return Record{r}
}

func RecordTuple(records ...Record) Record {
	recordArray := C.makeRecordArray(C.ulong(len(records)))
	defer C.freeRecordArray(recordArray)
	for idx, record := range records {
		C.addRecordToArray(recordArray, C.ulong(idx), record.ptr)
	}
	r := C.ddlog_tuple(recordArray, C.ulong(len(records)))
	return Record{r}
}

func RecordPair(r1, r2 Record) Record {
	r := C.ddlog_pair(r1.ptr, r2.ptr)
	return Record{r}
}

func RecordMap(records ...Record) Record {
	recordArray := C.makeRecordArray(C.ulong(len(records)))
	defer C.freeRecordArray(recordArray)
	for idx, record := range records {
		C.addRecordToArray(recordArray, C.ulong(idx), record.ptr)
	}
	r := C.ddlog_map(recordArray, C.ulong(len(records)))
	return Record{r}
}

func RecordMapPush(rMap, rKey, rValue Record) {
	C.ddlog_map_push(rMap.ptr, rKey.ptr, rValue.ptr)
}

func RecordVector(records ...Record) Record {
	recordArray := C.makeRecordArray(C.ulong(len(records)))
	defer C.freeRecordArray(recordArray)
	for idx, record := range records {
		C.addRecordToArray(recordArray, C.ulong(idx), record.ptr)
	}
	r := C.ddlog_vector(recordArray, C.ulong(len(records)))
	return Record{r}
}

func RecordVectorPush(rVec, rValue Record) {
	C.ddlog_vector_push(rVec.ptr, rValue.ptr)
}

func RecordSome(r Record) Record {
	return RecordStruct("std.Some", r)
}

func RecordNone() Record {
	return RecordStruct("std.None")
}

func RecordNull() Record {
	return Record{nil}
}

func RecordEither(left, right Record) Record {
	return RecordStruct("std.Either", left, right)
}

func (r *Record) Dump() string {
	cs := C.ddlog_dump_record(r.ptr)
	defer C.ddlog_string_free(cs)
	return C.GoString(cs)
}

func (r *Record) Free() {
	C.ddlog_free(r.ptr)
}
