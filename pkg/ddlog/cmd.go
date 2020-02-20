package ddlog

/*
#cgo LDFLAGS: -L${SRCDIR}/libs -lnetworkpolicy_controller_ddlog
#include "ddlog.h"
*/
import "C"

import "unsafe"

type Command struct {
	ptr unsafe.Pointer
}

func NewInsertCommand(tableID TableID, r Record) Command {
	cmd := C.ddlog_insert_cmd(C.size_t(tableID), r.ptr())
	return Command{unsafe.Pointer(cmd)}
}

func NewInsertOrUpdateCommand(tableID TableID, r Record) Command {
	cmd := C.ddlog_insert_or_update_cmd(C.size_t(tableID), r.ptr())
	return Command{unsafe.Pointer(cmd)}
}

func NewDeleteValCommand(tableID TableID, r Record) Command {
	cmd := C.ddlog_delete_val_cmd(C.size_t(tableID), r.ptr())
	return Command{unsafe.Pointer(cmd)}
}

func NewDeleteKeyCommand(tableID TableID, r Record) Command {
	cmd := C.ddlog_delete_key_cmd(C.size_t(tableID), r.ptr())
	return Command{unsafe.Pointer(cmd)}
}
