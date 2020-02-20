package ddlog

/*
#cgo LDFLAGS: -L${SRCDIR}/libs -lnetworkpolicy_controller_ddlog
#include "ddlog.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

// Functions have to be static or the linker gives "multiple definition" errors.
// Not sure why but it only started happening after I started exporting Go functions with //export
// to call them from this C code.

static ddlog_cmd **makeCmdArray(size_t s) {
    return malloc(s * sizeof(ddlog_cmd *));
}

static void addCmdToArray(ddlog_cmd **ca, size_t idx, ddlog_cmd *cmd) {
    ca[idx] = cmd;
}

static void freeCmdArray(ddlog_cmd **ca) {
    free(ca);
}

extern void handleOutRecord(uintptr_t, table_id table, ddlog_record *rec, bool polarity);

static void dumpChangesCb(void *arg, table_id table, const ddlog_record *rec, bool polarity) {
    handleOutRecord((uintptr_t)arg, table, (ddlog_record *)rec, polarity);
}

static int ddlogTransactionCommitDumpChanges(ddlog_prog hprog, uintptr_t arg) {
    return ddlog_transaction_commit_dump_changes(hprog, dumpChangesCb, arg);
}
*/
import "C"

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"
)

type TableID uint

// GetTableID gets the table id by name.
func GetTableID(name string) TableID {
	cs := C.CString(name)
	defer C.free(unsafe.Pointer(cs))
	return TableID(C.ddlog_get_table_id(cs))
}

// GetTableName gets the table name by id.
func GetTableName(tableID TableID) string {
	cs := C.ddlog_get_table_name(C.table_id(tableID))
	return C.GoString(cs)
}

// OutPolarity indicates whether an output record is being inserted or deleted.
type OutPolarity string

const (
	// OutPolarityInsert is used to indicate that an output record is being inserted.
	OutPolarityInsert OutPolarity = "+1"
	// OutPolarityInsert is used to indicate that an output record is being deleted.
	OutPolarityDelete OutPolarity = "-1"
)

// OutRecordHandler defines an interface which lets the client register a "callback" (when creating
// a Program) for DDlog changes.
type OutRecordHandler interface {
	// Handle is called for every change reported by DDlog. There will a call to Handle for each
	// new or deleted record (there is no notion of "modified" output record in DDlog). Handle
	// will be called exactly once for each new / deleted record.
	Handle(TableID, Record, OutPolarity)
}

// OutRecordSink implements the OutRecordHandler interface: use it to discard all the changes
// received from DDlog.
type OutRecordSink struct{}

// NewOutRecordSink creates an OutRecordSink instance.
func NewOutRecordSink() (*OutRecordSink, error) {
	return &OutRecordSink{}, nil
}

// Handle will discard all the changes received from DDlog.
func (s *OutRecordSink) Handle(tableID TableID, r Record, outPolarity OutPolarity) {}

// OutRecordSink implements the OutRecordHandler interface: use it to log all the changes recived
// from DDlog to a file.
type OutRecordDumper struct {
	changesFile *os.File
	// changesMutex is used to serialize all the "writes" to changesFile.
	changesMutex sync.Mutex
}

// NewOutRecordDumper creates an OutRecordDumper instance.
func NewOutRecordDumper(changesFileName string) (*OutRecordDumper, error) {
	changesFile, err := os.Create(changesFileName)
	if err != nil {
		return nil, fmt.Errorf("error when creating file '%s' to dump changes: %v", changesFileName, err)
	}
	return &OutRecordDumper{
		changesFile: changesFile,
	}, nil
}

// Handle logs all the changes received from DDlog to a file. This should roughly match the output
// format from the DDlog CLI. Errors occurring when writing to disk are ignored.
func (d *OutRecordDumper) Handle(tableID TableID, r Record, outPolarity OutPolarity) {
	d.changesMutex.Lock()
	defer d.changesMutex.Unlock()
	fmt.Fprintf(d.changesFile, "%s:\n%s: %s\n", GetTableName(tableID), r.Dump(), outPolarity)
}

// We can't pass pointers allocated in Go to C directly, because the Go concurrent garbage collector
// may move data around. Since we need to be able to retrieve the Program instance when the
// HandleOutRecord callback is called, we use an integer "index" as the user-defined callback
// argument and this index can be mapped to the correct Program instance (*Program) in our
// thread-safe store. This workaround is described in the Go wiki:
// https://github.com/golang/go/wiki/cgo#function-variables.
var (
	_progIdx   uintptr = 0
	_progStore sync.Map
)

// Program is an instance of a DDlog program. It corresponds to ddlog_prog struct in the C API.
type Program struct {
	ptr              C.ddlog_prog
	commandsFile     *os.File
	outRecordHandler OutRecordHandler
	progIdx          uintptr
}

// NewProgram creates a new instance of a DDlog Program. workers is the number of worker threads
// that DDlog is allowed to use. outRecordHandler implements the Handle method, which will be called
// every time an output record is created / deleted. If workers is greater than 1, Handle can be
// called concurrently from multiple worker threads.
func NewProgram(workers uint, outRecordHandler OutRecordHandler) (*Program, error) {
	progIdx := atomic.AddUintptr(&_progIdx, uintptr(1))
	// TODO: add ability to redirect error messages. At the moment we pass NULL as
	// print_err_msg, which means that DDlog will print messages to stderr.
	prog := C.ddlog_run(C.uint(workers), false, nil, 0, nil)
	p := &Program{
		ptr:              prog,
		commandsFile:     nil,
		outRecordHandler: outRecordHandler,
		progIdx:          progIdx,
	}
	_progStore.Store(progIdx, p)
	return p, nil
}

func (p *Program) stopRecording() error {
	if p.commandsFile == nil {
		return nil
	}
	defer func() {
		_ = p.commandsFile.Close()
		p.commandsFile = nil
	}()
	rc := C.ddlog_record_commands(p.ptr, C.int(-1))
	if rc != 0 {
		return fmt.Errorf("ddlog_record_commands returned error code %d", rc)
	}
	return nil
}

// RecordCommands creates a file with the provided name to record all the commands sent to DDlog. If
// the file already exists, it will be truncated.
func (p *Program) StartRecordingCommands(name string) error {
	if err := p.stopRecording(); err != nil {
		return fmt.Errorf("error when stopping command recording: %v", err)
	}

	commandsFile, err := os.Create(name)
	if err != nil {
		return fmt.Errorf("error when creating file '%s' to record commands: %v", name, err)
	}
	fd := commandsFile.Fd()
	rc := C.ddlog_record_commands(p.ptr, C.int(fd))
	if rc != 0 {
		_ = commandsFile.Close()
		return fmt.Errorf("ddlog_record_commands returned error code %d", rc)
	}
	p.commandsFile = commandsFile
	return nil
}

// StopRecordingCommands stops recording the commands sent to DDlog to file and closes the file.
func (p *Program) StopRecordingCommands() error {
	if err := p.stopRecording(); err != nil {
		return fmt.Errorf("error when stopping command recording: %v", err)
	}
	return nil
}

// DumpInputSnapshot dumps current snapshot of input tables to the provided file in a format
// suitable for replay debugging.
func (p *Program) DumpInputSnapshot(name string) error {
	snapshotFile, err := os.Create(name)
	defer snapshotFile.Close()
	if err != nil {
		return fmt.Errorf("error when creating file '%s' to dump input snapshot: %v", name, err)
	}
	fd := snapshotFile.Fd()
	rc := C.ddlog_dump_input_snapshot(p.ptr, C.int(fd))
	if rc != 0 {
		return fmt.Errorf("ddlog_dump_input_snapshot returned error code %d", rc)
	}
	return nil
}

// Stop stops the DDlog program and deallocates all the resources allocated by DDlog.
func (p *Program) Stop() error {
	if err := p.stopRecording(); err != nil {
		return fmt.Errorf("error when stopping command recording: %v", err)
	}

	rc := C.ddlog_stop(p.ptr)
	if rc != 0 {
		return fmt.Errorf("ddlog_stop returned error code %d", rc)
	}
	_progStore.Delete(p.progIdx)
	return nil
}

// StartTransaction starts a transaction. Note that DDlog does not support nested or concurrent
// transactions.
func (p *Program) StartTransaction() error {
	rc := C.ddlog_transaction_start(p.ptr)
	if rc != 0 {
		return fmt.Errorf("ddlog_transaction_start returned error code %d", rc)
	}
	return nil
}

// CommitTransaction commits a transaction.
func (p *Program) CommitTransaction() error {
	// Because of garbage collection, cgo does not let us pass a Go pointer as the callback
	// argument (C code is not supposed to store a Go pointer). We therefore use the trick
	// described in the Go wiki (https://github.com/golang/go/wiki/cgo#function-variables) and
	// we use a thread-safe registry for Program instances.
	rc := C.ddlogTransactionCommitDumpChanges(p.ptr, C.uint64_t(p.progIdx))
	if rc != 0 {
		return fmt.Errorf("ddlog_transaction_commit returned error code %d", rc)
	}
	return nil
}

// RollbackTransaction rollbacks an ongoing transaction.
func (p *Program) RollbackTransaction() error {
	rc := C.ddlog_transaction_rollback(p.ptr)
	if rc != 0 {
		return fmt.Errorf("ddlog_transaction_rollback returned error code %d", rc)
	}
	return nil
}

// ApplyUpdates applies updates to DDlog tables. Must be called as part of a transaction.
func (p *Program) ApplyUpdates(commands ...Command) error {
	cmdArray := C.makeCmdArray(C.size_t(len(commands)))
	defer C.freeCmdArray(cmdArray)
	for idx, command := range commands {
		C.addCmdToArray(cmdArray, C.size_t(idx), command.ptr)
	}
	rc := C.ddlog_apply_updates(p.ptr, cmdArray, C.size_t(len(commands)))
	if rc != 0 {
		return fmt.Errorf("ddlog_apply_updates returned error code %d", rc)
	}
	return nil
}

// ApplyUpdates applies a single update to DDlog tables. Must be called as part of a transaction.
func (p *Program) ApplyUpdate(command Command) error {
	rc := C.ddlog_apply_updates(p.ptr, &command.ptr, 1)
	if rc != 0 {
		return fmt.Errorf("ddlog_apply_updates returned error code %d", rc)
	}
	return nil
}

// ApplyUpdates starts a transaction, applies updates to DDlog tables and commits the transaction.
func (p *Program) ApplyUpdatesAsTransaction(commands ...Command) error {
	if err := p.StartTransaction(); err != nil {
		return err
	}
	if err := p.ApplyUpdates(commands...); err != nil {
		return err
	}
	if err := p.CommitTransaction(); err != nil {
		return err
	}
	return nil
}

// handleOutRecord is called from C for each new or deleted output record.
//export handleOutRecord
func handleOutRecord(progIdx C.uintptr_t, tableID C.size_t, recordPtr *C.ddlog_record, polarity C.bool) {
	pIntf, ok := _progStore.Load(uintptr(progIdx))
	if !ok {
		panic("Cannot find program in store")
	}
	p := pIntf.(*Program)
	var outPolarity OutPolarity
	if polarity {
		outPolarity = OutPolarityInsert
	} else {
		outPolarity = OutPolarityDelete
	}
	if p.outRecordHandler != nil {
		p.outRecordHandler.Handle(TableID(tableID), &record{unsafe.Pointer(recordPtr)}, outPolarity)
	}
}
