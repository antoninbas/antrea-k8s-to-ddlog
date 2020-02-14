package ddlog

/*
#cgo LDFLAGS: -L${SRCDIR}/libs -lnetworkpolicy_controller_ddlog
#include "ddlog.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

ddlog_cmd **makeCmdArray(size_t s) {
    return malloc(s * sizeof(ddlog_cmd *));
}

void addCmdToArray(ddlog_cmd **ca, size_t idx, ddlog_cmd *cmd) {
    ca[idx] = cmd;
}

void freeCmdArray(ddlog_cmd **ca) {
    free(ca);
}

void dumpChangesCb(void *arg, table_id table, const ddlog_record *rec, bool polarity) {
    int fd = (int)(uintptr_t)arg;
    char *str = ddlog_dump_record(rec);
    ssize_t s = write(fd, str, strlen(str));
    s = write(fd, "\n", 1);
    ddlog_string_free(str);
}

int ddlogTransactionCommitDumpChanges(ddlog_prog hprog, int fd) {
    return ddlog_transaction_commit_dump_changes(hprog, dumpChangesCb, (uintptr_t)fd);
}
*/
import "C"

import (
	"fmt"
	"os"
	"sync"
	"unsafe"
)

type TableID uint

func GetTableID(name string) TableID {
	cs := C.CString(name)
	defer C.free(unsafe.Pointer(cs))
	return TableID(C.ddlog_get_table_id(cs))
}

type Program struct {
	ptr          C.ddlog_prog
	commandsFile *os.File
	dumpChanges  bool
	changesFile  *os.File
	changesMutex sync.Mutex
}

func NewProgram(workers uint, changesFileName string) (*Program, error) {
	prog := C.ddlog_run(C.uint(workers), false, nil, C.ulong(0), nil)
	var changesFile *os.File
	if changesFileName != "" {
		var err error
		changesFile, err = os.Create(changesFileName)
		if err != nil {
			return nil, fmt.Errorf("error when creating file '%s' to dump changes: %v", changesFileName, err)
		}
	}
	return &Program{
		ptr:          prog,
		commandsFile: nil,
		dumpChanges:  (changesFileName != ""),
		changesFile:  changesFile,
	}, nil
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

// RecordCommands will create a file with the provided name to record all the commands sent to
// DDLog. If the file already exists, it will be truncated.
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

func (p *Program) StopRecordingCommands() error {
	if err := p.stopRecording(); err != nil {
		return fmt.Errorf("error when stopping command recording: %v", err)
	}
	return nil
}

func (p *Program) Stop() error {
	if err := p.stopRecording(); err != nil {
		return fmt.Errorf("error when stopping command recording: %v", err)
	}

	if p.changesFile != nil {
		defer p.changesFile.Close()
	}

	rc := C.ddlog_stop(p.ptr)
	if rc != 0 {
		return fmt.Errorf("ddlog_stop returned error code %d", rc)
	}
	return nil
}

func (p *Program) StartTransaction() error {
	rc := C.ddlog_transaction_start(p.ptr)
	if rc != 0 {
		return fmt.Errorf("ddlog_transaction_start returned error code %d", rc)
	}
	return nil
}

func (p *Program) CommitTransaction() error {
	var rc C.int
	if p.dumpChanges {
		p.changesMutex.Lock()
		rc = C.ddlogTransactionCommitDumpChanges(p.ptr, C.int(p.changesFile.Fd()))
		p.changesMutex.Unlock()
	} else {
		rc = C.ddlog_transaction_commit(p.ptr)
	}
	if rc != 0 {
		return fmt.Errorf("ddlog_transaction_commit returned error code %d", rc)
	}
	return nil
}

func (p *Program) ApplyUpdates(commands ...Command) error {
	cmdArray := C.makeCmdArray(C.ulong(len(commands)))
	defer C.freeCmdArray(cmdArray)
	for idx, command := range commands {
		C.addCmdToArray(cmdArray, C.ulong(idx), command.ptr)
	}
	rc := C.ddlog_apply_updates(p.ptr, cmdArray, C.ulong(len(commands)))
	if rc != 0 {
		return fmt.Errorf("ddlog_apply_updates returned error code %d", rc)
	}
	return nil
}

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
