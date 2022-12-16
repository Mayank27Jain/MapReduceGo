package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Work struct {
	operation int
	fileIndex int
	status    int
}

type Master struct {
	// Your definitions here.
	filenames []string
	mapOps    []Work
	redOps    []Work
	assigned  []Work
	mapDone   bool
	redDone   bool
	nRed      int
	nMap      int
	mu        sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

func (m *Master) GetWork(wa *WorkArgs, wr *WorkReply) error {
	defer m.mu.Unlock()
	m.mu.Lock()
	workerId := wa.WorkerIndex
	got_work := false
	if !m.mapDone {
		for i, _ := range m.mapOps {
			if m.mapOps[i].status == 0 {
				wr.AltNum = m.nRed
				wr.FileIndex = i
				wr.FileName = m.filenames[i]
				wr.Operation = 1
				toassign := Work{1, i, 1}
				m.assigned[workerId] = toassign
				m.mapOps[i] = m.assigned[workerId]
				got_work = true
			}
		}
		if !got_work {
			wr.AltNum = 0
			wr.FileIndex = 0
			wr.FileName = ""
			wr.Operation = 0
		}
	} else if !m.redDone {
		for i, _ := range m.redOps {
			if m.redOps[i].status == 0 {
				wr.AltNum = m.nMap
				wr.FileIndex = i
				wr.FileName = ""
				wr.Operation = 2
				toassign := Work{2, i, 1}
				m.assigned[workerId] = toassign
				m.redOps[i] = m.assigned[workerId]
				got_work = true
			}
		}
		if !got_work {
			wr.AltNum = 0
			wr.FileIndex = 0
			wr.FileName = ""
			wr.Operation = 0
		}
	} else {
		wr.AltNum = 0
		wr.FileIndex = 0
		wr.FileName = ""
		wr.Operation = 0
	}
	return nil
}

func (m *Master) GiveReport(ra *ReportArgs, rr *ReportReply) error {
	defer m.mu.Unlock()
	m.mu.Lock()
	workerId := ra.WorkerIndex
	if workerId == -1 {
		numWorkers := len(m.assigned)
		rr.WorkerIndex = numWorkers
		toassign := Work{0, 0, 0}
		m.assigned = append(m.assigned, toassign)
		return nil
	}
	workAss := m.assigned[workerId]
	if workAss.operation == 0 {
		rr.WorkerIndex = workerId
		return nil
	} else if workAss.operation == 1 {
		m.mapOps[workAss.fileIndex].status = 2
		m.mapDone = true
		for i, _ := range m.mapOps {
			if m.mapOps[i].status != 2 {
				m.mapDone = false
				break
			}
		}
		rr.WorkerIndex = workerId
		return nil
	} else {
		m.redOps[workAss.fileIndex].status = 2
		m.redDone = true
		for i, _ := range m.mapOps {
			if m.mapOps[i].status != 2 {
				m.redDone = false
				break
			}
		}
		rr.WorkerIndex = workerId
		return nil
	}
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	defer m.mu.Unlock()
	m.mu.Lock()
	ret := m.mapDone && m.redDone
	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.filenames = files
	for i, _ := range files {
		toadd := Work{1, i, 0}
		m.mapOps = append(m.mapOps, toadd)
	}
	for i := 0; i < nReduce; i++ {
		toadd := Work{2, i, 0}
		m.redOps = append(m.redOps, toadd)
	}
	m.nMap = len(files)
	m.nRed = nReduce
	m.mapDone = false
	m.redDone = false
	m.server()
	return &m
}
