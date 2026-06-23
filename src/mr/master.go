package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskMeta struct {
	State     int
	StartTime time.Time
}

type Master struct {
	mu sync.Mutex

	files   []string
	nReduce int

	mapTasks    []TaskMeta
	reduceTasks []TaskMeta
}

// Your code here -- RPC handlers for the worker to call.
// worker 调用这个 RPC 来向 master 要任务。
func (m *Master) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	reply.NReduce = m.nReduce
	reply.NMap = len(m.files)

	// 1. 先分配 map task。
	if !m.allMapDone() {
		for i := range m.mapTasks {
			if m.mapTasks[i].State == Idle ||
				(m.mapTasks[i].State == InProgress &&
					time.Since(m.mapTasks[i].StartTime) > 10*time.Second) {

				m.mapTasks[i].State = InProgress
				m.mapTasks[i].StartTime = time.Now()

				reply.TaskType = MapTask
				reply.TaskId = i
				reply.Filename = m.files[i]
				return nil
			}
		}

		// 没有 idle map task，但还有 map task 没完成。
		reply.TaskType = WaitTask
		return nil
	}

	// 2. map 全部完成后，开始分配 reduce task。
	if !m.allReduceDone() {
		for i := range m.reduceTasks {
			if m.reduceTasks[i].State == Idle ||
				(m.reduceTasks[i].State == InProgress &&
					time.Since(m.reduceTasks[i].StartTime) > 10*time.Second) {

				m.reduceTasks[i].State = InProgress
				m.reduceTasks[i].StartTime = time.Now()

				reply.TaskType = ReduceTask
				reply.TaskId = i
				return nil
			}
		}

		// 没有 idle reduce task，但还有 reduce task 没完成。
		reply.TaskType = WaitTask
		return nil
	}

	// 3. 所有 reduce task 完成，通知 worker 退出。
	reply.TaskType = ExitTask
	return nil
}

// worker 完成任务后调用这个 RPC 汇报。
func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch args.TaskType {
	case MapTask:
		if args.TaskId >= 0 && args.TaskId < len(m.mapTasks) {
			m.mapTasks[args.TaskId].State = Done
		}

	case ReduceTask:
		if args.TaskId >= 0 && args.TaskId < len(m.reduceTasks) {
			m.reduceTasks[args.TaskId].State = Done
		}
	}

	return nil
}

func (m *Master) allMapDone() bool {
	for i := range m.mapTasks {
		if m.mapTasks[i].State != Done {
			return false
		}
	}
	return true
}

func (m *Master) allReduceDone() bool {
	for i := range m.reduceTasks {
		if m.reduceTasks[i].State != Done {
			return false
		}
	}
	return true
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.allReduceDone()
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {

	// Your code here.
	m := Master{
		files:       files,
		nReduce:     nReduce,
		mapTasks:    make([]TaskMeta, len(files)),
		reduceTasks: make([]TaskMeta, nReduce),
	}
	m.server()
	return &m
}
