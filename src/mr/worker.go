package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

func (a ByKey) Len() int {
	return len(a)
}

func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := AskTaskArgs{}
		reply := AskTaskReply{}
		ok := call("Master.AskTask", &args, &reply)

		if !ok {
			log.Fatalf("call AskTask failed")
			return
		}

		switch reply.TaskType {
		case MapTask:
			err := doMapTask(reply, mapf)
			if err == nil {
				reportTask(MapTask, reply.TaskId)
			} else {
				log.Printf("map task %v failed: %v\n", reply.TaskId, err)
				time.Sleep(time.Second)
			}

		case ReduceTask:
			err := doReduceTask(reply, reducef)
			if err == nil {
				reportTask(ReduceTask, reply.TaskId)
			} else {
				log.Printf("reduce task %v failed: %v\n", reply.TaskId, err)
				time.Sleep(time.Second)
			}

		case WaitTask:
			time.Sleep(time.Second)

		case ExitTask:
			return
		}
	}
}

func doMapTask(task AskTaskReply, mapf func(string, string) []KeyValue) error {
	// 1. 读取文件内容。
	filename := task.Filename
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("cannot open %v: %v", filename, err)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		file.Close()
		return err
	}
	file.Close()

	kva := mapf(filename, string(content))

	// buckets[r] 保存应该交给 reduce task r 的 KeyValue。
	buckets := make([][]KeyValue, task.NReduce)

	for _, kv := range kva {
		r := ihash(kv.Key) % task.NReduce
		buckets[r] = append(buckets[r], kv)
	}

	// 一个 map task 会产生 nReduce 个中间文件：
	// mr-X-0, mr-X-1, ..., mr-X-(nReduce-1)
	for r := 0; r < task.NReduce; r++ {
		finalName := fmt.Sprintf("mr-%d-%d", task.TaskId, r)

		tmpFile, err := os.CreateTemp(".", "mr-tmp-*")
		if err != nil {
			return err
		}

		enc := json.NewEncoder(tmpFile)
		for _, kv := range buckets[r] {
			err := enc.Encode(&kv)
			if err != nil {
				tmpFile.Close()
				os.Remove(tmpFile.Name())
				return err
			}
		}

		if err := tmpFile.Close(); err != nil {
			os.Remove(tmpFile.Name())
			return err
		}

		// 原子 rename，避免 worker 崩溃时留下半成品文件。
		if err := os.Rename(tmpFile.Name(), finalName); err != nil {
			os.Remove(tmpFile.Name())
			return err
		}
	}

	return nil
}

func doReduceTask(task AskTaskReply, reducef func(string, []string) string) error {
	reduceId := task.TaskId

	kva := []KeyValue{}

	// reduce task Y 要读取所有 map task 产生的 mr-X-Y
	for mapId := 0; mapId < task.NMap; mapId++ {
		filename := fmt.Sprintf("mr-%d-%d", mapId, reduceId)
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))

	tmpFile, err := os.CreateTemp(".", "mr-out-tmp-*")
	if err != nil {
		return err
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)

		// this is the correct format.
		fmt.Fprintf(tmpFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpFile.Name())
		return err
	}

	finalName := fmt.Sprintf("mr-out-%d", reduceId)

	if err := os.Rename(tmpFile.Name(), finalName); err != nil {
		os.Remove(tmpFile.Name())
		return err
	}

	return nil
}

func reportTask(taskType int, taskId int) {
	args := ReportTaskArgs{
		TaskType: taskType,
		TaskId:   taskId,
	}

	reply := ReportTaskReply{}

	call("Master.ReportTask", &args, &reply)
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
