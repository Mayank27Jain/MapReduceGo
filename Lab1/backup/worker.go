package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
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

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	workerId := -1
	rep1 := CallGiveReport(workerId)
	workerId = rep1.WorkerIndex
	for {
		rep2 := CallGetWork(workerId)
		if rep2.Operation == -1 {
			break
		} else if rep2.Operation == 0 {
			time.Sleep(time.Second)
		} else if rep2.Operation == 1 {
			filename := rep2.FileName
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			sort.Sort(ByKey(kva))
			var encs []json.Encoder
			for i := 0; i < rep2.AltNum; i++ {
				filename := "temp-" + strconv.Itoa(rep2.FileIndex) + "-" + strconv.Itoa(i)
				file, _ := os.Create(filename)
				enc := json.NewEncoder(file)
				encs = append(encs, *enc)
			}
			for _, kv := range kva {
				enum := ihash(kv.Key) % rep2.AltNum
				encs[enum].Encode(&kv)
			}
			for i := 0; i < rep2.AltNum; i++ {
				oldname := "temp-" + strconv.Itoa(rep2.FileIndex) + "-" + strconv.Itoa(i)
				newname := "mr-" + strconv.Itoa(rep2.FileIndex) + "-" + strconv.Itoa(i)
				os.Rename(oldname, newname)
			}
		} else {
			var kva []KeyValue
			for i := 0; i < rep2.AltNum; i++ {
				filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(rep2.FileIndex)
				file, _ := os.Open(filename)
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
			sort.Sort(ByKey(kva))
			fname := "temp-out-" + strconv.Itoa(rep2.FileIndex)
			ofile, _ := os.Create(fname)
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
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				i = j
			}
			ofile.Close()
			nname := "mr-out-" + strconv.Itoa(rep2.FileIndex)
			os.Rename(fname, nname)
		}
		CallGiveReport(workerId)
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	call("Master.Example", &args, &reply)

// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

func CallGetWork(wi int) WorkReply {
	args := WorkArgs{}
	args.WorkerIndex = wi
	reply := WorkReply{}
	ret := call("Master.GetWork", &args, &reply)
	if !ret {
		reply = WorkReply{-1, 0, "", 0}
	}
	return reply
}

func CallGiveReport(wi int) ReportReply {
	args := ReportArgs{}
	args.WorkerIndex = wi
	reply := ReportReply{}
	ret := call("Master.GiveReport", &args, &reply)
	if !ret {
		reply = ReportReply{-1}
	}
	return reply
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

	// fmt.Println(err)
	return false
}
