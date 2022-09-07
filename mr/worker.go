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
	"strings"
)

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func RequestWorks() RequestWorkResponse {
	args := RequestWorkArgs{}
	args.M = 0 // not really necessary for now
	reply := RequestWorkReply{}

	ok := call("Coordinator.RequestWork", &args, &reply)
	if ok {
	} else {
		fmt.Printf("call failed!\n")
	}

	response := RequestWorkResponse{}
	response.WorkType = reply.WorkType
	response.Index = reply.Index
	response.MapFileList = reply.MapFileList
	response.NReduce = reply.NReduce

	return response
}

func Report(wType WorkType, index int) {
	args := ReportWorkArgs{}
	args.WType = wType
	args.Index = index

	reply := ReportWorkReply{}

	ok := call("Coordinator.ReportWork", &args, &reply)
	if ok {
	} else {
		logger.Debug(dWorker, WORKER, "The call to report work to master has failed, hmmm")
	}
}

func Paritition(pair KeyValue, mapNumber int, reduceNumber int) {
	fileName := "MAP_" + strconv.Itoa(mapNumber) + "_" + strconv.Itoa(reduceNumber)
	f, err := os.OpenFile(fileName,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	enc := json.NewEncoder(f)
	enc.Encode(&pair)
	f.Close()
}

func DoMap(mapf func(string, string) []KeyValue, mIndex int,
	nReduce int, fileNames []string) {
	for _, fileName := range fileNames {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", fileName)
		}
		file.Close()
		kva := mapf(fileName, string(content))

		for _, pair := range kva {
			rNumber := ihash(pair.Key) % nReduce
			Paritition(pair, mIndex, rNumber)
		}
	}

	Report(MAP, mIndex)
}

func DoReduce(reducef func(string, []string) string, rIndex int) {
	var mapFiles []string

	files, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		if !file.IsDir() {
			if strings.Contains(file.Name(), "MAP_") {
				_rIndex := file.Name()[len(file.Name())-1:]
				if _rIndex == strconv.Itoa(rIndex) {
					mapFiles = append(mapFiles, file.Name())
				}
			}
		}
	}

	var intermediate []KeyValue

	for _, fileName := range mapFiles {
		file, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
		if err != nil {
			fmt.Println("cannot open ", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var readBack KeyValue
			err = dec.Decode(&readBack)
			if err == nil { // ok
				intermediate = append(intermediate, readBack)
			} else {
				//fmt.Println("cannot decode back the file ", fileName)
				break
			}
		}

	}

	// sort by Key
	sort.Sort(ByKey(intermediate))

	// save the output
	oname := "mr-out-" + strconv.Itoa(rIndex)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	Report(REDUCE, rIndex)
}

//
// main/mrworker.go calls this function.
//
func process(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) WorkType {
	response := RequestWorks()

	if response.WorkType == MAP { // map work
		logger.Debug(dWorker, WORKER, "Processing Map index %d work", response.Index)
		DoMap(mapf, response.Index, response.NReduce, response.MapFileList)
	} else if response.WorkType == REDUCE { // reduce work
		logger.Debug(dWorker, WORKER, "Processing Reduce index %d work", response.Index)
		DoReduce(reducef, response.Index)
	}

	return response.WorkType
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	var workType WorkType
	// Your worker implementation here.
	for {
		workType = process(mapf, reducef)
		if workType == QUIT {
			logger.Debug(dWorker, WORKER, "Seem that all the works have been done, return ..")
			break
		}
		// time.Sleep(1 * time.Second)
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
