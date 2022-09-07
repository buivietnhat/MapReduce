package mr

import (
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type WorkType int
type Status int

const (
	MAP    WorkType = 1
	REDUCE WorkType = 2
	NONE   WorkType = 3
	QUIT   WorkType = 4
)

const (
	MASTER int = 0
	WORKER int = 1
)

const (
	FRESH    Status = 0
	ASSIGNED Status = 1
	COMPELED Status = 2
)

type RequestWorkResponse struct {
	WorkType    WorkType
	Index       int
	NReduce     int
	MapFileList []string
}

type Coordinator struct {
	// Your definitions here.
	filesMap map[int][]string
	nReduce  int
	nMap     int

	// index -> dummy: just to check what indexes are unassigned
	remainMapWorks             map[int]int
	remainReduceWorks          map[int]int
	remainUnassignedMapWork    map[int]int
	remainUnassignedReduceWork map[int]int

	done bool
	mu   sync.Mutex
}

func NewCoordinator(nReduce int) *Coordinator {

	//reduceWorkStatus := make([]Status, nReduce)
	remainUnassignedReduceWorks := map[int]int{}
	remainReduceWorks := map[int]int{}
	for i := 0; i < nReduce; i++ {
		remainReduceWorks[i] = -1
		remainUnassignedReduceWorks[i] = -1
	}

	return &Coordinator{
		nReduce:                    nReduce,
		remainReduceWorks:          remainReduceWorks,
		remainUnassignedReduceWork: remainUnassignedReduceWorks,
		done:                       false}
}

func (c *Coordinator) splitFiles(fileList []string, numMapSplits int) {
	size := len(fileList)/numMapSplits + 1
	count := 0
	c.filesMap = make(map[int][]string)
	for i := 0; i < len(fileList); i = i + size {
		for j := 0; j < size && i+j < len(fileList); j++ {
			c.filesMap[count] = append(c.filesMap[count], fileList[i+j])
		}
		count += 1
	}

	c.nMap = count
	c.remainMapWorks = make(map[int]int)
	c.remainUnassignedMapWork = make(map[int]int)
	for i := 0; i < c.nMap; i++ {
		c.remainMapWorks[i] = -1
		c.remainUnassignedMapWork[i] = -1
	}
	logger.Debug(dMaster, MASTER, "Just split the input files to %d map splits", c.nMap)

}

func (c *Coordinator) ReportWork(args *ReportWorkArgs, reply *ReportWorkReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.WType == MAP {
		logger.Debug(dMaster, MASTER, "Get report MAP work index %d successfully", args.Index)
		delete(c.remainMapWorks, args.Index)
	} else if args.WType == REDUCE {
		logger.Debug(dMaster, MASTER, "Get report REDUCE work index %d successfully", args.Index)
		delete(c.remainReduceWorks, args.Index)
	}

	return nil
}

func DeleteRedundantMapFiles(mIndex int) {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		if !file.IsDir() {
			if strings.Contains(file.Name(), "MAP_"+strconv.Itoa(mIndex)) {
				e := os.Remove(file.Name())
				if e != nil {
					log.Fatal(e)
				}
			}
		}
	}
}

// remove all the intermediate MAP files
func GarbageCollection() {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		if !file.IsDir() {
			if strings.Contains(file.Name(), "MAP_") {
				e := os.Remove(file.Name())
				if e != nil {
					log.Fatal(e)
				}
			}
		}
	}
}

func DeleteRedundantReduceFiles(rIndex int) {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		if !file.IsDir() {
			if strings.Contains(file.Name(), "mr-out-"+strconv.Itoa(rIndex)) {
				//fmt.Println(file.Name())
				e := os.Remove(file.Name())
				if e != nil {
					log.Fatal(e)
				}
			}
		}
	}
}

func (c *Coordinator) monitorWorkerStatus(workType WorkType, index int) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	if workType == MAP {
		if _, exist := c.remainMapWorks[index]; exist {
			logger.Debug(dDrop, MASTER, "The worker processing MAP index %d seems to be crashed, re-assign",
				index)
			// the index has not been removed from the hashtable ==> assume the worker has crashed
			c.remainUnassignedMapWork[index] = -1 // add back the waiting list

			DeleteRedundantMapFiles(index)
			return
		}
	} else if workType == REDUCE {
		if _, exist := c.remainReduceWorks[index]; exist {
			logger.Debug(dDrop, MASTER, "The worker processing Reduce index %d seems to be crashed, re-assign",
				index)
			c.remainUnassignedReduceWork[index] = -1
			DeleteRedundantReduceFiles(index)
		}
	}
}

func (c *Coordinator) respondMapWork(reply *RequestWorkReply, mapIndex int) {
	reply.WorkType = MAP
	reply.Index = mapIndex
	reply.NReduce = c.nReduce
	reply.MapFileList = c.filesMap[mapIndex]
}

func (c *Coordinator) respondReduceWork(reply *RequestWorkReply, reduceIndex int) {
	reply.WorkType = REDUCE
	reply.Index = reduceIndex
}

func (c *Coordinator) respondNoneWork(reply *RequestWorkReply) {
	reply.WorkType = NONE
}

func (c *Coordinator) respondQuit(reply *RequestWorkReply) {
	reply.WorkType = QUIT
}

func (c *Coordinator) RequestWork(args *RequestWorkArgs,
	reply *RequestWorkReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// check if the map works are done, otherwise assign a map work
	numRemainUnassignedMapWorks := len(c.remainUnassignedMapWork)
	if numRemainUnassignedMapWorks > 0 {
		for idx, _ := range c.remainUnassignedMapWork {
			// just need to pull out the first index
			delete(c.remainUnassignedMapWork, idx)
			logger.Debug(dMap, MASTER, "Assign MAP index %d work", idx)
			c.respondMapWork(reply, idx)
			go c.monitorWorkerStatus(MAP, idx)
			return nil
		}
	}

	numRemainUnassignedReduceWorks := len(c.remainUnassignedReduceWork)
	remainMapWorks := len(c.remainMapWorks)
	// if all the map works have been done, only then assign reduce works
	if remainMapWorks == 0 && numRemainUnassignedReduceWorks > 0 {
		for idx, _ := range c.remainUnassignedReduceWork {
			delete(c.remainUnassignedReduceWork, idx)
			logger.Debug(dReduce, MASTER, "Assign Reduce index %d work", idx)
			c.respondReduceWork(reply, idx)
			go c.monitorWorkerStatus(REDUCE, idx)
			return nil
		}
	}

	if remainMapWorks == 0 && len(c.remainReduceWorks) == 0 {
		// all work is done, return
		logger.Debug(dMaster, MASTER, "Seem we're done now")
		c.respondQuit(reply)
		// need to clean up before reporting done
		GarbageCollection()
		c.done = true
	} else {
		c.respondNoneWork(reply)
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()
	done := c.done
	c.mu.Unlock()
	return done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	numMapSplits := 5
	c := NewCoordinator(nReduce)
	c.splitFiles(files, numMapSplits)
	c.server()
	return c
}
