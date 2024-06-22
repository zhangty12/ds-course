package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "time"

type Coordinator struct {
	// Your definitions here.
	files []string
	nReduce int

	mutex chan bool
	
	mapTaskAssign []bool
	mapTaskFinish []bool

	isMapPhase bool
	isFinish bool

	reduceTaskAssign []bool
	reduceTaskFinish []bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

func (c *Coordinator) AssignTask(args *AssignArgs, reply *AssignReply) error {
	if c.isFinish {
		reply.isFinish = true
		return nil
	}
	reply.isFinish = false
	reply.nReduce = c.nReduce

	var taskID int
	var isMap bool
	flag := false
	
	// wait on channel
	<- c.mutex 

	if c.isMapPhase {
		reply.isMap = true
			isMap = true

			for i, b := range c.mapTaskAssign {
				if !b {
					if c.mapTaskFinish[i] {
						c.mapTaskAssign[i] = true
						continue
					}

					reply.taskID = i
					reply.inputFile = c.files[i]
					c.mapTaskAssign[i] = true

					taskID = i
					flag = true
					break
				}
			}
		} else {
			
		reply.isMap = false
		isMap = false

		for i, b := range c.reduceTaskAssign {
			if !b {
				if c.reduceTaskFinish[i] {
					c.reduceTaskAssign[i] = true
					continue
				}

				reply.taskID = i
				reply.inputFile = fmt.Sprintf("mr-out-%d", i)
				c.reduceTaskAssign[i] = true

				taskID = i
				flag = true
				break
			}
		}
	}
	c.mutex <- true	

	if flag {
		defer c.checkTask(isMap, taskID)
	}

	return nil
}

func (c *Coordinator) checkTask(isMap bool, taskID int) {
	time.Sleep(10 * time.Second)
	if isMap {
		if !c.mapTaskFinish[taskID] {
			c.mapTaskAssign[taskID] = false
		}
	} else {
		if !c.reduceTaskFinish[taskID] {
			c.reduceTaskAssign[taskID] = false
		}
	}
}

func (c *Coordinator) FinishTask(args *FinishArgs, reply *FinishReply) error {
	if c.isFinish {
		reply.isFinish = true
		return fmt.Errorf("This task is already finished")
	}

	if args.isMap {
		c.mapTaskFinish[args.taskID] = true
	} else {
		c.reduceTaskFinish[args.askID] = true
	}

	return nil
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	// map phase
	for !c.mapIsDone(){
		// loop
	}

	c.isMapPhase = false

	// reduce phase
	for !c.reduceIsDone(){
		// loop
	}

	c.isFinish = true

	ret = true
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files, nReduce: nReduce, isMapPhase: true, isFinish: false}

	// Your code here.

	c.mutex = make(chan bool, 1)
	c.mutex <- true

	c.mapTaskAssign = make([]bool, len(files))
	c.mapTaskFinish = make([]bool, len(files))
	c.reduceTaskAssign = make([]bool, nReduce)
	c.reduceTaskFinish = make([]bool, nReduce)

	c.server()
	return &c
}
