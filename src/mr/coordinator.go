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
		reply.IsFinish = true
		return nil
	}
	reply.IsFinish = false
	reply.NReduce = c.nReduce
	reply.NFile = len(c.files)

	taskID := -1
	isMap := c.isMapPhase
	flag := false
	
	// wait on channel
	<- c.mutex 

	if c.isMapPhase {

		reply.IsMap = true
		isMap = true

		for i, b := range c.mapTaskAssign {
			if !b {
				if c.mapTaskFinish[i] {
					c.mapTaskAssign[i] = true
					continue
				}

				reply.IsAssign = true
				reply.TaskID = i
				reply.InputFile = c.files[i]
				c.mapTaskAssign[i] = true

				taskID = i
				flag = true

				// fmt.Printf("Assign map task-%d\n", reply.TaskID)
				// reply.ReplyPrint()
				break
			}
		}
	} else {
		reply.IsMap = false
		isMap = false

		for i, b := range c.reduceTaskAssign {
			if !b {
				if c.reduceTaskFinish[i] {
					c.reduceTaskAssign[i] = true
					continue
				}

				reply.IsAssign = true
				reply.TaskID = i
				// reply.InputFile = fmt.Sprintf("./mr-inter/mr-%d-", i)
				c.reduceTaskAssign[i] = true

				taskID = i
				flag = true

				// fmt.Printf("Assign reduce task-%d\n", reply.TaskID)
				// reply.ReplyPrint()
				break
			}
		}
	}

	c.mutex <- true	

	if flag {
		//fmt.Println(isMap, taskID)
		go c.checkTask(isMap, taskID)
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
		reply.IsFinish = true
		return fmt.Errorf("This task is already finished")
	}

	if args.IsMap {
		c.mapTaskFinish[args.TaskID] = true
	} else {
		c.reduceTaskFinish[args.TaskID] = true
	}

	return nil
}

func (c *Coordinator) mapIsDone() bool {
	flag := true
	for _, b := range c.mapTaskFinish {
		if !b {
			flag = false
			break
		}
	}
	c.isMapPhase = !flag
	return flag
}

func (c *Coordinator) reduceIsDone() bool {
	flag := true
	for _, b := range c.reduceTaskFinish {
		if !b {
			flag = false
			break
		}
	}
	c.isFinish = flag
	return flag
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.

	if c.isMapPhase {
		c.mapIsDone()
	} else if !c.isFinish {
		c.reduceIsDone()
	} 

	if c.isFinish {
		// fmt.Println("Removing intermediate files")
		for i:=0; i<len(c.files); i++ {
			for j:=0; j<c.nReduce; j++ {
				os.Remove(fmt.Sprintf("./mr-inter/mr-%d-%d", i, j))
			}
		}
	}

	return c.isFinish
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

	os.Mkdir("./mr-inter/", 0755)

	c.server()
	return &c
}
