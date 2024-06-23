package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

import "time"
import "os"
import "io/ioutil"
import "encoding/json"
import "sort"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		reply := getAssign()
		finished := reply.IsFinish
		isAssign := reply.IsAssign
		isMap := reply.IsMap
		taskID := reply.TaskID
		nFile := reply.NFile
		nReduce := reply.NReduce
		inputFile := reply.InputFile

		if finished {
			break
		} else if !isAssign {
			// getAssign failed
			time.Sleep(100 * time.Millisecond)
			continue
		} else if isMap {
			// map phase, taskID = fileIdx
			tempFiles := make([]*os.File, nReduce)
			for i:=0; i<nReduce; i++ {
				tempFilename := fmt.Sprintf("mr-%d-%d-", taskID, i)

				// fmt.Println(tempFilename)

				ff, err := os.CreateTemp("./mr-inter/", tempFilename)
				if err != nil {
					//log.Fatal(err)
					fmt.Println(err)
				}
				tempFiles[i] = ff
				// defer os.Remove(tempFiles[i].Name())
			}

			input, err := os.Open(inputFile)
			if err != nil {
				log.Fatalf("cannot open %v", inputFile)
			}

			content, err := ioutil.ReadAll(input)
			if err != nil {
				log.Fatalf("cannot read %v", inputFile)
			}
			input.Close()
			kva := mapf(inputFile, string(content))

			encoders := make([]*json.Encoder, nReduce)
			for i:=0; i<nReduce; i++ {
				encoders[i] = json.NewEncoder(tempFiles[i])
			}
			for _, kv := range kva {
				idx := ihash(kv.Key) % nReduce
				err := encoders[idx].Encode(&kv)
				if err != nil {
					log.Fatal(err)
				}
			}

			for i, tmpf := range tempFiles {
				interFilename := fmt.Sprintf("./mr-inter/mr-%d-%d", taskID, i)
				err := os.Rename(tmpf.Name(), interFilename)
				if err != nil {
					// dst already exists
					os.Remove(tmpf.Name())
				} else {
					tmpf.Close()
				}
			}

			finishTask(isMap, taskID)
		} else{
			// reduce phase

			kva := []KeyValue{}

			for i:=0; i<nFile; i++ {
				input, err := os.Open(fmt.Sprintf("./mr-inter/mr-%d-%d", i, taskID))
				if err != nil {
					log.Fatalf("cannot open %v", input.Name())
				}
				dec := json.NewDecoder(input)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				input.Close()
			}

			sort.Sort(ByKey(kva))
			tempFilename := fmt.Sprintf("mr-out-%d-", taskID)
			tempFile, err := os.CreateTemp("./", tempFilename)
			if err != nil {
				fmt.Println(err)
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

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

				i = j
			}

			outputFilename := fmt.Sprintf("./mr-out-%d", taskID)
			err = os.Rename(tempFile.Name(), outputFilename)
			if err != nil {
				// dst already exists
				os.Remove(tempFile.Name())
			} else {
				tempFile.Close()
			}

			finishTask(isMap, taskID)
		}

	}

}

func getAssign() *AssignReply {
	args := new(AssignArgs)
	reply := new(AssignReply)

	// reply.ReplyPrint()
	ok := call("Coordinator.AssignTask", args, reply)
	// reply.ReplyPrint()
	if ok {
		return reply
	} else {
		reply.IsFinish = true
		return reply
	}
}

func finishTask(isMap bool, taskID int) {
	args := FinishArgs{IsMap: isMap, TaskID: taskID}
	reply := FinishReply{}
	call("Coordinator.FinishTask", &args, &reply)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
