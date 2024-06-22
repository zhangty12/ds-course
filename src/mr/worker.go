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
		finished, isMap, taskID, nReduce, inputFile:= getAssign()
		if finished {
			break
		} else if taskID == -1 {
			// getAssign failed
			time.Sleep(100 * time.Millisecond)
			continue
		} else if isMap {
			// map phase, taskID = fileIdx
			tempFiles := make([]File, nReduce)
			for i:=0; i<nReduce; i++ {
				tempFilename := fmt.Sprintf("mr-%d-%d", taskID, i)
				ff, err = os.CreateTemp("/", tempFilename)
				if err != nil {
					log.Fatal(err)
				}
				tempFiles[i] = ff
				// defer os.Remove(tempFiles[i].name())
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
			kva := mapf(input, string(content))

			encoders := make([]Encoder, nReduce)
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
				interFilename := fmt.Sprintf("/%s%d%s%e", prefix, taskID, "-", i)
				err := os.Rename(tmpf.name(), interFilename)
				if err != nil {
					// dst already exists
					os.Remove(tmpf.name())
				} else {
					tmpf.Close()
				}
			}

			finishMapTask(taskID)
		} else{
			// reduce phase
			input, err := os.Open(inputFile)
			if err != nil {
				log.Fatalf("cannot open %v", inputFile)
			}

			kva := []KeyValue{}
			dec := json.NewDecoder(input)
			for {
				var kv KeyValue
				if err := dec.Decoder(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
			input.Close()


			sort.Sort(kva)
			tempFilename := fmt.Sprintf("mr-out-%d", taskID)
			tempFile, err := os.CreateTemp("/", tempFilename)
			if err != nil {
				log.Fatal(err)
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

			outputFilename := fmt.Sprintf("/%s%d", prefix, taskID)
			err := os.Rename(tempFile.name(), outputFilename)
			if err != nil {
				// dst already exists
				os.Remove(tempFile.name())
			} else {
				tempFile.Close()
			}

			finishTask(isMap, taskID)
		}

	}

}

func getAssign() (bool, bool, int, int, string) {
	args := AssignArgs{}
	reply := AssignReply{taskID: -1}

	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		return reply.isFinish, reply.isMap, reply.taskID, reply.nReduce, reply.inputFile
	} else {
		return true, false, -1, -1, nil
	}
}

func finishMapTask(isMap bool, taskID int) {
	args := FinishArgs{isMap: isMap, taskID: taskID}
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
