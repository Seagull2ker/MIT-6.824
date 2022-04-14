package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "strconv"
import "encoding/json"
import "sort"
// import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// 用于sort排序
type ByKey []KeyValue
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

// 中间临时文件的保存路径
// const filePath = "./mr-tmp"
const filePath = "."

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// 第一次请求任务时候的参数
	request := GetTaskArgs{} 
	for {
		// 从master获取任务信息
		replyBody := GetTask(request)
		// 所有任务执行完毕，master不分配，结束for循环
		switch replyBody.JobType {
		case "map" :
			performMap(replyBody, mapf)
		case "reduce" :
			performReduce(replyBody, reducef)
		case "done" :
			os.Exit(0)
		default :
			log.Printf("Bad task type %s", replyBody.JobType)
		}
	}
}

func performMap(replyBody GetTaskReply, mapf func(string, string) []KeyValue){
	// 读取输入文件
	file, err := os.Open(replyBody.InputFile)
	if err != nil {
		log.Fatal("cannot open %v", replyBody.InputFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal("cannot read %v", replyBody.InputFile)
	}
	file.Close()
	// 执行map函数
	kyRes := mapf(replyBody.InputFile , string(content))
	// 根据ihash保存在对应的中间文件中
	kys := make([][]KeyValue, replyBody.NReduce) 
	for _ , v := range kyRes {
		kys[ihash(v.Key)%replyBody.NReduce] = append(kys[ihash(v.Key)%replyBody.NReduce],v)
	}
	for k, v := range kys { // 依次写入json中间文件
		filename := filePath + "/mr-" + strconv.Itoa(replyBody.TaskNum) + "-" + strconv.Itoa(k) + ".json"
		// 打开json文件
		fp, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			log.Fatal("cannot open %v", filename)
		}
		defer fp.Close()
		// 将kv键值对根据key值进行排序并写入
		sort.Sort(ByKey(v))				
		data , _ := json.MarshalIndent(v, "", "  ")	
		_, err = fp.Write(data)
		if err != nil {
			log.Fatal(err)
		}
	}
	// 通知master完成任务 
	finishreq := CompleteTaskArgs{replyBody.JobType , replyBody.TaskNum}
	finishreply := CompleteTask(finishreq)
	if finishreply.Resp == "fail" {
		log.Printf("任务失败")
	}
}

func performReduce(replyBody GetTaskReply, reducef func(string, []string) string){
	var kys []KeyValue // reduce输出
	for i:=0 ; i<replyBody.NMap ; i++{
		var t []KeyValue // 临时保存
		filename := filePath + "/mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(replyBody.TaskNum) + ".json"
		fp, err := os.OpenFile(filename, os.O_RDONLY, 0755)
		defer fp.Close()
		if err != nil {
			log.Fatal("cannot open %v", filename)
		}
		data := json.NewDecoder(fp)
		err = data.Decode(&t)
		if err != nil {
			log.Fatal(err)
		}
		kys = append(kys,t...)
	}
	// 对合并后的kys排序
	sort.Sort(ByKey(kys))
	// 创建输出文件
	filename := "./mr-out-" + strconv.Itoa(replyBody.TaskNum) + ".txt"
	file, err := os.Create(filename)
	if err!=nil{
		log.Printf("cannot create %v", filename)
	}
	defer file.Close()
	// 合并执行reduce
	i := 0
	for i < len(kys) {
		j := i + 1
		for j < len(kys) && kys[j].Key == kys[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kys[k].Value)
		}
		output := reducef(kys[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", kys[i].Key, output)
		i = j
	}
	// 通知master ------------ 
	finishreq := CompleteTaskArgs{replyBody.JobType , replyBody.TaskNum}
	finishreply := CompleteTask(finishreq)
	if finishreply.Resp == "fail" {
		log.Printf("任务失败")
	}
}

func GetTask(args GetTaskArgs) GetTaskReply {
	reply := GetTaskReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.WorkerGetTask", &args, &reply)
	return reply
}

func CompleteTask(args CompleteTaskArgs) CompleteTaskReply {
	reply := CompleteTaskReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.WorkerFinishTask", &args, &reply)
	return reply
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
