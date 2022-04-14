package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Add your RPC definitions here.
// worker请求参数
type GetTaskArgs struct {}
// coordinator返给worker的任务参数
type GetTaskReply struct {
	JobType		string	// 任务类型,map/reduce
	TaskNum		int		// 任务id,map时候是mapId,reduce时候是reduceId
	NReduce		int  	// reduce任务数量
	NMap		int		// map任务数量,也就是输入文件数
	InputFile 	string  // map时候是输入文件名
}
// worker完成任务的请求
type CompleteTaskArgs struct {
	JobType		string	// 任务类型,map/reduce
	TaskNum		int		// 任务id,map时候是mapId,reduce时候是reduceId
}
// master回应CompleteTaskArgs
type CompleteTaskReply struct {
	Resp  		string
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
