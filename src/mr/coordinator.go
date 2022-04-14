package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
// import "fmt"
import "sync"
import "time"

// 记录任务的相关信息
type JobState struct {
	TaskType	 string	    // 任务类型：map/reduce/done
	TaskId		 int		// 任务Id
	State 		 string	    // 任务执行状态:unexecuted/run/done
	StartTime	 time.Time  // 任务开始时间
}

type Coordinator struct {
	mu			 sync.Mutex // 互斥锁
	nReduce 	 int 	    // reduce的个数
	files		 []string   // 输入文件名
	mapJobs		 []JobState // 所有map任务
	reduceJobs	 []JobState // 所有reduce任务
	isDone		 bool		// 初始为false
}

// master给worker分配任务的函数
func (c *Coordinator) WorkerGetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 分配map任务
	for {
		mapdone := true
		for i, job := range c.mapJobs {
			switch job.State {
			case "unexecuted" :
				c.mapJobs[i].State = "run"
				c.mapJobs[i].StartTime = time.Now()
				*reply = GetTaskReply{"map" , i , c.nReduce , len(c.files) , c.files[i]}
				return nil
			case "run" : 
				if time.Since(job.StartTime).Seconds() > 10 {
					c.mapJobs[i].State = "run"
					c.mapJobs[i].StartTime = time.Now()
					*reply = GetTaskReply{"map" , i , c.nReduce , len(c.files) , c.files[i]}
					return nil
				}else{
					mapdone = false
				}
			default :
				continue	
			}
		}
		if !mapdone {
			// 继续循环
			continue
		}else {
			// map任务结束
			break
		}
	}

	
	// 分配reduce任务
	for {
		reducedone := true
		for i, job := range c.reduceJobs {
			switch job.State {
			case "unexecuted" :
				c.reduceJobs[i].State = "run"
				c.reduceJobs[i].StartTime = time.Now()
				*reply = GetTaskReply{"reduce" , i , c.nReduce , len(c.files) , ""}
				return nil
			case "run" : 
				if time.Since(job.StartTime).Seconds() > 10 {
					c.reduceJobs[i].State = "run"
					c.reduceJobs[i].StartTime = time.Now()
					*reply = GetTaskReply{"reduce" , i , c.nReduce , len(c.files) , ""}
					return nil
				}else{
					reducedone = false
				}
			}
		}
		if !reducedone {
			// 继续循环
			continue
		}else {
			// map任务结束
			break
		}
	}

	*reply = GetTaskReply{"done" , 0 , c.nReduce , len(c.files) , ""}
	c.isDone = true
	
	return nil
}

// worker申明作业完成
func (c *Coordinator) WorkerFinishTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 接受任务完成报告
	switch args.JobType {
	case "map" :
		for i, v := range c.mapJobs {
			if v.TaskId == args.TaskNum {
				if time.Since(v.StartTime).Seconds() <= 10 {
					c.mapJobs[i].State = "done"
					*reply = CompleteTaskReply{"success"}
					return nil
				}
			}
		}
	case "reduce" :
		for i, v := range c.reduceJobs {
			if v.TaskId == args.TaskNum {
				if time.Since(v.StartTime).Seconds() <= 10 {
					c.reduceJobs[i].State = "done"
					*reply = CompleteTaskReply{"success"}
					return nil
				}
			}
		}
	default :
		log.Printf("worker 任务错误")
		*reply = CompleteTaskReply{"fail"}
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
	c.mu.Lock()
	defer c.mu.Unlock()

	ret := c.isDone
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// 初始化master变量
	c.nReduce = nReduce
	c.files = files
	// 将jobstate初始化为unexecuted未执行状态
	c.mapJobs = make([]JobState, len(files))
	for i:=0; i<len(files); i++{
		c.mapJobs[i] = JobState{"map", i, "unexecuted", time.Time{}}
	}
	c.reduceJobs = make([]JobState, nReduce)
	for i:=0; i<nReduce; i++{
		c.reduceJobs[i] = JobState{"reduce", i, "unexecuted", time.Time{}}
	}
	c.isDone = false

	c.server()
	return &c
}
