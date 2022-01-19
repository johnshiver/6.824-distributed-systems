package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type jobType int

const (
	mapper jobType = iota + 1
	reducer
)

type Coordinator struct {
	mapJobs    []*Job
	reduceJobs []*Job
	nReduce    int

	logger zerolog.Logger
	mutex  sync.Mutex
}

type Job struct {
	ID uuid.UUID // used to track intermediate files + unique jobs

	InputFiles  []string
	OutputFiles []string
	NReduce     int

	Kind           jobType
	AssignedWorker uuid.UUID
	StartedAt      time.Time

	mutex sync.Mutex
	// TODO: to add finished at, need to expose method to update job after completion
}

func (j *Job) AssignToWorker(workerID uuid.UUID) {
	j.mutex.Lock()
	defer j.mutex.Unlock()

	j.AssignedWorker = workerID
	j.StartedAt = time.Now().UTC()
}

func (j *Job) IsAssigned() bool {
	j.mutex.Lock()
	defer j.mutex.Unlock()

	return j.AssignedWorker != uuid.Nil
}

func (j *Job) Unassign() {
	j.mutex.Lock()
	defer j.mutex.Unlock()

	j.AssignedWorker = uuid.Nil
	j.StartedAt = time.Time{}
}

func createOutputFileNames(nReduce int, jobID uuid.UUID, reduceInputs [][]string) []string {
	// TODO: worker will need access to this
	intermediateFileTemplate := "itmd-%s-%d"
	var outputs []string
	for i := 0; i < nReduce; i++ {
		name := fmt.Sprintf(intermediateFileTemplate, jobID, i)
		reduceInputs[i] = append(reduceInputs[i], name)
		outputs = append(outputs, name)
	}
	return outputs
}

// ------------------- TODO: could extract logic in Completed + Ready to separate function

func filesExist(files []string) bool {
	for _, f := range files {
		_, err := os.Stat(f)
		if errors.Is(err, os.ErrNotExist) {
			return false
		}

	}
	return true
}

// Completed checks if all job output files exist
func (j *Job) Completed() bool {
	return filesExist(j.OutputFiles)
}

// Ready checks to see if all job input files exist
func (j *Job) Ready() bool {
	return filesExist(j.InputFiles)
}

func (j *Job) String() string {
	return fmt.Sprintf("job: type: %d id: %s assigned to worker: %s completed: %v", j.Kind, j.ID, j.AssignedWorker, j.Completed())
}

func (c *Coordinator) GetWork(request *GetWorkRequest, response *GetWorkResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.logger.Debug().Msgf("received GetWork request from worker: %s", request.WorkerID)

	/*
		Workers will sometimes need to wait,
		e.g. reduces can't start until the last map has finished.
		One possibility is for workers to periodically ask the coordinator for work,
		sleeping with time.Sleep() between each request. Another possibility is for the
		relevant RPC handler in the coordinator to have a loop that waits, either with
		time.Sleep() or sync.Cond. Go runs the handler for each RPC in its own thread,
		so the fact that one handler is waiting won't prevent the coordinator from processing
		other RPCs. look for unassigned work and assign it
	*/
	for _, j := range c.mapJobs {
		if !j.IsAssigned() && !j.Completed() && j.Ready() {
			j.AssignToWorker(request.WorkerID)
			response.InputFiles = j.InputFiles
			response.OutputFiles = j.OutputFiles
			response.ID = j.ID
			response.Kind = j.Kind
			c.logger.Debug().Msgf("%s", j.String())
			return nil
		}
	}

	// if no map jobs, try reduce job
	for _, j := range c.reduceJobs {
		if !j.IsAssigned() && !j.Completed() && j.Ready() {
			j.AssignToWorker(request.WorkerID)
			response.InputFiles = j.InputFiles
			response.OutputFiles = j.OutputFiles
			response.ID = j.ID
			response.Kind = j.Kind
			c.logger.Debug().Msgf("%s", j.String())
			return nil
		}
	}

	return fmt.Errorf("no jobs available")
}

// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	err := rpc.Register(c)
	if err != nil {
		c.logger.Fatal().Err(err)
	}
	rpc.HandleHTTP()

	// NOTE: if you want to run on different machines, uncomment
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)

	ticker := time.NewTicker(time.Second * 10)
	go func() {
		c.logger.Debug().Msg("starting background checker")
		for {
			select {
			case <-ticker.C:
				c.checkJobs()
			}
		}
	}()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, j := range c.mapJobs {
		if !j.Completed() {
			return false
		}
	}

	for _, j := range c.reduceJobs {
		if !j.Completed() {
			return false
		}
	}
	return true
}

// a function that can be used to periodically check the state of jobs
// if job is assigned / has been idle for a while, cancel and re-assign
func (c *Coordinator) checkJobs() {
	// NOTE: might separate into read + write locks
	//c.mutex.Lock()
	//defer c.mutex.Unlock()

	// check for in progress jobs that are taking too long
	c.logger.Debug().Msg("checking for stalled map jobs")
	var mComplete, rComplete int

	// map jobs
	for _, j := range c.mapJobs {
		c.logger.Debug().Msgf("%s", j.String())
		if j.Completed() {
			mComplete++
			continue
		}
		if j.IsAssigned() && !j.Completed() && time.Since(j.StartedAt) > time.Second*10 {
			c.logger.Debug().Msgf("%s is taking too long to complete, unassigning", j)
			j.Unassign()
		}

		if j.IsAssigned() && !j.Completed() {
			c.logger.Debug().Msgf("map %s is assigned but not completed", j)
		}

	}

	c.logger.Debug().Msg("checking for stalled reduce jobs")
	// TODO: looks like reduce jobs arent getting re-assigned
	// reduce jobs
	for _, j := range c.reduceJobs {
		c.logger.Debug().Msgf("%s", j.String())
		if j.Completed() {
			rComplete++
			continue
		}
		if j.IsAssigned() && !j.Completed() && time.Since(j.StartedAt) > time.Second*10 {
			c.logger.Debug().Msgf("%s is taking too long to complete, unassigning", j)
			j.Unassign()
		}
		if j.IsAssigned() && !j.Completed() {
			c.logger.Debug().Msgf("reduce %s is assigned but not completed", j)
		}
	}

	totalJobs := len(c.mapJobs) + len(c.reduceJobs)
	c.logger.Debug().Msgf("%d map jobs complete %d reduce jobs complete total jobs %d", mComplete, rComplete, totalJobs)
}

// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	logger := zerolog.New(os.Stderr).With().Timestamp().Str("service", "coordinator").Logger()

	logger.Debug().Msgf("creating coordinator with inputs: %s nReduce: %d", files, nReduce)

	c := Coordinator{nReduce: nReduce, logger: logger}

	reduceInputsByPartition := make([][]string, nReduce)
	maxFilesPerMapper := len(files) / nReduce
	logger.Debug().Msgf("maxFilesPerMapper: %d", maxFilesPerMapper)
	// create map jobs from input files
	var inputs []string
	for _, f := range files {
		inputs = append(inputs, f)
		jobID := uuid.New()
		outputs := createOutputFileNames(nReduce, jobID, reduceInputsByPartition)
		if len(inputs) >= maxFilesPerMapper {
			logger.Debug().Msgf("creating reduce job: %s outputs: %s", inputs, outputs)
			mj := Job{
				ID:          jobID,
				NReduce:     nReduce,
				InputFiles:  inputs,
				OutputFiles: outputs,
				Kind:        mapper,
			}
			c.mapJobs = append(c.mapJobs, &mj)
			inputs = nil
		}
	}

	// create jobs for each reduce task
	// each reduce task will pick up jobs related to key files
	for n := 0; n < nReduce; n++ {
		jobID := uuid.New()
		rj := Job{
			ID:          jobID,
			NReduce:     nReduce,
			InputFiles:  reduceInputsByPartition[n], // reduce jobs take a partition, each mapper writes to multiple partitions
			OutputFiles: []string{fmt.Sprintf("reduce-%d", n)},
			Kind:        reducer,
		}
		logger.Debug().Msgf("creating map job: %s outputs: %s", rj.InputFiles, rj.OutputFiles)
		c.reduceJobs = append(c.reduceJobs, &rj)
	}

	c.server()
	return &c
}
