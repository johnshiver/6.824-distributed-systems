package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func getReduceTaskFile(key string, nReduce int) string {
	reduceID := ihash(key) % nReduce
	return fmt.Sprintf("reduce-task-%d", reduceID)
}

/* main/mrworker.go calls this function.

Workers will sometimes need to wait, e.g. reduces can't start until
the last map has finished. One possibility is for workers to periodically
ask the coordinator for work, sleeping with time.Sleep() between each request.
Another possibility is for the relevant RPC handler in the coordinator to have a loop
that waits, either with time.Sleep() or sync.Cond. Go runs the handler for each RPC in
its own thread, so the fact that one handler is waiting won't prevent the coordinator
from processing other RPCs.

To ensure that nobody observes partially written files in the presence of crashes,
the MapReduce paper mentions the trick of using a temporary file and atomically renaming
it once it is completely written. You can use ioutil.TempFile to create a temporary file
and os.Rename to atomically rename it.

*/
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// TODO: add recovery if possible to clean up job

	workerID := uuid.New()
	logger := zerolog.New(os.Stderr).With().Timestamp().Str("workerID", workerID.String()).Logger()

	logger.Debug().Msg("starting worker")
	logger.Debug().Msg("getting work")
	w, err := GetWork(workerID)
	if err != nil {
		logger.Fatal().Msgf("while calling Coordinator.GetWork: %s", err)
	}

	if filesExist(w.OutputFiles) {
		logger.Fatal().Msg("job already complete, exiting")
		return
	}
	for !filesExist(w.InputFiles) {
		logger.Info().Msgf("job %s is not ready, sleeping 10 seconds", w.ID)
		time.Sleep(time.Second * 10)
	}

	// TODO: rather than outputs, create temp file
	outputs := make(map[int][]KeyValue)

	switch w.Kind {
	case mapper:
		logger.Debug().Msgf("worker: %s starting map job: %s", workerID, w.ID)
		for _, f := range w.InputFiles {
			logger.Debug().Msgf("reading file: %s", f)
			content, err := ioutil.ReadFile(f)
			if err != nil {
				logger.Fatal().Msgf("cannot read input file %s: %s", f, err)
			}
			results := mapf(f, string(content))
			// put results in the correct bucket
			for _, r := range results {
				//bucket := ihash(r.Key) % j.NReduce
				bucket := ihash(r.Key) % 10
				outputs[bucket] = append(outputs[bucket], r)
			}
		}
		// write output files
		for bucket, results := range outputs {
			buff := bytes.NewBuffer(nil)
			enc := json.NewEncoder(buff)
			for _, r := range results {
				err := enc.Encode(&r)
				if err != nil {
					logger.Error().Err(err)
				}
			}
			if filesExist(w.OutputFiles) {
				logger.Debug().Msg("job already done, finishing")
				return
			}
			if err = ioutil.WriteFile(fmt.Sprintf("itmd-%s-%d", w.ID, bucket), buff.Bytes(), 0444); err != nil {
				logger.Fatal().Msgf("while writing output file: %s", err)
			}
		}
		// TODO: need to write any remaining output files, in case nothing was written to that partition
	case reducer:
		logger.Debug().Msgf("worker: %s starting reducer job: %s", workerID, w.ID)
		var kvs []KeyValue
		i := 0
		for _, f := range w.InputFiles {
			logger.Debug().Msgf("reading file: %s", f)
			file, err := os.Open(f)
			if err != nil {
				logger.Fatal().Msgf("while opening input file: %s", err)
			}
			defer file.Close()
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kvs = append(kvs, kv)
			}
		}

		logger.Debug().Msg("sorting keys")
		sort.Sort(ByKey(kvs))
		buff := bytes.NewBuffer(nil)
		i = 0
		for i < len(kvs) {
			j := i + 1
			for j < len(kvs) && kvs[j].Key == kvs[i].Key {
				j++
			}
			var values []string
			for k := i; k < j; k++ {
				values = append(values, kvs[k].Value)
			}
			output := reducef(kvs[i].Key, values)
			_, _ = fmt.Fprintf(buff, "%v %v\n", kvs[i].Key, output)
			i = j
		}
		if err = ioutil.WriteFile(w.OutputFiles[0], buff.Bytes(), 0444); err != nil {
			logger.Fatal().Msgf("while writing output file: %s", err)
		}
	}

	logger.Debug().Msgf("%s is complete", w.ID)
}

func GetWork(id uuid.UUID) (GetWorkResponse, error) {
	var response GetWorkResponse
	request := GetWorkRequest{WorkerID: id}
	err := call("Coordinator.GetWork", &request, &response)
	if err != nil {
		return GetWorkResponse{}, err
	}
	return response, nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	return c.Call(rpcname, args, reply)
}
