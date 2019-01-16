package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"encoding/json"
	"container/list"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

const (
	unitWorkload=1000000
)

type server struct {
	lspServer lsp.Server
	serverClosingChan chan bool
	rcvMsgQChan chan *list.List
	minersMap map[int]*list.Element
	clientsResultsMap map[int]*Result
	freeL *list.List
	busyL *list.List
	jobL *list.List
	jobsMap map[int]*Job
	jobid int
}

type Result struct {
	empty bool
	hash uint64
	nonce uint64
	nUnfinished int
}

type miner struct {
	mid int
	working bool
	jobid int
}

type Job struct {
	cid int
	jobid int
	data string
	lower uint64
	upper uint64
}

type Msg struct {
	connid int
	payload []byte
	err error
}

func startServer(port int) (*server, error) {
	// TODO: implement this!
	lspSrv, err := lsp.NewServer(port, lsp.NewParams())
	if err != nil {
		return nil, err
	}
	srv := &server{
		lspServer: lspSrv, 
		serverClosingChan: make(chan bool),
		rcvMsgQChan: make(chan *list.List, 1),
		minersMap: make(map[int]*list.Element),
		clientsResultsMap: make(map[int]*Result),
		freeL: list.New(),
		busyL: list.New(),
		jobL: list.New(),
		jobsMap: make(map[int]*Job),
	}
	srv.rcvMsgQChan <- list.New()
	return srv, nil
}

func (s *server) readRoutine() {
	for {
		select {
			case <- s.serverClosingChan:
				return
			default:
				connid, payload, err := s.lspServer.Read()
				q := <- s.rcvMsgQChan
				q.PushBack(&Msg{connid: connid, payload: payload, err:err})
				s.rcvMsgQChan <- q
		}
	}
}

// similar to cpu round robin slicing
func (s *server) mainRoutine() {
	for {
		select {
			case <- s.serverClosingChan:
				return
			case rcvQ := <- s.rcvMsgQChan:
				if rcvQ.Len() > 0 {
 					head := rcvQ.Front()
 					rcvQ.Remove(head)
 					m := head.Value.(*Msg)
					connid := m.connid
					payload := m.payload
					err := m.err
					if err != nil { // miner or client connection lost
						if minerE, ok := s.minersMap[connid]; ok {
							miner := minerE.Value.(*miner)
							jobid := miner.jobid
							if miner.working {
								s.busyL.Remove(minerE)
								s.jobL.PushBack(s.jobsMap[jobid])
							} else {
								s.freeL.Remove(minerE)
							}
							delete(s.minersMap, connid)
						} else {
							delete(s.clientsResultsMap, connid)
						}
						s.lspServer.CloseConn(connid)
					} else {
						var msg bitcoin.Message
						json.Unmarshal(payload, &msg)
						switch msg.Type {
							case bitcoin.Join:
								s.freeL.PushBack(&miner{working: false, mid: connid})
								s.minersMap[connid] = s.freeL.Back()
							case bitcoin.Request:
								data := msg.Data
								lower := msg.Lower
								upper := msg.Upper
								s.clientsResultsMap[connid] = &Result{hash:0, nonce:0, empty:true}
								count := 0
								for i := lower; i < upper; i += unitWorkload {
									u := i + unitWorkload
									if upper < u {
										u = upper
									}
									job := &Job{jobid: s.jobid, data: data, lower: i, upper: u, cid: connid}
									s.jobL.PushBack(job)
									s.jobsMap[s.jobid] = job
									s.jobid++
									count++
								}
								s.clientsResultsMap[connid].nUnfinished = count
							case bitcoin.Result:
								hash := msg.Hash
								nonce := msg.Nonce
								minerE := s.minersMap[connid]
								miner := minerE.Value.(*miner)
								jobid := miner.jobid 
								job := s.jobsMap[jobid]
								cid := job.cid
								s.busyL.Remove(minerE)
								miner.working = false
								miner.jobid = -1
								s.freeL.PushBack(miner)
								delete(s.jobsMap, jobid)
								if partialResult, ok := s.clientsResultsMap[cid]; ok {
									if partialResult.empty || hash < partialResult.hash {
										partialResult.nonce = nonce
										partialResult.hash = hash
										partialResult.empty = false
									}
									partialResult.nUnfinished--
									if partialResult.nUnfinished == 0 {
										resultMsg := bitcoin.NewResult(partialResult.hash, partialResult.nonce)
										payload, _ := json.Marshal(resultMsg)
										s.lspServer.Write(cid, payload)
										delete(s.clientsResultsMap, cid)
									}
								}
						}
					}
				}
				s.rcvMsgQChan <- rcvQ
		}
		for s.jobL.Len() > 0 {
			head := s.jobL.Front()
			job := head.Value.(*Job)
			if _, ok := s.clientsResultsMap[job.cid]; ok {
				if s.freeL.Len() > 0 {
					minerE := s.freeL.Front()
					s.freeL.Remove(minerE)
					miner := minerE.Value.(*miner)
					s.minersMap[miner.mid] = minerE
					miner.working = true 
					miner.jobid = job.jobid
					s.busyL.PushBack(miner)
					requestMsg := bitcoin.NewRequest(job.data, job.lower, job.upper)
					payload, _ := json.Marshal(requestMsg)
					s.lspServer.Write(miner.mid, payload)
					s.jobL.Remove(head)
				} else {
					break
				}
			} else {
				s.jobL.Remove(head)
			}
		}
	}
}

var LOGF *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name = "log.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	// Usage: LOGF.Println() or LOGF.Printf()

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <port>", os.Args[0])
		return
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port must be a number:", err)
		return
	}

	srv, err := startServer(port)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("Server listening on port", port)

	defer srv.lspServer.Close()
	defer close(srv.serverClosingChan)

	// TODO: implement this!
	go srv.readRoutine()
	srv.mainRoutine()	
}
