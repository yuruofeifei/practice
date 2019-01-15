// Contains the implementation of a LSP server.

package lsp

import (
    "errors"
    "encoding/json"
    "time"
    "strconv"
    "github.com/cmu440/lspnet"
)

type cliMsg struct {
    msg *Message
    addr *lspnet.UDPAddr
}

type cli struct {
    closing bool
    addr *lspnet.UDPAddr
}

type server struct {
    // TODO: implement this!
    conn *lspnet.UDPConn
    cid int
    connMap map[int]*cli
    ticker *time.Ticker
    params *Params
    countdownMap map[int]int 
    rdataMap map[int]map[int]*Message
    ridxMap map[int]int
    slidingWindowMap map[int]map[int]*item
    swIdxMap map[int]int
    swBaseIdxMap map[int]int
    dataMsgChan chan *cliMsg
    ackMsgChan chan *cliMsg
    connectMsgChan chan *cliMsg
    apiWriteRequestChan chan *Message
    apiReadRequestChan chan bool
    apiReadResponseChan chan *Message
    readQ []*Message
    pendingRead bool
    closing bool
    clientsLost bool
    closeClientChan chan int
    baseIdx int
    closeRequestChan chan bool
    closeStatusChan chan bool
    serverClosedChan chan bool
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
    hostport := lspnet.JoinHostPort("localhost", strconv.Itoa(port))
    raddr, err := lspnet.ResolveUDPAddr("udp", hostport)
    if err != nil {
        return nil, errors.New("Cannot resolve server addr")
    }
    conn, err := lspnet.ListenUDP("udp", raddr)
    if err != nil {
        return nil, errors.New("Cannot connect to server")
    }

    s := &server {
        connMap: make(map[int]*cli),
        conn: conn,
        ticker: time.NewTicker(time.Duration(params.EpochMillis) * time.Millisecond),
        params: params,
        countdownMap: make(map[int]int),
        rdataMap: make(map[int]map[int]*Message),
        ridxMap: make(map[int]int),
        slidingWindowMap: make(map[int]map[int]*item),
        swIdxMap: make(map[int]int),
        swBaseIdxMap: make(map[int]int),
        dataMsgChan: make(chan *cliMsg),
        ackMsgChan: make(chan *cliMsg),
        connectMsgChan: make(chan *cliMsg),
        apiWriteRequestChan: make(chan *Message),
        apiReadRequestChan: make(chan bool),
        apiReadResponseChan: make(chan *Message),
        readQ: make([]*Message, 0),
        pendingRead: false,
        closing: false,
        clientsLost: false,
        closeClientChan: make(chan int),
        closeRequestChan: make(chan bool),
        closeStatusChan: make(chan bool),
        serverClosedChan: make(chan bool),
    } 

    go s.mainRoutine()
    go s.readRoutine()
    
    return s, nil 
}

func (s *server) mainRoutine() {
    for {
        select {
            case <- s.ticker.C:
                for cid, cli := range s.connMap {
                    s.countdownMap[cid]--
                    if s.countdownMap[cid] == 0 {
                        if s.closing {
                            s.clientsLost = true
                        }
                        delete(s.connMap, cid)
                        s.readQ = append(s.readQ, NewData(cid, -1, 0, nil, 0))
                    } else if !s.closing {
                        go s.sendMsg(NewAck(cid, 0), cli.addr)
                    }
                }
            case connectMsg := <- s.connectMsgChan:
                addr := connectMsg.addr
                s.connMap[s.cid] = &cli{addr: addr, closing: false}
                s.rdataMap[s.cid] = make(map[int]*Message)
                s.ridxMap[s.cid] = 1 
                s.slidingWindowMap[s.cid] = make(map[int]*item)
                s.swIdxMap[s.cid] = 1
                s.swBaseIdxMap[s.cid] = 1
                s.countdownMap[s.cid] = s.params.EpochLimit
                go s.sendMsg(NewAck(s.cid, 0), addr)
                s.cid ++
            case dataMsg := <- s.dataMsgChan:
                if !s.closing { 
                    msg := dataMsg.msg
                    addr := dataMsg.addr
                    connid := msg.ConnID
                    if cli, ok := s.connMap[connid]; ok && !cli.closing {
                        go s.sendMsg(NewAck(connid, msg.SeqNum), addr)
                        if msg.Size <= len(msg.Payload) {
                            msg.Payload = msg.Payload[:msg.Size]
                            s.rdataMap[connid][msg.SeqNum] = msg
                            i := s.ridxMap[connid]
                            for ; true; i++ {
                                if v, ok := s.rdataMap[connid][i]; ok {
                                    s.readQ = append(s.readQ, v)
                                } else {
                                    break
                                }
                            }
                            s.ridxMap[connid] = i
                        }
                        s.countdownMap[connid] = s.params.EpochLimit
                    }
                }
            case ackMsg := <- s.ackMsgChan:
                msg := ackMsg.msg
                if cli, connected := s.connMap[msg.ConnID]; connected {
                    slidingWindow := s.slidingWindowMap[msg.ConnID]
                    if v, found := slidingWindow[msg.SeqNum]; found {
                        baseIdx := s.swBaseIdxMap[msg.ConnID]
                        swIdx := s.swIdxMap[msg.ConnID]
                        v.acked <- true
                        delete(slidingWindow, msg.SeqNum)
                        if len(slidingWindow) == 0 {
                            baseIdx += s.params.WindowSize
                        } else {
                            for i := baseIdx; i < swIdx; i++ {
                                if _, swok := slidingWindow[i]; swok {
                                    baseIdx = i
                                    break
                                }
                            }
                        }
                        s.swBaseIdxMap[msg.ConnID] = baseIdx
                        for i := baseIdx; i < baseIdx + s.params.WindowSize; i++ {
                            if w, found := slidingWindow[i]; found && !w.sent {
                                go s.retrySend(w.msg, w.acked, cli.addr)
                                w.sent = true
                            }
                        }
                    } 
                    if len(slidingWindow) == 0 && cli.closing {
                        delete(s.connMap, msg.ConnID)
                        s.readQ = append(s.readQ, NewData(msg.ConnID, -1, 0, nil, 0))
                    }
                    if !s.closing {
                        s.countdownMap[msg.ConnID] = s.params.EpochLimit
                    }
                }
            case <- s.apiReadRequestChan:
                s.pendingRead = true
            case msg := <- s.apiWriteRequestChan:
                if !s.closing {
                    connid := msg.ConnID
                    if cli, connected := s.connMap[connid]; connected && !cli.closing {
                        swIdx := s.swIdxMap[connid]
                        if _, found := s.slidingWindowMap[connid][swIdx]; !found {
                            msg.SeqNum = swIdx
                            v := &item{msg: msg, sent: false, acked: make(chan bool, 1)}
                             s.slidingWindowMap[connid][swIdx] = v
                            if s.swBaseIdxMap[connid] + s.params.WindowSize > swIdx {
                                go s.retrySend(v.msg, v.acked, cli.addr)
                                v.sent = true
                            }
                            s.swIdxMap[connid] = swIdx + 1
                        }
                    }
                }
            case cid := <- s.closeClientChan:
                if cli, ok := s.connMap[cid]; ok {
                    cli.closing = true
                }
            case <- s.closeRequestChan:
                s.closing = true
                for _, cli := range s.connMap {
                    cli.closing = true
                }
            case <- s.serverClosedChan:
                return
        }
        if s.pendingRead {
            if len(s.readQ) > 0 {
                s.pendingRead = false
                s.apiReadResponseChan <- s.readQ[0]
                s.readQ = s.readQ[1:]
            }
        }
        if len(s.connMap) == 0 && s.closing {
            s.conn.Close()
            close(s.serverClosedChan)
            s.closeStatusChan <- s.clientsLost
        }
    }
}

func (s *server) readRoutine() {
    for {
        select {
        case <- s.serverClosedChan:
            return
        default:
            rdata := make([]byte, 1024)
            n, addr, err := s.conn.ReadFromUDP(rdata[0:])
            if err == nil {
                var msg Message
                json.Unmarshal(rdata[:n], &msg)
                rcvMsg := &cliMsg{msg: &msg, addr: addr}
                switch msg.Type {
                case MsgData:
                    s.dataMsgChan <- rcvMsg
                case MsgAck:
                    s.ackMsgChan <- rcvMsg
                case MsgConnect:
                    s.connectMsgChan <- rcvMsg
                }
            }
        }
    }
}

func(s *server) retrySend(msg *Message, ackedChan chan bool, addr *lspnet.UDPAddr) {
    backoff := 1
    countdown := 0
    ticker := time.NewTicker(time.Duration(s.params.EpochMillis) * time.Millisecond)
    go s.sendMsg(msg, addr)
    for {
        select {
            case <- ackedChan:
                return 
            case <- ticker.C:
                if countdown <= 0 {
                    go s.sendMsg(msg, addr)
                    if backoff * 2 > s.params.MaxBackOffInterval {
                        backoff = s.params.MaxBackOffInterval 
                    } else {
                        backoff *= 2
                    }
                    countdown = backoff
                }
                countdown --
        }
    }
}

func (s *server) sendMsg(msg *Message, addr *lspnet.UDPAddr) {
    MsgBytes, _ := json.Marshal(msg)
    s.conn.WriteToUDP(MsgBytes, addr)
}

func (s *server) Read() (int, []byte, error) {
    s.apiReadRequestChan <- true
    msg := <- s.apiReadResponseChan
    var err error
    if msg.SeqNum == -1 {
        err = errors.New("Failed to read...")
    }
    return msg.ConnID, msg.Payload, err
}

func (s *server) Write(connID int, payload []byte) error {
    s.apiWriteRequestChan <- NewData(connID, 0, len(payload), payload, 0)
    return nil
}

func (s *server) CloseConn(connID int) error {
    s.closeClientChan <- connID
    return nil
}

func (s *server) Close() error {
    s.closeRequestChan <- true
    closeStatus := <- s.closeStatusChan
    if !closeStatus {
        return errors.New("One or more clients lost...")
    }
    return nil
}
