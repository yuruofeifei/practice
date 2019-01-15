// Contains the implementation of a LSP client.

package lsp

import (
    "errors"
    "encoding/json"
    "time"
    "github.com/cmu440/lspnet"
)

type item struct {
    msg *Message
    sent bool
    acked chan bool
}

type client struct {
    // TODO: implement this!
    conn *lspnet.UDPConn
    params *Params
    connid int
    connIDChan chan int
    ticker *time.Ticker
    rdata map[int][]byte
    ridx int
    slidingWindow map[int]*item
    swIdx int
    swBaseIdx int
    apiReadRequestChan chan bool
    apiReadResponseChan chan []byte
    apiWriteRequestChan chan []byte
    apiWriteResponseChan chan bool
    dataMsgChan chan *Message
    ackMsgChan chan *Message
    closeRequestChan chan bool
    closeConfirmChan chan bool
    countdown int
    pendingRead bool
    closing bool
    connected bool
    clientClosedChan chan bool
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {  
    raddr, err := lspnet.ResolveUDPAddr("udp", hostport)
    if err != nil {
        return nil, errors.New("Cannot resolve server addr")
    }
    conn, err := lspnet.DialUDP("udp", nil, raddr)
    if err != nil {
        return nil, errors.New("Cannot connect to server")
    }

    c := &client {
        conn: conn,
        params: params,
        connid: -1, 
        connIDChan: make(chan int, 1),
        ticker: time.NewTicker(time.Duration(params.EpochMillis) * time.Millisecond),
        rdata: make(map[int][]byte),
        ridx: 1,
        slidingWindow: make(map[int]*item),
        swIdx: 1,
        swBaseIdx: 1,
        apiReadRequestChan: make(chan bool),
        apiReadResponseChan: make(chan []byte),
        apiWriteRequestChan: make(chan []byte),
        apiWriteResponseChan: make(chan bool),
        dataMsgChan: make(chan *Message),
        ackMsgChan: make(chan *Message),
        closeRequestChan: make(chan bool),
        closeConfirmChan: make(chan bool, 1),
        countdown: params.EpochLimit,
        pendingRead: false,
        closing: false,
        connected: false,
        clientClosedChan: make(chan bool),
    } 

    go c.sendMsg(NewConnect())
    go c.mainRoutine()
    go c.readRoutine()
    
    return c, nil 
}

func (c *client) mainRoutine() {
    for {
        select {
            case <- c.ticker.C:
                c.countdown --
                if c.countdown == 0 { 
                    c.closing = true
                    c.slidingWindow = make(map[int]*item)
                }
                if !c.closing {
                     if !c.connected {
                        c.sendMsg(NewConnect())
                    } else {
                        go c.sendMsg(NewAck(c.connid, 0))
                    }
                }
            case ackMsg := <- c.ackMsgChan:
                if ackMsg.SeqNum == 0 {
                    if !c.connected {
                        c.connected = true
                        c.connid = ackMsg.ConnID
                        c.connIDChan <- c.connid
                        for i := c.swBaseIdx; i < c.swBaseIdx + c.params.WindowSize; i++ {
                            if w, found := c.slidingWindow[i]; found && !w.sent {
                                w.msg.ConnID = c.connid
                                go c.retrySend(w.msg, w.acked)
                                w.sent = true
                            }
                        }
                    }
                } else {                       
                    if v, ok := c.slidingWindow[ackMsg.SeqNum]; ok {
                        v.acked <- true
                        delete(c.slidingWindow, ackMsg.SeqNum)
                        if len(c.slidingWindow) == 0 {
                            c.swBaseIdx += c.params.WindowSize
                        } else {
                            for i := c.swBaseIdx; i < c.swIdx; i++ {
                                if _, found := c.slidingWindow[i]; found {
                                    c.swBaseIdx = i
                                    break
                                }
                            }
                        }
                        for i := c.swBaseIdx; i < c.swBaseIdx + c.params.WindowSize; i++ {
                            if w, found := c.slidingWindow[i]; found && !w.sent {
                                go c.retrySend(w.msg, w.acked)
                                w.sent = true
                            }
                        }
                    } 
                }
                if !c.closing {
                    c.countdown = c.params.EpochLimit
                }
            case dataMsg := <- c.dataMsgChan:
                if !c.closing {
                    go c.sendMsg(NewAck(c.connid, dataMsg.SeqNum))
                    if dataMsg.Size <= len(dataMsg.Payload) {
                        c.rdata[dataMsg.SeqNum] = dataMsg.Payload[:dataMsg.Size]
                    }
                    c.countdown = c.params.EpochLimit
                }
            case <- c.apiReadRequestChan:
                c.pendingRead = true
            case payload := <- c.apiWriteRequestChan:
                if _, found := c.slidingWindow[c.swIdx]; !found && !c.closing{
                    v := &item{msg: NewData(c.connid, c.swIdx, len(payload), payload, 0), sent: false, acked: make(chan bool, 1)}
                    c.slidingWindow[c.swIdx] = v
                    if c.connected && c.swBaseIdx + c.params.WindowSize > c.swIdx {
                        go c.retrySend(v.msg, v.acked)
                        v.sent = true
                    }
                    c.swIdx++
                    c.apiWriteResponseChan <- true
                    break
                }
                c.apiWriteResponseChan <- false
            case <- c.closeRequestChan:
                c.closing = true
            case <- c.clientClosedChan:
                return
        }
        if c.pendingRead {
            if payload, found := c.rdata[c.ridx]; found {
                c.pendingRead = false
                c.apiReadResponseChan <- payload
                delete(c.rdata, c.ridx)
                c.ridx ++
            } 
            if c.closing {
                c.apiReadResponseChan <- nil
            }
        }
        if len(c.slidingWindow) == 0 && c.closing {
            c.conn.Close()
            close(c.clientClosedChan)
            c.closeConfirmChan <- true
            return
        }
    }
}

func (c *client) readRoutine() {
    for {
        select {
        case <- c.clientClosedChan:
            return
        default:
            rdata := make([]byte, 1024)
            n, err := c.conn.Read(rdata[0:])
            if err == nil {
                var msg Message
                json.Unmarshal(rdata[:n], &msg)
                switch msg.Type {
                    case MsgData:
                        c.dataMsgChan <- &msg
                    case MsgAck:
                        c.ackMsgChan <- &msg
                }
            }

        }
    }
}

func(c *client) retrySend(msg *Message, ackedChan chan bool) {
    backoff := 1
    countdown := 0
    ticker := time.NewTicker(time.Duration(c.params.EpochMillis) * time.Millisecond)
    go c.sendMsg(msg)
    for {
        select {
            case <- ackedChan:
                return 
            case <- ticker.C:
                if countdown <= 0 {
                    go c.sendMsg(msg)
                    if backoff * 2 > c.params.MaxBackOffInterval {
                        backoff = c.params.MaxBackOffInterval 
                    } else {
                        backoff *= 2
                    }
                    countdown = backoff
                }
                countdown --
        }
    }
}

func (c *client) sendMsg(msg *Message) {
    MsgBytes, _ := json.Marshal(msg)
    c.conn.Write(MsgBytes)
}

func (c *client) ConnID() int {
    cid := <- c.connIDChan
    c.connIDChan <- cid
    return cid
}

func (c *client) Read() ([]byte, error) {
    c.apiReadRequestChan <- true
    response := <- c.apiReadResponseChan
    if response == nil {
        return nil, errors.New("Client failed to read...")
    }
    return response, nil
}

func (c *client) Write(payload []byte) error {
    c.apiWriteRequestChan <- payload
    succ := <- c.apiWriteResponseChan
    if !succ {
        return errors.New("Client failed to write...")
    }
    return nil
}

func (c *client) Close() error {
    c.closeRequestChan <- true
    <- c.closeConfirmChan
    return nil
}
