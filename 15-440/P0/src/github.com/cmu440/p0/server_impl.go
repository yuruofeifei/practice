// Implementation of a KeyValueServer. Students should write their code in this file.

package p0

import (
    "net"
    "strconv"
    "fmt"
    "bufio"
    "strings"
    "io"
)

const (
	// client message buffer size
	MAX_CLIENT_MSG_BUF = 500

	// kv server action
	PUT = "put"
	GET = "get"
	DEL = "delete"
)

type keyValueServer struct {
    // TODO: implement this!
    ln net.Listener
    clients_list_chan chan map[int]*client
    client_request_buffer chan *client_request
    close_server_chan chan int
    close_client_chan chan int
	active_count_chan chan int
    dropped_count_chan chan int
}

type client struct {
	c_id int
	conn net.Conn
	dead chan int
	response_buffer chan []byte
}

type client_request struct {
	cli *client
	msg []string
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
    // TODO: ivmplement this!
    return &keyValueServer{
    	nil, // net.Listener 
    	make(chan map[int]*client, 1), // clients_list(map) channel
    	make(chan *client_request, 1), // client_request_buffer channel
    	make(chan int, 1), // close_server channel
    	make(chan int, 1), // close_client channel
    	make(chan int, 1), // active_count channel
    	make(chan int, 1), // dropped_count channel
    }
}

func (kvs *keyValueServer) Start(port int) error {
    // TODO: implement this!
    ln, err := net.Listen("tcp", ":" + strconv.Itoa(port))
    if err != nil {
        fmt.Printf("Failed to start server!")
        return err
    }

    init_db()

    kvs.ln = ln

    go start_server(kvs)
    go join_client(kvs)

    return nil
}

func (kvs *keyValueServer) Close() {
    // TODO: implement this!
    kvs.close_server_chan <- 1
}

func (kvs *keyValueServer) CountActive() int {
    // TODO: implement this!
    active_count := <- kvs.active_count_chan
    kvs.active_count_chan <- active_count

    return active_count
}


func (kvs *keyValueServer) CountDropped() int {
    // TODO: implement this!

    dropped_count := <- kvs.dropped_count_chan
    kvs.dropped_count_chan <- dropped_count

    return dropped_count
}

// TODO: add additional methods/functions below!
func start_server(kvs *keyValueServer) {
    kvs.clients_list_chan <- make(map[int]*client)
    kvs.active_count_chan <- 0
    kvs.dropped_count_chan <- 0
	for {
		select {
			case <- kvs.close_server_chan:
				clients_list := <- kvs.clients_list_chan
				for c_id := range clients_list {
					clients_list[c_id].conn.Close()
				}
				kvs.clients_list_chan <- nil
				kvs.ln.Close()
				return 
			case c_id := <- kvs.close_client_chan:
				clients_list := <- kvs.clients_list_chan
				client, ok := clients_list[c_id]
				delete(clients_list, c_id)
				kvs.clients_list_chan <- clients_list
				if ok {
					client.dead <- 1
					client.conn.Close()
					active_count := <- kvs.active_count_chan
					kvs.active_count_chan <- active_count - 1
					dropped_count := <- kvs.dropped_count_chan 
					kvs.dropped_count_chan <- dropped_count + 1
				}
			case cli_request := <- kvs.client_request_buffer: 
				key := cli_request.msg[1]
				switch cli_request.msg[0] {
					case PUT: 
			        	put(key, []byte(cli_request.msg[2]))
		            case GET:
		                send_to_client(cli_request.cli, key, get(key))
		            case DEL:
		                clear(key)
		        }
		}
	}
	
}

func send_to_client(cli *client, key string, values []([]byte)) {
	for _, v := range values {
		select {
			case cli.response_buffer <- []byte(fmt.Sprintf("%s,%s\n", key, string(v))):
		    default:
		        fmt.Println("Client response buffer reach limit!")
		}
	}
}

func join_client(kvs *keyValueServer) {
	client_id := 0
    for {
        conn, err := kvs.ln.Accept()
        if err != nil {
            fmt.Println("Client failed to join.")
            return
        }

        active_count := <- kvs.active_count_chan
        new_client := &client{conn: conn, c_id: client_id, dead: make(chan int, 1), response_buffer: make(chan []byte, MAX_CLIENT_MSG_BUF)} 
        
        clients_list := <- kvs.clients_list_chan
        clients_list[client_id] = new_client
        kvs.clients_list_chan <- clients_list

        client_id ++
        kvs.active_count_chan <- active_count + 1

        go process_client_read(kvs, new_client)
        go process_client_write(kvs, new_client)
    }
}

func process_client_read(kvs *keyValueServer, cli *client) {
    for {
    	select {
    		case <- cli.dead:
    			return
	    	default:
	    		msg := <- cli.response_buffer
				cli.conn.Write(msg)
    	}
    	
    }
}

func process_client_write(kvs *keyValueServer, cli *client) {
	reader := bufio.NewReader(cli.conn)
    for {
    	select {
    		case <- cli.dead:
    			return
    		default:
    			message, err := reader.ReadString('\n')
		        if err == io.EOF {
					kvs.close_client_chan <- cli.c_id
		        } else if err != nil {
		        	return
		        } else {

					message = strings.TrimSuffix(message, "\n")
				    kvs.client_request_buffer <- &client_request{cli: cli, msg: strings.Split(message, ",")}
				}
    	}
    	
    }
}