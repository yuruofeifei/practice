package main

import (
	"fmt"
	"os"
	"encoding/json"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

// Attempt to connect miner as a client to the server.
func joinWithServer(hostport string) (lsp.Client, error) {
	// TODO: implement this!
	client, err := lsp.NewClient(hostport, lsp.NewParams())
	return client, err
}

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport>", os.Args[0])
		return
	}

	hostport := os.Args[1]
	miner, err := joinWithServer(hostport)
	if err != nil {
		fmt.Println("Failed to join with server:", err)
		return
	}

	defer miner.Close()

	// TODO: implement this!
	connectBytes, _ := json.Marshal(bitcoin.NewJoin())
	err = miner.Write(connectBytes)
	if err != nil {
		return
	}
	for {
		readBytes, readErr := miner.Read()
		if readErr != nil {
			fmt.Println("Miner failed to read...")
			return
		}
		var result bitcoin.Message
		json.Unmarshal(readBytes, &result)

		msg := result.Data
		lower := result.Lower
		upper := result.Upper

		minHash := bitcoin.Hash(msg, lower)
		nounce := lower
		for i := lower; i <= upper; i++ {
			hash := bitcoin.Hash(msg, i)
			if hash < minHash {
				minHash = hash
				nounce = i
			} 
		}

		writeBytes, _ := json.Marshal(bitcoin.NewResult(minHash, nounce))
		writeErr := miner.Write(writeBytes)
		if writeErr != nil {
			fmt.Println("Miner failed to write...")
			return
		}
	}
}
