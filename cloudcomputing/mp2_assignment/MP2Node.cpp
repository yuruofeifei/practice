/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());

	if (curMemList.size() != ring.size()) {
		change = true;
	} else {
		auto it_ring = ring.begin();
		auto it_cml = curMemList.begin();
		for (; it_ring != ring.end() && it_cml != ring.end(); it_ring++, it_cml++) {
			if (!(it_cml->nodeAddress == it_ring->nodeAddress)) {
				change = true;
				break;
			}
		}
	}

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	if (change) {
		ring = std::move(curMemList);
		stabilizationProtocol();
	}
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

// coordinator dispatches messages to corresponding nodes
void MP2Node::dispatchMessages(Message &&message) {
	auto replicas = findNodes(message.key);
	for (int i = 0; i < replicas.size(); i++) {
		message.replica = static_cast<ReplicaType>(i);
		emulNet->ENsend(&memberNode->addr, &replicas[i].nodeAddress, message.toString());
	}
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */
	trans_logs[g_transID] = transaction(0, 2, par->getcurrtime(), CREATE, key, value);
	dispatchMessages(Message(g_transID++, memberNode->addr, CREATE, key, value));
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
	trans_logs[g_transID] = transaction(0, 2, par->getcurrtime(), READ, key, "");
	dispatchMessages(Message(g_transID++, memberNode->addr, READ, key, ""));
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */
	trans_logs[g_transID] = transaction(0, 2, par->getcurrtime(), UPDATE, key, value);
	dispatchMessages(Message(g_transID++, memberNode->addr, UPDATE, key, value));
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */
	trans_logs[g_transID] = transaction(0, 2, par->getcurrtime(), DELETE, key, "");
	dispatchMessages(Message(g_transID++, memberNode->addr, DELETE, key, ""));
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
	return ht->create(key, value);
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
	return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
	return ht->update(key, value);
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
	return ht->deleteKey(key);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);
		Message msg(message);
		/*
		 * Handle the message types here
		 */
		switch(msg.type) {
			case CREATE: {
				bool create_success = createKeyValue(msg.key, msg.value, msg.replica);
				if (create_success) {
					log->logCreateSuccess(&memberNode->addr, false, msg.transID, msg.key, msg.value);
				} else {
					log->logCreateFail(&memberNode->addr, false, msg.transID, msg.key, msg.value);
				}
				emulNet->ENsend(&memberNode->addr, &msg.fromAddr, 
					Message(msg.transID, memberNode->addr, REPLY, create_success).toString());
				break;
			}
			case READ: {
				string value = readKey(msg.key);
				if (!value.empty()) {
					log->logReadSuccess(&memberNode->addr, false, msg.transID, msg.key, value);
				} else {
					log->logReadFail(&memberNode->addr, false, msg.transID, msg.key);
				}
				emulNet->ENsend(&memberNode->addr, &msg.fromAddr, 
					Message(msg.transID, memberNode->addr, READREPLY, msg.key, value).toString());
				break;
			}
			case UPDATE: {
				bool update_success = updateKeyValue(msg.key, msg.value, msg.replica);
				if (update_success) {
					log->logUpdateSuccess(&memberNode->addr, false, msg.transID, msg.key, msg.value);
				} else {
					log->logUpdateFail(&memberNode->addr, false, msg.transID, msg.key, msg.value);
				}
				emulNet->ENsend(&memberNode->addr, &msg.fromAddr, 
					Message(msg.transID, memberNode->addr, REPLY, update_success).toString());
				break;
			}
			case DELETE: {
				bool delete_success = deletekey(msg.key);
				if (delete_success) {
					log->logDeleteSuccess(&memberNode->addr, false, msg.transID, msg.key);
				} else {
					log->logDeleteFail(&memberNode->addr, false, msg.transID, msg.key);
				}
				emulNet->ENsend(&memberNode->addr, &msg.fromAddr, 
					Message(msg.transID, memberNode->addr, REPLY, delete_success).toString());
				break;
			}
			case READREPLY: {
				if (!msg.value.empty() && trans_logs.count(msg.transID)) {
					trans_logs[msg.transID].success_count = trans_logs[msg.transID].success_count + 1;
					trans_logs[msg.transID].value = msg.value;
				}
				break;
			}
			case REPLY: {
				if (msg.success && trans_logs.count(msg.transID)) {
					trans_logs[msg.transID].success_count ++;
				}
				break;
			}
			default:
				break;
		}

	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
	for (auto it = trans_logs.begin(); it != trans_logs.end(); ) {
		transaction tc = it->second;
		if ((tc.success_count >= tc.target_count) && (tc.target_count == 2)) {
			switch (tc.msg_type) {
				case READ: 
					log->logReadSuccess(&memberNode->addr, true, it->first, tc.key, tc.value);
					break;
				case CREATE: 
					log->logCreateSuccess(&memberNode->addr, true, it->first, tc.key, tc.value);
					break;
				case UPDATE:
					log->logUpdateSuccess(&memberNode->addr, true, it->first, tc.key, tc.value);
					break;
				case DELETE:
					log->logDeleteSuccess(&memberNode->addr, true, it->first, tc.key);
					break;
				default:
					break;
			}
			it = trans_logs.erase(it);
		} else if (tc.success_count >= tc.target_count) {
			it = trans_logs.erase(it); // stablization update
		} else if (par->getcurrtime() - tc.timestamp >= TIME_OUT)  {
			if (tc.target_count > 0) {
				switch (tc.msg_type) {
					case READ: 
						log->logReadFail(&memberNode->addr, true, it->first, tc.key);
						break;
					case CREATE: 
						log->logCreateFail(&memberNode->addr, true, it->first, tc.key, tc.value);
						break;
					case UPDATE:
						log->logUpdateFail(&memberNode->addr, true, it->first, tc.key, tc.value);
						break;
					case DELETE:
						log->logDeleteFail(&memberNode->addr, true, it->first, tc.key);
						break;
					default:
						break;
				}
			}
			it = trans_logs.erase(it);
		} else {
			it ++;
		}

	}
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */
	auto i = std::distance(ring.begin(), find_if(ring.begin(), ring.end(), 
		[this](Node &node) { return node.nodeAddress == memberNode->addr; }));

	auto seconary = ring[(i + 1) % ring.size()], tertiary = ring[(i + 2) % ring.size()];
	vector<Node> update;
	if (hasMyReplicas.size() < 2 || !(seconary.nodeAddress == hasMyReplicas[0].nodeAddress) || !(seconary.nodeAddress == hasMyReplicas[1].nodeAddress)) {
		update.push_back(seconary);
	}
	if (hasMyReplicas.size() < 2 || !(tertiary.nodeAddress == hasMyReplicas[0].nodeAddress) || !(tertiary.nodeAddress == hasMyReplicas[1].nodeAddress)) {
		update.push_back(seconary);
	}
	if (!update.empty()) {
		for(auto & it : ht->hashTable) {
			for (int u = 0; u < update.size(); u++) {
				trans_logs[g_transID] = transaction(0, 1, par->getcurrtime(), CREATE, it.first, it.second);
				emulNet->ENsend(&memberNode->addr, &update[u].nodeAddress, Message(g_transID++, memberNode->addr, CREATE, it.first, it.second).toString());
			}
		}
		hasMyReplicas = std::move(update);
	}

	haveReplicasOf.clear();
	haveReplicasOf.push_back(ring[(i - 2) % ring.size()]);
	haveReplicasOf.push_back(ring[(i - 1) % ring.size()]);
}
