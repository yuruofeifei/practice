/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <iostream>

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = TREMOVE;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    int id = *(int *)(memberNode->addr.addr);
    short port = *(short *)(memberNode->addr.addr + 4);
    memberNode->memberList.push_back(MemberListEntry(id, port, memberNode->heartbeat, par->getcurrtime()));

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        std::cout << "Starting up group... " << memberNode->addr.getAddress() << std::endl;
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + 1 * sizeof(MemberListEntry);
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));
        msg->msgType = JOINREQ;
        msg->n_msg = 1;
        memcpy((MemberListEntry *)(msg+1), &memberNode->memberList[0], sizeof(MemberListEntry));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

std::string to_addr(int id, short port) {
    return to_string(id) + ":" + to_string(port);
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
    
    MessageHdr *msg = (MessageHdr *)data;
    MemberListEntry *peer_member_list = (MemberListEntry *)(msg + 1);
    switch (msg->msgType) {
        case JOINREQ: {
            size_t msgsize = sizeof(MessageHdr);
            MessageHdr *rep_msg = (MessageHdr *) malloc(msgsize * sizeof(char));
            rep_msg->msgType = JOINREP;
            rep_msg->n_msg = 0;
            Address dst_addr(to_addr(peer_member_list[0].getid(), peer_member_list[0].getport()));
            emulNet->ENsend(&memberNode->addr, &dst_addr, (char *)rep_msg, msgsize);
            // no break here to update membership list
        }
        case HB: {
            for (int i = 0; i < msg->n_msg; i++) {
                MemberListEntry peer_mle = peer_member_list[i];
                bool new_member = true;
                for (MemberListEntry &local_mle: memberNode->memberList) {
                    if (local_mle.getid() == peer_mle.getid() && local_mle.getport() == peer_mle.getport()) {
                        if (local_mle.getheartbeat() < peer_mle.getheartbeat()) {
                            local_mle.setheartbeat(peer_mle.getheartbeat());
                            local_mle.settimestamp(par->getcurrtime());
                        }
                        new_member = false;
                    }
                }
                if (new_member) {
                    Address peer_addr(to_addr(peer_mle.getid(), peer_mle.getport()));
                    log->logNodeAdd(&(memberNode->addr), &peer_addr);
                    memberNode->memberList.push_back(MemberListEntry(peer_mle.getid(), peer_mle.getport(), peer_mle.getheartbeat() ,par->getcurrtime()));
                }
            }
            break;
        }
        case JOINREP: {
            memberNode->inGroup = true;
            break;
        }
        default:
            log->LOG(&memberNode->addr, "Invalid message type, ignored...");
    }

    free(msg);

    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */
    memberNode->heartbeat ++;

    int n_mle = memberNode->memberList.size();
    size_t msgsize = sizeof(MessageHdr) + n_mle * sizeof(MemberListEntry);
    MessageHdr *msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    msg->msgType = HB;
    msg->n_msg = 0;
    for (auto it = memberNode->memberList.begin(); it != memberNode->memberList.end(); ) {
        MemberListEntry mle = *it;
        Address dst_addr(to_addr(mle.getid(), mle.getport()));
        if (dst_addr == memberNode->addr) {
            mle.setheartbeat(memberNode->heartbeat);
            mle.settimestamp(par->getcurrtime());
        }
        if (par->getcurrtime() - mle.gettimestamp() <= memberNode->pingCounter) {
            memcpy((MemberListEntry *)(msg + 1) + msg->n_msg, &mle, sizeof(MemberListEntry));
            msg->n_msg ++;
        }
        if (par->getcurrtime() - mle.gettimestamp() > memberNode->timeOutCounter) {
            log->logNodeRemove(&(memberNode->addr), &dst_addr);
            it = memberNode->memberList.erase(it);
        } else it++;
    }

    std::random_shuffle(memberNode->memberList.begin(), memberNode->memberList.end());
    int N = par->EN_GPSZ * 0.2;
    for (int i = 0; i < memberNode->memberList.size() && i < N; i++) {
        MemberListEntry mle = memberNode->memberList[i];
        Address dst_addr(to_addr(mle.getid(), mle.getport()));
        emulNet->ENsend(&memberNode->addr, &dst_addr, (char *)msg, sizeof(MessageHdr) + msg->n_msg * sizeof(MemberListEntry));
    }
  
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
