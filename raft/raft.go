package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"log"
	"math/rand"
	"mit/src/labrpc"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
// type ApplyMsg struct {
// 	CommandValid bool
// 	Command      interface{}
// 	CommandIndex int
// }

type ServerState string

const (
	Follower  ServerState = "Follower"
	Candidate             = "Candidate"
	Leader                = "Leader"
)
const HeartBeatInterval = 100 * time.Millisecond
const CommitApplyIdleCheckInterval = 25 * time.Millisecond
const LeaderPeerTickInterval = 10 * time.Millisecond

// const Debug = 1

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// General
	id               string
	state            ServerState
	isDecommissioned bool

	// Election state
	currentTerm int
	votedFor    string
	leaderID    string

	// Log state
	log         []LogEntry
	commitIndex int
	lastApplied int

	// Log compaction state if snapshots are enabled
	lastSnapshotIndex int
	lastSanpshotTerm  int

	// Leader state
	nextIndex      []int
	matchIndex     []int
	sendAppendChan []chan struct{}

	//Liveness state
	lastHeartBeat time.Time
}

func (rf *Raft) Lock() {
	rf.mu.Lock()
}

func (rf *Raft) Unlock() {
	rf.mu.Unlock()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	fmt.Println("getState")
	var isleader bool
	// Your code here (2A).
	// isleader = (rf.leaderID == rf.id)
	isleader = rf.state == Leader
	return rf.currentTerm, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
// type RequestVoteArgs struct {
// 	// Your data here (2A, 2B).
// 	Term         int
// 	CandidateID  string
// 	LastLogTerm  int
// 	LastLogIndex int
// }

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
// type RequestVoteReply struct {
// 	// Your data here (2A).
// 	Term        int
// 	VoteGranted bool
// 	ID          string
// }

func (rf *Raft) getLastEntryInfo() (int, int) {
	fmt.Println("getLastEntryInfo")
	if len(rf.log) > 0 {
		entry := rf.log[len(rf.log)-1]
		return entry.Index, entry.Term
	}
	return rf.lastSnapshotIndex, rf.lastSanpshotTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	fmt.Println("RequestVote")
	// Your code here (2A, 2B).
	rf.Lock()
	defer rf.Unlock()

	lastIndex, lastTerm := rf.getLastEntryInfo()
	logUpToDate := func() bool {
		if lastTerm == args.LastLogTerm {
			return lastIndex <= args.LastLogIndex
		}
		return lastIndex < args.LastLogTerm
	}()

	reply.Term = rf.currentTerm
	reply.Id = rf.id

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if args.Term >= rf.currentTerm && logUpToDate {
		rf.transitionToFollower(args.Term)
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
	} else if (rf.votedFor == "" || args.CandidateID == rf.votedFor) && logUpToDate {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
	}
}

func (rf *Raft) transitionToFollower(newTerm int) {
	fmt.Println("transitionTo Follower")
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = ""
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	fmt.Println("sendRequest")
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	fmt.Println("Raft Start")
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	fmt.Println("Make")

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startElectionProcess()
	// go rf.startLocalApplyProcess()

	return rf
}

func (rf *Raft) startElectionProcess() {
	electionTimeout := func() time.Duration {
		return (200 + time.Duration(rand.Intn(300))) * time.Millisecond
	}

	currentTimeout := electionTimeout()
	currentTime := <-time.After(currentTimeout)

	rf.Lock()
	defer rf.Unlock()
	if !rf.isDecommissioned {
		if rf.state != Leader && currentTime.Sub(rf.lastHeartBeat) >= currentTimeout {
			RaftInfo("Election timer timed out. Timeout: %fs", rf, currentTimeout.Seconds())
			go rf.beginElection()
		}
		go rf.startElectionProcess()
	}
}

func (rf *Raft) beginElection() {
	rf.Lock()
	rf.transitionToCandidate()
	RaftInfo("Election started", rf)

	lastIndex, lastTerm := rf.getLastEntryInfo()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.id,
		LastLogTerm:  lastTerm,
		LastLogIndex: lastIndex,
	}

	replies := make([]RequestVoteReply, len(rf.peers))
	// voteChan := make(chan int, len(rf.peers))
	rf.Unlock()
	votes := 1
	for i := range rf.peers {
		if i != rf.me {
			go func() {
				var reply RequestVoteReply
				res := rf.sendRequestVote(i, &args, &reply)
				if res {
					rf.Lock()
					if reply.Term > rf.currentTerm {
						RaftInfo("Switching to follower as %s's term is %d", rf, reply.Id, reply.Term)
						rf.transitionToFollower(reply.Term)
						// break
					} else if votes += reply.VoteCount(); votes > len(replies)/2 { // Has majority vote
						// Ensure that we're still a candidate and that another election did not interrupt
						if rf.state == Candidate && args.Term == rf.currentTerm {
							RaftInfo("Election won. Vote: %d/%d", rf, votes, len(rf.peers))
							go rf.promoteToLeader()
							// break
						} else {
							RaftInfo("Election for term %d was interrupted", rf, args.Term)
							// break
						}
					}
					rf.Unlock()
				}
			}()
		}
	}

	// rf.persist()
	// rf.Unlock()

	// votes := 1
	// for i := 0; i < len(replies); i++ {
	// 	// reply := replies[<]
	// }

}

func (rf *Raft) promoteToLeader() {
	rf.Lock()
	defer rf.Unlock()

	rf.state = Leader
	rf.leaderID = rf.id

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.sendAppendChan = make([]chan struct{}, len(rf.peers))

	for i := range rf.peers {
		if i != rf.me {
			rf.nextIndex[i] = len(rf.log) + 1 // ?
			rf.matchIndex[i] = 0              // index of highest log entry known to be replicated on server
			rf.sendAppendChan[i] = make(chan struct{}, 1)

			go rf.startLeaderPeerProcess(i, rf.sendAppendChan[i])
		}
	}
}

func (rf *Raft) sendAppendEntries(peerIndex int, sendAppendChan chan struct{}) {
	rf.Lock()

	if rf.state != Leader || rf.isDecommissioned {
		rf.Unlock()
		return
	}

	var entries []LogEntry = []LogEntry{}
	var prevLogIndex, prevLogTerm int = 0, 0

	peerId := string(rune(peerIndex + 'A'))
	lastLogIndex, _ := rf.getLastEntryInfo()

	if lastLogIndex > 0 && lastLogIndex >= rf.nextIndex[peerIndex] {
		if rf.nextIndex[peerIndex] <= rf.lastSnapshotIndex { // We don't have the required entry in our log; sending snapshot.
			rf.Unlock()
			rf.sendSnapshot(peerIndex, sendAppendChan)
			return
		} else {
			for i, v := range rf.log { // Need to send logs beginning from index `rf.nextIndex[peerIndex]`
				if v.Index == rf.nextIndex[peerIndex] {
					if i > 0 {
						lastEntry := rf.log[i-1]
						prevLogIndex, prevLogTerm = lastEntry.Index, lastEntry.Term
					} else {
						prevLogIndex, prevLogTerm = rf.lastSnapshotIndex, rf.lastSnapshotTerm
					}
					entries = make([]LogEntry, len(rf.log)-i)
					copy(entries, rf.log[i:])
					break
				}
			}
			RaftInfo("Sending log %d entries to %s", rf, len(entries), peerId)
		}
	} else { // We're just going to send a heartbeat
		if len(rf.log) > 0 {
			lastEntry := rf.log[len(rf.log)-1]
			prevLogIndex, prevLogTerm = lastEntry.Index, lastEntry.Term
		} else {
			prevLogIndex, prevLogTerm = rf.lastSnapshotIndex, rf.lastSnapshotTerm
		}
	}

	reply := AppendEntriesReply{}
	args := AppendEntriesArgs{
		Term:             rf.currentTerm,
		LeaderID:         rf.id,
		PreviousLogIndex: prevLogIndex,
		PreviousLogTerm:  prevLogTerm,
		LogEntries:       entries,
		LeaderCommit:     rf.commitIndex,
	}
	rf.Unlock()

	ok := rf.sendAppendEntryRequest(peerIndex, &args, &reply)

	rf.Lock()
	defer rf.Unlock()

	if !ok {
		RaftDebug("Communication error: AppendEntries() RPC failed", rf)
	} else if rf.state != Leader || rf.isDecommissioned || args.Term != rf.currentTerm {
		RaftInfo("Node state has changed since request was sent. Discarding response", rf)
	} else if reply.Success {
		if len(entries) > 0 {
			RaftInfo("Appended %d entries to %s's log", rf, len(entries), peerId)
			lastReplicated := entries[len(entries)-1]
			rf.matchIndex[peerIndex] = lastReplicated.Index
			rf.nextIndex[peerIndex] = lastReplicated.Index + 1
			rf.updateCommitIndex()
		} else {
			RaftDebug("Successful heartbeat from %s", rf, peerId)
		}
	} else {
		if reply.Term > rf.currentTerm {
			RaftInfo("Switching to follower as %s's term is %d", rf, peerId, reply.Term)
			rf.transitionToFollower(reply.Term)
		} else {
			RaftInfo("Log deviation on %s. T: %d, nextIndex: %d, args.Prev[I: %d, T: %d], FirstConflictEntry[I: %d, T: %d]", rf, peerId, reply.Term, rf.nextIndex[peerIndex], args.PreviousLogIndex, args.PreviousLogTerm, reply.ConflictingLogIndex, reply.ConflictingLogTerm)
			// Log deviation, we should go back to `ConflictingLogIndex - 1`, lowest value for nextIndex[peerIndex] is 1.
			rf.nextIndex[peerIndex] = Max(reply.ConflictingLogIndex-1, 1)
			sendAppendChan <- struct{}{} // Signals to leader-peer process that appends need to occur
		}
	}
	rf.persist()
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func (rf *Raft) sendAppendEntryRequest(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	requestName := "Raft.AppendEntries"
	request := func() bool {
		return rf.peers[server].Call(requestName, args, reply)
	}
	return SendRPCRequest(requestName, request)
}

func (rf *Raft) startLeaderPeerProcess(peerIndex int, sendAppendChan chan struct{}) {
	ticker := time.NewTicker(LeaderPeerTickInterval)

	rf.sendAppendEntries(peerIndex, sendAppendChan)

	lastEntrySent := time.Now()

	for {
		rf.Lock()
		if rf.state != Leader || rf.isDecommissioned {
			ticker.Stop()
			rf.Unlock()
			break
		}
		rf.Unlock()

		select {
		case <-sendAppendChan:

		}
	}
}

func (rf *Raft) transitionToCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.id
}

func RaftInfo(format string, rf *Raft, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		args := append([]interface{}{rf.id, rf.currentTerm, rf.state}, a...)
		log.Printf("[INFO] Raft: [Id: %s | Term: %d | %v] "+format, args...)
	}
	return
}
