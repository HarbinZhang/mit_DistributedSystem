package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) election() {
	var electionTimeout time.Duration

	for {
		if _, isLeader := rf.GetState(); isLeader {
			// electionTimeout = time.Duration(rand.Intn(25)+minHeartbeatTimeout) * time.Millisecond
			electionTimeout = time.Duration(rand.Intn(25)+minHeartbeatTimeout) * time.Millisecond
		} else {
			electionTimeout = time.Duration(rand.Intn(200)+minElectionTimeout) * time.Millisecond
		}
		select {
		case <-time.After(electionTimeout):
			log.Printf("Term(%d): peer(%d) election timeout %v", rf.currentTerm, rf.me, electionTimeout)
			if _, isLeader := rf.GetState(); isLeader {
				rf.lock.RLock()
				log.Printf("Term(%d): peer(%d) starts a periodical heartbeat", rf.currentTerm, rf.me)
				rf.lock.RUnlock()

				// go rf.sendHeartbeat()
				go rf.replicate(100)
			} else {
				rf.lock.RLock()
				log.Printf("Term(%d): peer(%d) starts a new election", rf.currentTerm, rf.me)
				rf.lock.RUnlock()

				go rf.elect()
			}
		case <-rf.leaderChange:
			fmt.Println("wo")
			if _, isLeader := rf.GetState(); isLeader {
				rf.lock.RLock()
				log.Printf("Term(%d): peer(%d) starts an instant heartbeat", rf.currentTerm, rf.me)
				rf.lock.RUnlock()
				go rf.replicate(0)
				// go rf.sendHeartbeat()
			} else {
				fmt.Println("not the leader", rf.leaderID)
			}
		case <-rf.resetElectionTimer:
			rf.lock.RLock()
			// log.Printf("Term(%d): peer(%d) resets election timer", rf.currentTerm, rf.me)
			rf.lock.RUnlock()
		case <-rf.done:
			rf.lock.RLock()
			log.Printf("Term(%d): peer(%d) quits", rf.currentTerm, rf.me)
			rf.lock.RUnlock()
			return
		}
	}
}

func (rf *Raft) elect() {
	// be candidate
	// Vote req
	// waiting for response
	// jg.

	fmt.Println("Into elect")

	rf.lock.Lock()

	rf.persist()
	rf.votedFor = rf.me
	rf.resetLeader(-1)
	// rf.leaderID = rf.me
	rf.state = Candidate
	rf.currentTerm++

	req := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
	}
	// ???: because of index
	req.LastLogIndex = len(rf.log) - 1
	if req.LastLogIndex >= 0 {
		req.LastLogTerm = rf.log[req.LastLogIndex].Term
		fmt.Println(rf.log)
	}

	rf.lock.Unlock()

	voted := int32(1)
	total := int32(len(rf.peers))
	half := total / 2

	done := make(chan struct{})
	finish := make(chan struct{})
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(i int) {
			defer func() {
				if atomic.AddInt32(&total, -1) == 0 {
					close(done)
				}
			}()

			reply := &RequestVoteReply{}

			log.Printf("Term(%d): sending Raft.RequestVote RPC from peer(%d) to peer(%d)", req.Term, rf.me, i)
			if !rf.sendRequestVote(i, req, reply) {
				log.Printf("Term(%d): sent Raft.RequestVote RPC from peer(%d) to peer(%d) failed", req.Term, rf.me, i)
				return
			}

			rf.lock.Lock()
			defer rf.lock.Unlock()

			if rf.currentTerm != req.Term {
				return
			}

			if rf.resetState(reply.Term) {
				rf.persist()
				log.Printf("Term(%d): peer(%d) becomes %v", rf.currentTerm, rf.me, Follower)
			}

			if reply.VoteGranted {
				log.Printf("Term(%d): peer(%d) got a vote from peer(%d)", req.Term, rf.me, i)
				if atomic.AddInt32(&voted, 1) == half+1 {

					log.Printf("Term(%d): peer(%d) becomes the leader", rf.currentTerm, rf.me)
					// rf.resetLeader(rf.me)
					close(finish)
					fmt.Println(rf.leaderID)
					rf.resetLeader(rf.me)
					rf.state = Leader

					for i := 0; i < len(rf.matchIndex); i++ {
						if i != rf.me {
							rf.nextIndex[i] = len(rf.log)
							rf.matchIndex[i] = -1
						}
					}

				}
			}

		}(i)
	}

	select {
	case <-done:
	case <-finish:
	case <-rf.done:
	}

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) commit(index int) {
	log.Printf("Term(%d): peer(%d) commits index(%d) log(%v)", rf.currentTerm, rf.me, index, rf.log[index])
	rf.commitIndex = index

	select {
	case rf.applySignal <- struct{}{}:
	default:
	}
}

func (rf *Raft) apply(ch chan<- ApplyMsg) {
	rf.lock.RLock()
	commitIndex := rf.commitIndex
	rf.lock.RUnlock()

	for i := rf.lastApplied + 1; i <= commitIndex; i++ {
		rf.lock.RLock()
		cmd := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
		rf.lock.RUnlock()

		select {
		case ch <- cmd:
			rf.lastApplied = i

			rf.lock.RLock()
			log.Printf("Term(%d): peer(%d) applied index(%d) cmd(%v)",
				rf.currentTerm, rf.me, rf.lastApplied, cmd.Command)
			rf.lock.RUnlock()
		case <-rf.done:
			return
		}
	}
}

func (rf *Raft) replicate(maxSize int) {
	var (
		replicated = int32(1)
		total      = int32(len(rf.peers))
		half       = total / 2

		done   = make(chan struct{})
		finish = make(chan struct{})
	)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(i int) {
			defer func() {
				if atomic.AddInt32(&total, -1) == 0 {
					close(done)
				}
			}()

			for {
				args := &AppendEntriesArgs{}
				reply := &AppendEntriesReply{}

				rf.lock.RLock()
				if rf.leaderID != rf.me {
					rf.lock.RUnlock()
					return
				}

				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[i] - 1
				if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.log) {
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				}

				if rf.nextIndex[i] >= 0 && rf.nextIndex[i] < len(rf.log) {
					entries := rf.log[rf.nextIndex[i]:min(len(rf.log), rf.nextIndex[i]+maxSize)]
					args.Entries = make([]LogEntry, len(entries))
					copy(args.Entries, entries)
				}
				args.LeaderCommit = rf.commitIndex

				rf.lock.RUnlock()

				if !rf.sendAppendEntries(i, args, reply) {
					log.Printf("Term(%d): sent Raft.AppendEntries(%d) RPC from peer(%d) to peer(%d) failed",
						args.Term, len(args.Entries), rf.me, i)
					return
				}

				rf.lock.Lock()

				if rf.currentTerm != args.Term {
					rf.lock.Unlock()
					return
				}

				if rf.resetState(reply.Term) {
					rf.persist()
					log.Printf("Term(%d): peer(%d) becomes %v", rf.currentTerm, rf.me, Follower)
				}

				switch reply.Status {
				case 0: // success
					if len(args.Entries) > 0 {
						rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[i] = rf.matchIndex[i] + 1

						log.Printf("Term(%d): peer(%d) matchIndex[%d] is %d, %v",
							rf.currentTerm, rf.me, i, rf.matchIndex[i], rf.log[rf.matchIndex[i]])
					}

					if atomic.AddInt32(&replicated, 1) == half+1 {
						// majority
						//
						close(finish)

						commitIndex := rf.commitIndex

						log.Printf("Term(%d): peer(%d) entries: %v", rf.currentTerm, rf.me, rf.log)

						// get latest index in majority
						// can be improved
						for i := commitIndex + 1; i < len(rf.log); i++ {
							count := int32(1)
							for j := 0; j < len(rf.matchIndex) && count <= half; j++ {
								if j != rf.me && rf.matchIndex[j] >= i {
									count++
								}
							}

							if count > half && rf.log[i].Term == rf.currentTerm {
								commitIndex = i
							}
						}

						if commitIndex > rf.commitIndex {
							rf.commit(commitIndex)
						}
					}

					rf.lock.Unlock()
					return
				case 1: // log inconsistency
					if args.PrevLogIndex == rf.nextIndex[i]-1 {
						if reply.ConflictTerm == -1 {
							rf.nextIndex[i] = reply.ConflictIndex
						} else {
							j := args.PrevLogIndex
							for j > 0 && rf.log[j-1].Term != reply.ConflictTerm {
								j--
							}
							if j > 0 {
								rf.nextIndex[i] = j
							} else {
								rf.nextIndex[i] = reply.ConflictIndex
							}
						}
						rf.lock.Unlock()
					} else {
						rf.lock.Unlock()
						return
					}
				default: // old term
					rf.lock.Unlock()
					return
				}

			}
		}(i)
	}

	select {
	case <-rf.done:
	case <-done:
	case <-finish:
	}
}

func (rf *Raft) resetLeader(i int) {
	if rf.leaderID != i {
		rf.leaderID = i
		select {
		case rf.leaderChange <- struct{}{}:
		default:
		}
	}
}

func (rf *Raft) resetState(term int) bool {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.resetLeader(-1)
		rf.state = Follower
		return true
	}

	return false
}

func (rf *Raft) resetElection() {
	select {
	case rf.resetElectionTimer <- struct{}{}:
	default:
	}
}
