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
			electionTimeout = time.Duration(rand.Intn(20)+minElectionTimeout) * time.Millisecond
		} else {
			electionTimeout = time.Duration(rand.Intn(150)+minElectionTimeout) * time.Millisecond
		}
		select {
		case <-time.After(electionTimeout):
			log.Printf("Term(%d): peer(%d) election timeout", rf.currentTerm, rf.me)
			if time.Since(rf.lastHeartbeatTime) < electionTimeout {
				fmt.Printf("continue(%d)", rf.me)
				fmt.Println(time.Since(rf.lastHeartbeatTime))
				fmt.Println(electionTimeout)
				continue
			} else {
				fmt.Printf("timeout(%d)", rf.me)
				fmt.Println(time.Since(rf.lastHeartbeatTime))
				fmt.Println(electionTimeout)
				// log.Printf("Term(%d): peer(%d) timeout for heartbeat", rf.currentTerm, rf.me)
			}
			if _, isLeader := rf.GetState(); isLeader {
				// rf.lock.RLock()
				// log.Printf("Term(%d): peer(%d) starts a periodical heartbeat", rf.currentTerm, rf.me)
				// rf.lock.RUnlock()

				// go rf.sendHeartbeat()
				// go rf.replicate(100)
			} else {
				// rf.lock.RLock()

				// log.Printf("Term(%d): peer(%d) starts a new election", rf.currentTerm, rf.me)
				// rf.lock.RUnlock()

				go rf.elect()
				// break
			}
			// case <-rf.leaderChange:
			// 	if _, isLeader := rf.GetState(); isLeader {
			// 		rf.lock.RLock()
			// 		log.Printf("Term(%d): peer(%d) starts an instant heartbeat", rf.currentTerm, rf.me)
			// 		rf.lock.RUnlock()

			// 		go rf.replicate(0)
			// 	}
			// case <-rf.resetElectionTimer:
			// 	rf.lock.RLock()
			// 	log.Printf("Term(%d): peer(%d) resets election timer", rf.currentTerm, rf.me)
			// 	rf.lock.RUnlock()
			// case <-rf.done:
			// 	rf.lock.RLock()
			// 	log.Printf("Term(%d): peer(%d) quits", rf.currentTerm, rf.me)
			// 	rf.lock.RUnlock()
			// 	return
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
	rf.votedFor = -1
	rf.state = Candidate
	rf.currentTerm++
	rf.lock.Unlock()

	req := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
	}

	voted := int32(1)
	total := int32(len(rf.peers))
	half := total / 2

	done := make(chan struct{})
	finish := make(chan struct{})
	for i, _ := range rf.peers {
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

			if reply.VoteGranted {
				log.Printf("Term(%d): peer(%d) got a vote from peer(%d)", req.Term, rf.me, i)
				if atomic.AddInt32(&voted, 1) == half+1 {
					close(finish)
					log.Printf("Term(%d): peer(%d) becomes the leader", rf.currentTerm, rf.me)
					// rf.resetLeader(rf.me)

					// for i := 0; i < len(rf.matchIndex); i++ {
					// 	if i != rf.me {
					// 		rf.nextIndex[i] = len(rf.log)
					// 		rf.matchIndex[i] = -1
					// 	}
					// }
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

func (rf *Raft) sendHeartbeat(quit chan struct{}) {
	if rf.state != Leader {
		log.Panicln("Not the leader.")
		return
	}
	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	heartbeatTicker := time.NewTicker(minHeartbeatTimeout)

	for true {
		select {
		case <-heartbeatTicker.C:
			rf.lock.Lock()
			rf.lastHeartbeatTime = time.Now()
			rf.lock.Unlock()

			total := int32(len(rf.peers))
			successNum := int32(1)
			half := total / 2

			done := make(chan struct{})
			finish := make(chan struct{})

			for i, _ := range rf.peers {
				if i == rf.me {
					continue
				}

				go func(i int) {
					defer func() {
						if atomic.AddInt32(&total, -1) == 0 {
							if successNum < half+1 {
								log.Printf("Term(%d): Leader peer(%d) is NOT working fine on Leader",
									rf.currentTerm, rf.me)
								rf.lock.Lock()
								rf.state = Follower
								rf.lock.Unlock()
							}
							close(done)
						}

					}()

					reply := &AppendEntriesReply{}
					rf.sendHeartbeatRequset(i, args, reply)

					if reply.Success {
						if atomic.AddInt32(&successNum, 1) == half+1 {
							// log.Printf("Term(%d): Leader peer(%d) is working fine on Leader",
							// rf.currentTerm, rf.me)
							close(finish)
						}
					}

				}(i)
			}
			select {
			case <-done:
			case <-finish:
			case <-rf.done:
			}
		case <-quit:
			log.Printf("Term(%d): Leader peer(%d) quit", rf.currentTerm, rf.me)
			heartbeatTicker.Stop()
			return
		}
	}

	return
}

func (rf *Raft) sendHeartbeatRequset(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[i].Call("Raft.Heartbeat", args, reply)
	return ok
}
