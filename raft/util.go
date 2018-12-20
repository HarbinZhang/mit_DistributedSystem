package raft

import (
	"fmt"
	"log"
	"math/rand"
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
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		reply := &RequestVoteReply{}
		go rf.sendRequestVote(i, req, reply)
	}

	halfPeers := len(rf.peers) / 2
	voted := 1
loop:
	for true {
		select {
		case reply := <-rf.voteReplyChan:
			fmt.Printf("reply:%v", reply)
			if reply.Term > rf.currentTerm {

				rf.lock.Lock()
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.lock.Unlock()

				break loop
			} else if reply.Term < rf.currentTerm {
				continue
			} else if reply.VoteGranted {
				voted++
				if voted > halfPeers {

					log.Printf("it's ok, voted(%d), quarum(%d), to be a leader(%d)", voted, halfPeers, rf.me)

					rf.lock.Lock()
					rf.leaderID = rf.me
					rf.state = Leader
					go rf.sendHeartbeat(rf.done)
					rf.lock.Unlock()

					return
				}
			} else {
				// log.
				break loop
			}
		}
	}

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.voteReplyChan <- reply
	log.Printf("Term(%d): peer(%d) got RequestVoteReply from peer(%d) with term(%d)",
		rf.currentTerm, rf.me, server, reply.Term)
	return ok
}

func (rf *Raft) sendHeartbeat(quit chan struct{}) bool {
	if rf.state != Leader {
		log.Panicln("Not the leader.")
		return false
	}
	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	heartbeatTicker := time.NewTicker(minHeartbeatTimeout)

	for true {
		select {
		case <-heartbeatTicker.C:
			rf.lastHeartbeatTime = time.Now()
			for i, _ := range rf.peers {
				if i == rf.me {
					continue
				}
				reply := &AppendEntriesReply{}
				go rf.sendHeartbeatRequset(i, args, reply)
			}

			replyNum := 1
			successNum := 1
			for reply := range rf.heartbeatReplyChan {
				replyNum++
				if reply.Success {
					successNum++
				}
				if replyNum == len(rf.peers) {
					break
				}
			}

			if successNum > len(rf.peers)/2 {
				log.Printf("Term(%d): Leader peer(%d) is working fine on Leader",
					rf.currentTerm, rf.me)
			} else {
				rf.lock.Lock()
				defer rf.lock.Unlock()
				log.Printf("Term(%d): Leader peer(%d) is NOT working fine on Leader",
					rf.currentTerm, rf.me)
				rf.state = Follower
			}
		case <-quit:
			log.Printf("Term(%d): Leader peer(%d) quit", rf.currentTerm, rf.me)
			heartbeatTicker.Stop()
			return true
		}
	}

	return true
}

func (rf *Raft) sendHeartbeatRequset(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	log.Printf("Term(%d): Leader send Heartbeat to peer(%d) with term(%d)",
		rf.currentTerm, i, reply.Term)
	ok := rf.peers[i].Call("Raft.Heartbeat", args, reply)
	if !ok {
		return false
	}
	rf.heartbeatReplyChan <- reply
	log.Printf("Term(%d): peer(%d) got Heartbeat Reply from peer(%d) with term(%d)",
		rf.currentTerm, rf.me, i, reply.Term)
	return true
}
