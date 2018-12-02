package raft

import (
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
			electionTimeout = time.Duration(rand.Intn(25)+minHeartbeatTimeout) * time.Millisecond
		} else {
			electionTimeout = time.Duration(rand.Intn(200)+minElectionTimeout) * time.Millisecond
		}
		select {
		case <-time.After(electionTimeout):
			if _, isLeader := rf.GetState(); isLeader {
				rf.lock.RLock()
				log.Printf("Term(%d): peer(%d) starts a periodical heartbeat", rf.currentTerm, rf.me)
				rf.lock.RUnlock()

				go rf.replicate(100)
			} else {
				rf.lock.RLock()
				log.Printf("Term(%d): peer(%d) starts a new election", rf.currentTerm, rf.me)
				rf.lock.RUnlock()

				go rf.elect()
			}
		case <-rf.leaderChange:
			if _, isLeader := rf.GetState(); isLeader {
				rf.lock.RLock()
				log.Printf("Term(%d): peer(%d) starts an instant heartbeat", rf.currentTerm, rf.me)
				rf.lock.RUnlock()

				go rf.replicate(0)
			}
		case <-rf.resetElectionTimer:
			rf.lock.RLock()
			log.Printf("Term(%d): peer(%d) resets election timer", rf.currentTerm, rf.me)
			rf.lock.RUnlock()
		case <-rf.done:
			rf.lock.RLock()
			log.Printf("Term(%d): peer(%d) quits", rf.currentTerm, rf.me)
			rf.lock.RUnlock()
			return
		}
	}
}
