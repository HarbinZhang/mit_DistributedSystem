# For Raft


### lab 2.1
1. No Warning for "the channel is not init"
2. heartbeat() for each leader would cost a lot cpu usage, which uses too many tickers, routines?
3. select is like a wait, also try
```
	select {
	case rf.resetElectionTimer <- struct{}{}:
	default:
	}
```
So that there is no need to block.
4. go func(){}(i) to use lower level variables, remember atomic.AddInt32(&name, 1)