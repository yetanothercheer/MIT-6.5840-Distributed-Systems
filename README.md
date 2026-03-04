6.5840

```
Notes: 

Lab3 Raft
https://pdos.csail.mit.edu/6.824/labs/lab-raft1.html
https://raft.github.io/raft.pdf

Part 3A: leader election

Step 1: Initialize
state = Follower
currentTerm = 0
votedFor = -1

Step 2: One server times out
On timeout, start a goroutine:
state = Candidate
currentTerm++
votedFor = me
for all peers: go sendRequestVote(peer, args, reply) { send vote to main loop }
Main loop reads channel: if vote > n/2 { state = Leader; go send heartbeat loop }

General Requirements:
If an RPC req/res contains a term > currentTerm, set currentTerm = term and state = Follower.

Part 3B: log
more states:
log = []
commitIndex = 0
lastApplied = 0

leader only states (initialize at Candicate -> Leader):
nextIndex = [len(log) +1] * len(peers)
matchIndex = [0] * len(peers)

Step1. leader receives Start(command)
append command to log
set matchIndex[me] = len(log)
set nextIndex[me] = len(log) + 1

Step2. extended sendHeartbeats()
send entries from nextIndex[peer] to len(log) - 1

Step3. follower and leader apply logs in a loop

Part 3C, 3D: Vibe Coding
```
