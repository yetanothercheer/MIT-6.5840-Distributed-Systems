6.5840

```
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

TODO: Part 3B, Part 3C, Part 3D
```
