package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 3A: leader election
	applyCh       chan raftapi.ApplyMsg
	state         ServerState
	lastHeartbeat time.Time

	currentTerm int
	votedFor    int

	// 3B: log
	log         []LogEntry
	commitIndex int
	lastApplied int

	// leader only states
	nextIndex  []int
	matchIndex []int

	// for 3D
	snapshotIndex int
	snapshotTerm  int
}

func (rf *Raft) logAt(i int) LogEntry {
	return rf.log[i-rf.snapshotIndex]
}

func (rf *Raft) lastLogIndex() int {
	return rf.snapshotIndex + len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var snapshotIndex int
	var snapshotTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&snapshotIndex) != nil ||
		d.Decode(&snapshotTerm) != nil {
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.snapshotIndex = snapshotIndex
		rf.snapshotTerm = snapshotTerm
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.snapshotIndex {
		return
	}

	rf.log = rf.log[index-rf.snapshotIndex:]
	rf.snapshotIndex = index
	rf.snapshotTerm = rf.log[0].Term

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// RequestVote RPC Rule 1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// Rules for Servers, All Servers, Rule 2
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1

		rf.persist()
	}

	// RequestVote RPC Rule 2
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.lastLogTerm() ||
			args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex()) {
		rf.votedFor = args.CandidateId
		rf.lastHeartbeat = time.Now()
		reply.VoteGranted = true

		rf.persist()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	term := rf.currentTerm

	// Your code here (3B).
	index := rf.lastLogIndex() + 1
	rf.log = append(rf.log, LogEntry{Term: term, Command: command})
	rf.persist()
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	return index, term, true
}

const electionTimeout = 400 * time.Millisecond

func (rf *Raft) ticker() {
	for true {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state != Leader && time.Since(rf.lastHeartbeat) > electionTimeout {
			// start election in a new goroutine
			// so that ticker can continue to run
			go rf.startElection()
			rf.lastHeartbeat = time.Now()
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 100
		// milliseconds.
		ms := 50 + (rand.Int63() % 50)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = Candidate
	term := rf.currentTerm
	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.lastLogTerm()
	rf.persist()
	rf.mu.Unlock()

	votes := 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		// use go routine to send request to each peer
		// for loop is bad because it will block the election
		go func(peer int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			if rf.sendRequestVote(peer, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// If higher term is received, I'm Follower now
				// If election is over, I'm Leader now
				// TODO: is rf.currentTerm != term necessary?
				// ANSWER: imagine you sent VoteRequest in term N
				//         but network is slow, so you timeout, start a new election in term N+1
				//         then you receive the reply for term N
				if rf.state != Candidate || rf.currentTerm != term {
					return
				}

				// Rules for Servers, All Servers, Rule 2
				if reply.Term > term {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()
					return
				}

				// Check if I got majority of votes
				if reply.VoteGranted {
					votes++

					if votes > len(rf.peers)/2 {
						rf.state = Leader
						rf.becomeLeader()
						go rf.sendHeartbeats()
					}
				}
			}
		}(i)
	}
}

// Rules for Servers, All Servers, Rule 1
func (rf *Raft) applyCommittedLogsLoop() {
	for {
		rf.mu.Lock()

		if rf.commitIndex <= rf.lastApplied {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.log[lastApplied+1-rf.snapshotIndex:commitIndex+1-rf.snapshotIndex])
		rf.mu.Unlock()

		for i, entry := range entries {
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: lastApplied + i + 1,
			}
		}

		rf.mu.Lock()
		if commitIndex > rf.lastApplied {
			rf.lastApplied = commitIndex
		}
		rf.mu.Unlock()
	}
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(peer int, term int, me int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:             term,
		LeaderId:         me,
		LastIncludeIndex: rf.snapshotIndex,
		LastIncludeTerm:  rf.snapshotTerm,
		Data:             rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	reply := InstallSnapshotReply{}
	if rf.peers[peer].Call("Raft.InstallSnapshot", &args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.state != Leader || rf.currentTerm != term {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
			return
		}

		rf.nextIndex[peer] = args.LastIncludeIndex + 1
		rf.matchIndex[peer] = args.LastIncludeIndex
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	rf.lastHeartbeat = time.Now()
	rf.state = Follower

	if args.LastIncludeIndex <= rf.snapshotIndex {
		rf.mu.Unlock()
		return
	}

	// trim log
	if args.LastIncludeIndex < rf.lastLogIndex() {
		rf.log = rf.log[args.LastIncludeIndex-rf.snapshotIndex:]
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludeTerm}}
	}

	rf.snapshotIndex = args.LastIncludeIndex
	rf.snapshotTerm = args.LastIncludeTerm
	rf.commitIndex = max(rf.commitIndex, args.LastIncludeIndex)
	rf.lastApplied = max(rf.lastApplied, args.LastIncludeIndex)

	// persist
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	rf.persister.Save(w.Bytes(), args.Data)

	msg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludeTerm,
		SnapshotIndex: args.LastIncludeIndex,
	}
	rf.mu.Unlock()
	rf.applyCh <- msg
}

func (rf *Raft) sendHeartbeats() {
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		term := rf.currentTerm
		me := rf.me
		leaderCommit := rf.commitIndex
		rf.mu.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			if i == me {
				continue
			}

			// send heartbeat to each peer in a goroutine
			// for loop is absolutely inefficient here
			// TODO: send immediately when new log is added, use condvar + sleep / channel + timer
			go func(peer int) {
				rf.mu.Lock()

				if rf.nextIndex[peer] <= rf.snapshotIndex {
					rf.mu.Unlock()
					go rf.sendInstallSnapshot(peer, term, me)
					return
				}

				prevLogIndex := rf.nextIndex[peer] - 1
				prevLogTerm := rf.logAt(prevLogIndex).Term
				startIdx := rf.nextIndex[peer] - rf.snapshotIndex
				entries := make([]LogEntry, len(rf.log)-startIdx)
				copy(entries, rf.log[startIdx:])

				rf.mu.Unlock()

				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: leaderCommit,
				}
				reply := AppendEntriesReply{}
				if rf.sendAppendEntries(peer, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					// If higher term is received, I'm Follower now
					// TODO: is rf.currentTerm != term necessary?
					if rf.state != Leader || rf.currentTerm != term {
						return
					}

					// Rules for Servers, All Servers, Rule 2
					if reply.Term > term {
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.votedFor = -1
						rf.persist()
						return
					}

					// If AppendEntries RPC succeeds
					if reply.Success {
						rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
						rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)

						// update commitIndex after matchIndex is updated
						rf.updateCommitIndex()
					}

					// If AppendEntries RPC fails
					if !reply.Success {
						// Before 3C:
						// rf.nextIndex[peer]--
						// if rf.nextIndex[peer] < 1 {
						// rf.nextIndex[peer] = 1
						// }

						// After 3C:
						//   Case 1: leader doesn't have XTerm:
						//     nextIndex = XIndex
						//   Case 2: leader has XTerm:
						//     nextIndex = (index of leader's last entry for XTerm) + 1
						//   Case 3: follower's log is too short:
						//     nextIndex = XLen

						hasXTerm := false
						for j := len(rf.log) - 1; j >= 0; j-- {
							if rf.log[j].Term == reply.XTerm {
								hasXTerm = true
								break
							}
						}

						if reply.XTerm == -1 {
							// Case 3: follower's log is too short
							rf.nextIndex[peer] = reply.XLen
						} else if !hasXTerm {
							// Case 1: leader doesn't have XTerm
							rf.nextIndex[peer] = reply.XIndex
						} else {
							// Case 2: leader has XTerm
							// find the last index in leader's log with XTerm
							lastXTermIndex := 0
							for j := len(rf.log) - 1; j >= 0; j-- {
								if rf.log[j].Term == reply.XTerm {
									lastXTermIndex = j
									break
								}
							}
							rf.nextIndex[peer] = lastXTermIndex + 1
						}
					}
				}
			}(i)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) updateCommitIndex() {
	// use sort to find the median of matchIndex
	sortedMatchIndex := make([]int, len(rf.peers))
	copy(sortedMatchIndex, rf.matchIndex)
	sortedMatchIndex[rf.me] = rf.lastLogIndex()

	sort.Ints(sortedMatchIndex)

	majorityIndex := len(rf.peers) / 2
	N := sortedMatchIndex[majorityIndex]

	if N > rf.commitIndex && rf.logAt(N).Term == rf.currentTerm {
		rf.commitIndex = N
		// TODO: consider using condvar to wake up apply loop
	}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// You will probably need the optimization that backs up nextIndex by more than one entry at a time.
	XTerm  int
	XIndex int
	XLen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Rules For Servers, All Servers, Rule 2
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1

		rf.persist()
	}

	reply.Term = rf.currentTerm
	reply.Success = false

	// AppendEntries RPC, Rule 1
	if args.Term < rf.currentTerm {
		return
	}

	// reset timer for valid leader
	rf.lastHeartbeat = time.Now()
	rf.state = Follower

	if args.PrevLogIndex < rf.snapshotIndex {
		reply.Success = true
		return
	}

	// AppendEntries RPC, Rule 2
	if args.PrevLogIndex > rf.lastLogIndex() || rf.logAt(args.PrevLogIndex).Term != args.PrevLogTerm {
		if args.PrevLogIndex > rf.lastLogIndex() {
			reply.XLen = rf.lastLogIndex() + 1
			reply.XTerm = -1
			reply.XIndex = rf.lastLogIndex() + 1
		} else {
			reply.XTerm = rf.logAt(args.PrevLogIndex).Term
			xIndex := args.PrevLogIndex
			for xIndex > rf.snapshotIndex && rf.logAt(xIndex-1).Term == reply.XTerm {
				xIndex--
			}
			reply.XIndex = xIndex
			reply.XLen = rf.lastLogIndex() + 1
		}
		return
	}

	// AppendEntries RPC, Rule 3 & 4
	insertIndex := args.PrevLogIndex + 1
	argsIndex := 0

	// find the first index where the log entries differ, delete entries after that
	for insertIndex <= rf.lastLogIndex() && argsIndex < len(args.Entries) {
		if rf.logAt(insertIndex).Term != args.Entries[argsIndex].Term {
			rf.log = rf.log[:insertIndex-rf.snapshotIndex]
			rf.persist()
			break
		}
		insertIndex++
		argsIndex++
	}

	// append entries after the first differing index
	if argsIndex < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[argsIndex:]...)
		rf.persist()
	}

	// AppendEntries RPC, Rule 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) becomeLeader() {
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastHeartbeat = time.Now()

	// initialization code for 3B
	// log[0] is a dummy entry
	rf.log = []LogEntry{{Term: 0, Command: nil}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	// initialize nextIndex and matchIndex in becomeLeader
	// when Candidate -> Leader

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.lastApplied = rf.snapshotIndex
	rf.commitIndex = rf.snapshotIndex

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyCommittedLogsLoop()

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		go func() {
			rf.applyCh <- raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snapshot,
				SnapshotTerm:  rf.snapshotTerm,
				SnapshotIndex: rf.snapshotIndex,
			}
		}()
	}

	return rf
}
