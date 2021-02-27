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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

// import "bytes"
// import "6.824/labgob"

// ApplyMsg ...
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type serverID = int
type term = int

// LogEntry ...
type LogEntry struct {
	TermCreated term
	Command     interface{}
}

type status = uint8

const (
	follower status = iota
	candidate
	leader
)

// FIXME not clear what the units are
const minElectionTimeoutMS = 500
const maxElectionTimeoutMS = 1000

// used for appendEntries: how long to wait between sending heartbeats if you're the leader
const timeBetweenHeartBeatsMS = 150

// Raft ...
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm term
	votedFor    serverID
	log         []LogEntry

	// Volatile state on all servers
	currentStatus status // am I a leader, follower or candidate?
	// initialised at 0. When it hits 0, all messages have timed out, start election
	lastHeardFrom     time.Time     // used for election timeout
	electionTimeout   time.Duration // used for election timeout
	lastSentHeartbeat time.Time     // used for appendEntries
	commitIndex       uint          // index of highest log entry known to be committed
	lastApplied       uint          // index of highest log entry applied to state machine
	votesReceived     []serverID

	// Volatile state on leaders
	nextIndex  []uint // for each server, index of next log entry to send to that server
	matchIndex []uint // for each server, index of highest log entry known to be replicated on that server
}

// GetState ...
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term term
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = status(rf.currentStatus) == leader
	return term, isleader
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

// CondInstallSnapshot ...
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2C).

	return true
}

// Snapshot ...
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2C).

}

// AppendEntriesArgs ...
type AppendEntriesArgs struct {
	// 2A
	//
	Term         term
	LeaderID     int
	PrevLogIndex uint
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit uint
}

// AppendEntriesReply ...
type AppendEntriesReply struct {
	// 2A
	Term    term
	Success bool
}

// AppendEntries ...
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// DPrintf("Node %v (term %v) received AppendEntries from %v", rf.me, rf.currentTerm, args.LeaderID)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Reply false if term < currentTerm
	// Do not reset the election timer (don't reset lastheardfrom)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// 0. If RPC request or response contains term T > current Term:
	// set currentTerm = T, convert to follower
	// Note that if you receive an AppendEntries in the same term,
	// it's safe to demote to follower
	if args.Term >= rf.currentTerm {
		rf.lastHeardFrom = time.Now()
		rf.currentStatus = follower
		// TODO Increment term and do cleanup
		// What cleanup exactly?
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.votesReceived = []serverID{}
		rf.nextIndex = []uint{}
		rf.matchIndex = []uint{}
	}

	// 2. Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm.
	// Two subconditions:

	// 2.1. If log doesn't contain an entry at prevLogIndex, reply false
	// TODO assert here that args.PrevLogIndex >= 0
	if uint(len(rf.log)-1) < args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// 2.2. If log does contain an entry at prevLogIndex but prevLogTerm does not match,
	//      reply false.
	if len(rf.log) > 0 && uint(len(rf.log)-1) >= args.PrevLogIndex && rf.log[len(rf.log)-1].TermCreated != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// At this point, it should be the case that
	// log contains an entry at prevLogIndex (or the log is empty), AND
	// that prevLogTerm == log[prevLogIndex].
	// TODO write an assert here...

	// 3. If an existing entry conflicts with a new one,
	// (same index but different terms),
	// delete the existing entry and all that follow it
	truncateEntries := false
	// 4. Append any new entries not already in the log
	lastIndex := -1

	for i, entry := range args.Entries {
		logIndex := int(args.PrevLogIndex) + i + 1
		if logIndex >= len(rf.log) {
			rf.log = append(rf.log, entry)
		} else {
			// If the entry in the log conflicts with the incoming log,
			// truncate all future entries.
			// Overwrite the entry regardless:
			// if there is no conflict (same term), then it's OK
			// to rewrite the log since the master is what counts.
			if rf.log[logIndex].TermCreated != entry.TermCreated {
				truncateEntries = true
			}
			rf.log[logIndex] = entry
		}
		lastIndex = logIndex
	}

	// ONLY If we had a conflicting log entry do we truncate all entries after logIndex
	// lastIndex is the last entry in the log
	if truncateEntries {
		rf.log = rf.log[0 : lastIndex+1]
	}

	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < uint(lastIndex) {
			rf.commitIndex = args.LeaderCommit
		}
		rf.commitIndex = uint(lastIndex)
	}

	// Success reply
	reply.Success = true
	reply.Term = rf.currentTerm
	return

}

// RequestVoteArgs ...
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         term
	CandidateID  int
	LastLogIndex uint
	LastLogTerm  term
}

// RequestVoteReply ...
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        term
	VoteGranted bool
}

// RequestVote ...
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v (term %v) received RequestVote from %v", rf.me, rf.currentTerm, args.CandidateID)

	// 0. If RPC request or response contains term T > current Term:
	//    set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.lastHeardFrom = time.Now()
		rf.currentStatus = follower
		// TODO Increment term and do cleanup
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.votesReceived = []serverID{}
		rf.nextIndex = []uint{}
		rf.matchIndex = []uint{}
	}

	// 1. Reply false if term < currentTerm
	// Do not reset election timer
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// "Raft determines which of two logs is more up-to-date
	// by comparing the index and term of the last entries in the logs.
	// If the logs have last entries with different terms,
	// then the log with the later term is more up-to-date.
	// If the logs end with the same term, then whichever log is longer
	// is more up to date.
	reply.Term = rf.currentTerm
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && rf.logAtLeastUpToDate(args) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
	} else {
		reply.VoteGranted = false
	}
	return
}

func (rf *Raft) logAtLeastUpToDate(args *RequestVoteArgs) bool {
	// Checks if the log in RequestVote is at least as up to date as yours.
	// Returns true if their log is as up-to-date as yours,
	// otherwise returns false.

	// TODO assert here that len(rf.log) >= 1

	if len(rf.log) == 1 {
		return true
	}
	if args.LastLogTerm < rf.log[len(rf.log)-1].TermCreated {
		return true
	}
	if args.LastLogTerm == rf.log[len(rf.log)-1].TermCreated && args.LastLogIndex >= uint(len(rf.log)-1) {
		return true
	}
	return false
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		DPrintf("%v (term %v) received RequestVoteReply with result %v from %v",
			rf.me, rf.currentTerm, reply.VoteGranted,
			server)
		if reply.Term > rf.currentTerm {
			// convert to follower
			rf.currentStatus = follower
			// TODO Increment term and do cleanup
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.votesReceived = []serverID{}
			rf.nextIndex = []uint{}
			rf.matchIndex = []uint{}
			return ok
		}
		// check if my own request has expired
		// Thanks to Liang Jun for pointing this out
		if args.Term < rf.currentTerm { // reply.Term < rf.currentTerm also works
			return ok
		}
		if reply.Term == rf.currentTerm && reply.VoteGranted {
			rf.votesReceived = append(rf.votesReceived, server)
			if len(rf.votesReceived) > len(rf.peers)/2 {
				// Change to leader
				rf.currentStatus = leader
				rf.nextIndex = []uint{}
				rf.matchIndex = []uint{}
				for i, _ := range rf.peers {
					rf.nextIndex = append(rf.nextIndex, uint(len(rf.log)))
					rf.matchIndex = append(rf.nextIndex, 0)
					if i == rf.me {
						rf.matchIndex[i] = uint(len(rf.log))
					}
				}

				DPrintf("%v has become the leader in term %v!", rf.me, rf.currentTerm)
			}
		}
	}
	// TODO handle not OK??
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// SendAppendEntries cannot block!
	// This is because it will result in a deadlock if you lock the whole function.
	// So what you need to do is quickly lock to read the data
	// and update args.

	// Then once you receive, wait and grab the lock again,
	// and handle the mutation

	rf.mu.Lock()
	// Leaders point 3: If last log index >= nextIndex for a follower:
	// send AppendEntries RPC with log entries starting at nextIndex
	// and matchIndex for follower
	args.Term = rf.currentTerm
	args.LeaderID = rf.me
	rf.mu.Unlock()

	// This call is blocking
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	// Wait for a result, then come back to this thread...
	// and grab the lock again
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If AppendEntries fails because my term is less than the node's term,
	// immediately convert to follower and clean up.
	if reply.Term > rf.currentTerm {
		// convert to follower
		rf.currentStatus = follower
		// TODO Increment term and do cleanup
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.votesReceived = []serverID{}
		rf.nextIndex = []uint{}
		rf.matchIndex = []uint{}
		return ok
	}

	// If AppendEntries fails because my own request has expired,
	// (request term < current term), ignore it
	if args.Term < rf.currentTerm { // reply.Term < rf.currentTerm also works
		return ok
	}

	// Also, if I am no longer a leader: ignore it.
	if status(rf.currentStatus) != leader {
		return ok
	}

	// If AppendEntries fails because of log inconsistency:
	// decrement nextIndex and retry.
	if reply.Term == rf.currentTerm && !reply.Success {
		// TODO assert that rf.nextIndex[server] > 0
		rf.nextIndex[server]--
	}

	// If successful: update nextIndex and matchIndex for follower.

	if reply.Term == rf.currentTerm && reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + uint(len(args.Entries))
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	}

	// Point 4: If there exists an N such that N > commitIndex,
	// a majority of matchIndex[i] >= N, and
	// log[N].term == currentTerm:
	// set commitIndex = N

	// Why do we need to check log[N].term == currentTerm?
	// A leader is not
	// allowed to update commitIndex to somewhere in a previous term (or, for
	// that matter, a future term). Thus, as the rule says, you specifically
	// need to check that log[N].term == currentTerm. This is because Raft
	// leaders cannot be sure an entry is actually committed (and will not ever
	// be changed in the future) if it’s not from their current term. This is
	// illustrated by Figure 8 in the paper.

	// We may or may not want to apply the successful committed log entry here.

	return ok
}

// Start ...
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
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2B).
	// command is actually an int
	isLeader = status(rf.currentStatus) == leader

	// FIXME flag out check whether this is correct index
	index = int(len(rf.log) + 1)
	term = rf.currentTerm

	// We should return right away,
	// then start the agreement.
	// If we are the leader, we should try and
	// append entry to local log.
	// Even though the paper says
	// "respond after entry applied to state machine (§5.3)",
	// here we return immediately.

	if isLeader {
		rf.log = append(rf.log, LogEntry{term, command})
	}

	return index, term, isLeader
}

// Kill ...
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// DPrintf("%v (term %v) ticking", rf.me, rf.currentTerm)
		rf.mu.Lock()

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		time.Sleep(10 * time.Millisecond)

		if status(rf.currentStatus) == follower {
			// If election timeout elapses without receiving
			// AppendEntriesRPC or granting vote to candidate
			// convert to candidate
			if rf.lastHeardFrom.Add(rf.electionTimeout).Before(time.Now()) {
			}
			rf.lastHeardFrom = time.Now()
			rf.currentStatus = candidate
		}
		if status(rf.currentStatus) == candidate {
			// If election timeout elapses: start new election
			if rf.lastHeardFrom.Add(rf.electionTimeout).Before(time.Now()) {
				rf.lastHeardFrom = time.Now()
				// increment term, start again
				rf.currentTerm++
				rf.electionTimeout = time.Duration(minElectionTimeoutMS+rand.Intn(maxElectionTimeoutMS-minElectionTimeoutMS)) * time.Millisecond
				rf.votedFor = rf.me
				rf.votesReceived = append(rf.votesReceived, rf.me)
				for i := range rf.peers {
					if i == rf.me {
						continue
					}
					go rf.sendRequestVote(i,
						&RequestVoteArgs{
							Term:        rf.currentTerm,
							CandidateID: rf.me,
						},
						&RequestVoteReply{})

				}
			}
		}
		if status(rf.currentStatus) == leader {
			// Don't care about election timer just keep sending
			if rf.lastSentHeartbeat.Add(time.Duration(timeBetweenHeartBeatsMS) * time.Millisecond).Before(time.Now()) {
				for i := range rf.peers {
					if i == rf.me {
						continue
					}
					go rf.sendAppendEntries(i,
						&AppendEntriesArgs{},
						&AppendEntriesReply{},
					)
				}
				rf.lastSentHeartbeat = time.Now()
			}
		}
		rf.mu.Unlock()
	}
}

// Make ...
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.currentStatus = follower
	rf.lastHeardFrom = time.Now()
	rf.electionTimeout = time.Duration(minElectionTimeoutMS+rand.Intn(maxElectionTimeoutMS-minElectionTimeoutMS)) * time.Millisecond
	rf.log = append(rf.log, LogEntry{-1, -1})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
