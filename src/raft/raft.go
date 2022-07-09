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
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// ApplyMsg
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

type NodeState int8

const (
	Follower  NodeState = 0
	Candidate           = 1
	Leader              = 2
)

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	currentTerm int // latest term server has seen, init to 0, increase monotonically
	votedFor    int // candidateID that received vote in current term (or -1 if none, spec says null but id prefer not to use null here)

	// volatile state
	commitIndex int // index of the highest log entry known to be committed
	lastApplied int // index of the highest log entry applied to state machine

	// volatile state only leaders only (reinit after election)
	nextIndex  []int // for each server, index of next log entry to send to that server. init to leader last log index + 1
	matchIndex []int // for each server, index of the highest log entry known to be replicated on server

	// other data that i added
	state               NodeState
	lastHeardFromLeader time.Time

	electionTimeout time.Duration
	votingTimeout   time.Duration
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// TODO: do i need to protect with mutex?

	// Your code here (2A).
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RPCS  ----------------------

// RequestVoteArgs
// RequestVote RPC arguments structure.
// field names must be exported!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term         int
	CandidateID  int
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// RequestVoteReply
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote ...
// RequestVote RPCs are initiated by candidates during an election
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("RequestVote for node: %d", rf.me)

	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = -1
		reply.VoteGranted = false
		return
	}

	// if votedFor is null or candidateId, and candidate's log is at least as up to date as receiver's log, grant vote
	if (rf.votedFor < 0 || rf.votedFor == args.CandidateID) && (args.LastLogIndex >= rf.commitIndex) {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
	}
}

// AppendEntriesArgs ...
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []byte
	LeaderCommit int // leader's commit index
}

// AppendEntriesReply ...
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
}

// AppendEntries ...
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).

	// TODO implement heartbeat code

	// if entries is nil, this is heart beat
	if args.Entries == nil {
		lastHeard := time.Now().UTC()
		rf.lastHeardFromLeader = lastHeard
	}
}

func (rf *Raft) beginElection() error {
	// TODO: worry about race conditions later

	// after election timeout, begin election

	// increment current term and transition to candidate
	// TODO: maybe there should be a 'potential term' variable instead
	//rf.currentTerm += 1
	//newTerm := rf.currentTerm + 1
	// TODO: should probably create func SetCandidate with mutex protection
	rf.state = Candidate

	// current node (candidate) votes for itself and
	// TODO: might need separate storage for this
	votes := make(map[int]int)
	votes[rf.me] += 1

	votingTimeout := time.NewTicker(rf.votingTimeout)
	// issue RequestVote RPCs in parallel to every other server in the cluster
	// Candidate continues in this state unless
	// it wins the election (has majority vote)
	//  - receives votes from majority of servers in the full cluster for the same term
	//  - each server will vote for at most one candidate in a given term, on a first-come-first-served basis
	//  - once becoming leader, it sends heart beat messages to all other servers to establish its authority and prevent new elections
	// another server establishes itself as the leader
	// a period of time goes by with no winner
	continueVoting := true
	for continueVoting {
		// if no longer a candidate, stop the process
		if rf.state != Candidate {
			continueVoting = false
			_, _ = DPrintf("node is no longer a candidate, canceling vote for term: %d, %d", rf.me, rf.currentTerm)
		}
		select {
		case <-votingTimeout.C:
			continueVoting = false
			_, _ = DPrintf("voting timed out for node: %d", rf.me)
			// when this happens, a new election should happen
			// new election should increment term again and initiate another round of RequestVote RPCs
			// TODO: should break this up into logical parts, based on how this election-retry should work
		default:
			// TODO: sleep for some duration
			time.Sleep(time.Millisecond * 1)
		}
	}

	// while voting, a candidate may receive AppendEntries RPCs from another server claiming to be the leader
	// if the leader's term (included in its RPC) is at least as large as the candidate's term, then the candidate recognizes
	// the leader as legitimate and returns to follower state
	// if te term in the RPC is smaller than the candidate's current term, then the candidate rejects the RPC and continues in candidate state

	return nil
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
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
	for rf.killed() == false {
		// if leader, send heart beat (empty append entries)
		switch rf.state {
		case Follower:
			// if follower hasnt received communication since election timeout, it assumes no viable leader and begins and election to choose a new leader
			delta := time.Now().UTC().Sub(rf.lastHeardFromLeader)
			if delta > rf.electionTimeout {
				// probs makes sense just to change state to candidate
				rf.beginElection()
			}
		case Candidate:
		case Leader:
		}

		// once we're done all the operations, sleep

		// TODO: randomize sleep
		time.Sleep(time.Millisecond * 500)
	}
}

// Make
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// when servers start up, they begin as followers
	// Follower is the default value, but better to be explicit
	rf.state = Follower

	// randomize electionTimeout, between 150-300 ms
	// want to spread out servers so that in most cases only a single server will timeout
	// same mechanism is used to handle split votes
	// each candidate restarts its randomized election timout at the start of the election, and waits for that timeout before starting the next election
	// this reduces the likelihood of split vote in the new election

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
