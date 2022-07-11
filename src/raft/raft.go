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

	// volatile state
	commitIndex int // index of the highest log entry known to be committed
	lastApplied int // index of the highest log entry applied to state machine

	// volatile state only leaders only (reinit after election)
	nextIndex  []int // for each server, index of next log entry to send to that server. init to leader last log index + 1
	matchIndex []int // for each server, index of the highest log entry known to be replicated on server

	state NodeState

	followerData  FollowerMetaData
	candidateData CandidateMetaData
	leaderData    LeaderMetaData

	electionTimeout time.Duration
	votingTimeout   time.Duration
}

type FollowerMetaData struct {
	lastHeardFromLeader time.Time
	currTerm            int // latest term server has seen, init to 0, increase monotonically
	currLeader          int // current leader for term
	votedFor            int // candidateID that received vote in current term (or -1 if none, spec says null but id prefer not to use null here)
}

type CandidateMetaData struct {
	election Election
}

type LeaderMetaData struct {
	currTerm int
}

type Election struct {
	startedAt    time.Time // if curr-startedAt is greater than timeout period, re-election
	forTerm      int       // might be able to use term on raft directly, want to separate for now
	votes        []int
	forCandidate int

	mut sync.Mutex
}

func NewElection(forCandidate, term, clientCount int) Election {
	votes := make([]int, clientCount)
	return Election{
		startedAt:    time.Now().UTC(),
		forTerm:      term,
		votes:        votes,
		forCandidate: forCandidate,
	}
}

func (e *Election) AddVote(fromCli int) {
	e.mut.Lock()
	defer e.mut.Unlock()

	e.votes[fromCli] += 1
}

func (e *Election) CandidateHasMajority() bool {
	e.mut.Lock()
	defer e.mut.Unlock()

	var (
		votesNeeded = 1 + (len(e.votes) / 2)
		votesRecv   int
	)

	for i, votes := range e.votes {
		if votes > 0 {
			// each server only gets one vote per election term
			if votes != 1 {
				DPrintf("server %d voted more than once: %d", i, votes)
			}
			votesRecv += 1
		}
	}

	return votesRecv >= votesNeeded
}

// GetState return currTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch rf.state {
	case Leader:
		return rf.leaderData.currTerm, true
	case Follower:
		return rf.followerData.currTerm, false
	case Candidate:
		// TODO: not sure if election term is correct or not
		return rf.candidateData.election.forTerm, false
	default:
		// should never happen, <3 rust enum
		return -1, false
	}
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

	switch rf.state {
	case Candidate:
		// TODO: check what to do here
	case Follower:
		// if vote request is for an old term, say no
		if args.Term < rf.followerData.currTerm {
			reply.Term = -1
			reply.VoteGranted = false
			return
		}
		// if votedFor is null or candidateId, and candidate's log is at least as up to date as receiver's log, grant vote
		//if (rf.votedFor < 0 || rf.votedFor == args.CandidateID) && (args.LastLogIndex >= rf.commitIndex) {
		//	reply.VoteGranted = true
		//	reply.Term = rf.currentTerm
		//}
	case Leader:
		// TODO: check what to do here
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

	switch rf.state {
	case Candidate:
		// if candidate receives AppendEntries RPC from new leader, convert to follower
		// since this server was a candidate, assume it didnt vote for the curr leader
		rf.TransitionToFollower(args.LeaderID, args.Term, -1)
	case Follower:
		rf.followerData.lastHeardFromLeader = time.Now().UTC()
	case Leader:
		// TODO: not sure what
	}
}

func (rf *Raft) TransitionToFollower(newLeader, newTerm, votedFor int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch rf.state {
	case Candidate:
		DPrintf("server %d: transitioning from candidate to follower", rf.me)
		rf.state = Follower
		rf.followerData = FollowerMetaData{
			lastHeardFromLeader: time.Now().UTC(),
			currTerm:            newTerm,
			currLeader:          newLeader,
			votedFor:            votedFor,
		}
	case Follower:
		// shouldn't happen
	case Leader:
		// TODO: figure out what to do here
	}

}

func (rf *Raft) TransitionToLeader(newTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch rf.state {
	case Candidate:
		DPrintf("server %d: transitioning from candidate to follower", rf.me)
		rf.state = Leader
		rf.leaderData = LeaderMetaData{newTerm}
	case Follower:
		// should never happen
	case Leader:
		// should never happen
	}
}

func (rf *Raft) TransitionToCandidate(forTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch rf.state {
	case Follower:
		// should never happen
		rf.state = Candidate
		newElection := NewElection(rf.me, forTerm, len(rf.peers))
		// current node (candidate) votes for itself and
		newElection.AddVote(rf.me)
		rf.candidateData = CandidateMetaData{election: newElection}
	case Candidate:
		// should never happen
	case Leader:
		// should never happen
	}
}

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
// heartbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		switch rf.state {
		case Follower:
			// if follower hasnt received communication since election timeout, it assumes no viable leader and begins and election to choose a new leader
			delta := time.Now().UTC().Sub(rf.followerData.lastHeardFromLeader)
			if delta > rf.electionTimeout {
				rf.TransitionToCandidate(rf.followerData.currTerm + 1)
			}
		case Candidate:

			// if candidate has majority, transition to leader
			if rf.candidateData.election.CandidateHasMajority() {
				rf.TransitionToLeader(rf.candidateData.election.forTerm)
				continue
			}

			// if election timeout, start a new election
			delta := time.Now().UTC().Sub(rf.candidateData.election.startedAt)
			if delta > rf.electionTimeout {
				//  start a new election
				DPrintf("server %d: election timeout during active election, starting new election", rf.me)
				newElection := NewElection(rf.me, rf.candidateData.election.forTerm+1, len(rf.peers))
				rf.candidateData.election = newElection
				continue
			}

			// otherwise, send out votes

			DPrintf("server %d: sending out RequestVotes", rf.me)
			for i := range rf.peers {
				// dont need to send to self
				if i == rf.me {
					continue
				}
				// TODO: in parallel
				args := RequestVoteArgs{
					Term:         rf.candidateData.election.forTerm,
					CandidateID:  rf.me,
					LastLogIndex: rf.lastApplied,
					LastLogTerm:  -1, // TODO: this is wrong
				}
				reply := RequestVoteReply{}
				success := rf.sendRequestVote(i, &args, &reply)
				if !success {
					DPrintf("sendRequestVote failed: %v", args)
					return
				}
				// if the vote was granted and
				if reply.VoteGranted && reply.Term == rf.candidateData.election.forTerm {
					DPrintf("%d received vote from: %d for term: %d", rf.me, i, rf.candidateData.election.forTerm)
					rf.candidateData.election.AddVote(i)
				}
				// TODO: handle case if vote not granted or for term is wrong
			}
		case Leader:
			// send heartbeats
			for i := range rf.peers {
				// dont need to send to self
				if i == rf.me {
					continue
				}
				// TODO: in parallel
				args := AppendEntriesArgs{
					Term:     rf.leaderData.currTerm,
					LeaderID: rf.me,
				}
				reply := AppendEntriesReply{}
				success := rf.sendAppendEntries(i, &args, &reply)
				if !success {
					DPrintf("sendRequestVote failed: %v", args)
					continue
				}
				// TODO: handle these later
			}
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
