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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	//	"6.824/labgob"
	"6.824/labrpc"
	// "golang.org/x/crypto/ssh/terminal"
)

const (
	Follower int32 = 0
	Candidate int32 = 1
	Leader int32 = 2
)
//
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu				sync.Mutex          // Lock to protect shared access to this peer's state
	appendEntriesCond 		*sync.Cond
	applierCond		*sync.Cond
	peers			[]*labrpc.ClientEnd // RPC end points of all peers
	persister		*Persister          // Object to hold this peer's persisted state
	me		        int                 // this peer's index into peers[]
	dead	        int32               // set by Kill()
	logs			[]LogEntry          // logs
	voted			bool				// have voted in current term
	votedFor  		int          		// voted for
	currentTerm		int					// current term number
	commitIndex 	int					// index of highest log entry known to be committed (init 0)
	lastApplied		int					// index of the highest log entry applied to the state machine
	state			int32 		// Follower, Candidate, Leader
	firstIdx		int					// firstIdx: 1 if no snapshot, can be others if snapshot
	recvHB			bool				// received HB within the interval
	leaderStates	RaftLeader
	applyCh			chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type RaftLeader struct {
	nextIndex		[]int
	matchIndex		[]int
}

type LogEntry struct {
	Term int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state==Leader)
	return term, isleader
}


// func (rf* Raft) getInternalState() (currentTerm int,lastLogIdx int,
// 	 LastLogTerm int, commitIndex int, lastApplied int, state int32) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	currentTerm = rf.currentTerm
// 	lastLogIdx = rf.slice2Log(len(rf.logs)-1)
// 	LastLogTerm = rf.getLastLogTerm()
// 	commitIndex = rf.commitIndex
// 	lastApplied = rf.lastApplied
// 	state = rf.state
// 	return
// }

func (rf* Raft) getLogTermSafe(idx int) (int) {
	if rf.log2Slice(idx)>=0 {
		return rf.logs[rf.log2Slice(idx)].Term
	} else {
		return 0
	}
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


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	DPrintf("ID %d starts Election term %d",rf.me,rf.currentTerm)
	rf.state = Candidate
	rf.voted = true
	rf.votedFor = rf.me
	currentTerm := rf.currentTerm
	lastLogIdx := rf.slice2Log(len(rf.logs)-1)
	LastLogTerm := rf.getLogTermSafe(lastLogIdx)
	rf.mu.Unlock()
	voteArgs := &RequestVoteArgs{
		Term: currentTerm,
		CandidateId: rf.me,
		LastLogIndex: lastLogIdx,
		LastLogTerm: LastLogTerm,
	}
	voteChannel := make(chan bool)
	voteCnt, yesCnt := 0,0
	for id:=0; id<len(rf.peers); id++ {
		if id==rf.me {
			continue
		}
		go func (peerId int)  {
			replyArgs := &RequestVoteReply{
				Term: -1,
				VoteGranted: false,
			}
			ok := rf.sendRequestVote(peerId,voteArgs,replyArgs)
			if ok {
				voteChannel<-replyArgs.VoteGranted
			} else {
				voteChannel<-false
			}
		} (id)
	}
	/* vote for myself */
	voteCnt ++
	yesCnt ++
	/* get votes  */
	for {
		recvVote:=<-voteChannel
		voteCnt++
		if recvVote {
			yesCnt ++
		}
		if yesCnt>len(rf.peers)/2||voteCnt>=len(rf.peers) {
			break
		}
	}
	if yesCnt>len(rf.peers)/2 {
		DPrintf("ID %d get enough vote for term %d.",rf.me,rf.currentTerm)
		rf.mu.Lock()
		state:= rf.state
		if rf.currentTerm==currentTerm && state==Candidate {
			DPrintf("ID %d becomes leader for term %d.",rf.me,rf.currentTerm)
			rf.state = Leader
			rf.leaderStates = RaftLeader{
				nextIndex: make([]int, len(rf.peers)),
				matchIndex: make([]int, len(rf.peers)),
			}
			for i:=0;i<len(rf.leaderStates.nextIndex);i++ {
				rf.leaderStates.nextIndex[i] = rf.slice2Log(len(rf.logs))
				rf.leaderStates.matchIndex[i] = 0
			}
			rf.appendEntriesCond.Broadcast()
		}
		rf.mu.Unlock()
	}
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	reply.Term = rf.currentTerm
	if args.Term<rf.currentTerm {
		DPrintf("RequestVote called for candidate #%d at term %d, not voted as its term is behind",args.CandidateId,args.Term)
		reply.VoteGranted = false
		return
	}
	myLastLogIdx := rf.slice2Log(len(rf.logs)-1)
	myLastLogTerm := rf.getLogTermSafe(myLastLogIdx)
	upToDate := args.LastLogTerm > myLastLogTerm || 
	(args.LastLogTerm==myLastLogTerm && args.LastLogIndex>=myLastLogIdx)
	if args.Term>rf.currentTerm {
		rf.voted = false
		rf.currentTerm = args.Term
		rf.recvHB = true
		DPrintf("[ID %d] RequestVote called for candidate #%d at term %d, update my term to his",
		rf.me,args.CandidateId,args.Term)
	}
	if (!rf.voted || rf.votedFor == args.CandidateId) && upToDate {
		DPrintf("[ID %d] RequestVote called for candidate #%d at term %d, update my term to his",
		rf.me,args.CandidateId,args.Term)
		reply.VoteGranted = true
		rf.voted = true
		rf.votedFor = args.CandidateId
		rf.recvHB = true
		rf.state = Follower
		return
	} else {
		reply.VoteGranted = false
		return
	}
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
	return ok
}

func (rf *Raft) sendAE(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


//
// Append Entries RPC arguments data structrue
//
type AppendEntriesArgs struct {
	Term int			// leader's term
	PrevLogIndex int	// used to uphold consistency
	PrevLogTerm  int	// used to uphold consistency
	Entries	[]LogEntry  // log entries payload
	LeaderCommit int	// latest commit point
}

//
// Append Entries RPC reply data structrue
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term int			// inform leader of my current term
	Success bool		// successfully appended the entry
}

type AER struct {
	Term int
	Success bool
	Id int
}

// In case index does not match slice index (with snapshot)
func (rf *Raft) log2Slice(idx int) (int){
	return idx-rf.firstIdx
}

func (rf *Raft) slice2Log(idx int) (int){
	return idx+rf.firstIdx
}

// Append Entries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("[ID %d] AppendEntires:replyArgs %d.",rf.me,reply.Me)
	/* catch up with fall behind the term */
	if rf.currentTerm<=args.Term {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.voted = false
	}
	reply.Term = rf.currentTerm
	/* sender term lagging behind */
	if args.Term<rf.currentTerm {
		reply.Success = false
		return
	}
	/* knowing received from a real leader from this point*/
	// DPrintf("[ID %d] Received AE from leader of term %d",rf.me,args.Term)
	rf.recvHB = true
	/* Consistency check */
	if  args.PrevLogIndex>=rf.slice2Log(len(rf.logs)) || 
			rf.getLogTermSafe(args.PrevLogIndex)!=args.PrevLogTerm {
		reply.Success = false
		return
	}
	if len(args.Entries)>0 {
		for i,ent:= range args.Entries {
			sliceIdx := rf.log2Slice(args.PrevLogIndex+1+i)
			if sliceIdx<len(rf.logs) {
				if rf.logs[sliceIdx]!=ent {
					rf.logs[sliceIdx] = ent
					rf.logs = rf.logs[:sliceIdx+1]
				}
			} else if sliceIdx==len(rf.logs){
				rf.logs = append(rf.logs, ent)
			} else {
				panic("Accepting an entry out of bound!")
			}
		}
		DPrintf("[ID %d] Append Logs to be %v at term %d",rf.me,rf.logs,rf.currentTerm)
	}
	rf.commitIndex = min(args.LeaderCommit,args.PrevLogIndex+len(args.Entries))
	rf.applierCond.Broadcast()
	reply.Success = true
}


// send will block until rf.state becomes leader
func (rf *Raft) blockingSendAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.state!=Leader {
		rf.appendEntriesCond.Wait()
	}
	replyChan := make(chan AER)
	replyCnt := 0
	sucessCnt := 0
	receiveCnt := 0
	for id:=0; id<len(rf.peers); id++ {
		if id==rf.me {
			continue
		}
		go func (peerId int)  {
			replyArgs := &AppendEntriesReply{
				Term: -1,
				Success: false,
			}
			payload := make([]LogEntry, 0)
			if rf.log2Slice(rf.leaderStates.nextIndex[peerId])<len(rf.logs){
				payload = rf.logs[rf.log2Slice(rf.leaderStates.nextIndex[peerId]):]
			}
			sendArgs := &AppendEntriesArgs{
				Term: rf.currentTerm,
				PrevLogIndex: rf.leaderStates.nextIndex[peerId]-1,
				PrevLogTerm: rf.getLogTermSafe(rf.leaderStates.nextIndex[peerId]-1),
				Entries: payload,
				LeaderCommit: rf.commitIndex,
			}
			// DPrintf("[ID %d] I am sending out LeaderCommit of  %d",rf.me,sendArgs.LeaderCommit)
			//DPrintf("[ID %d] As a leader, I am sending out AE to %d",rf.me,peerId)
			ok := rf.sendAE(peerId,sendArgs,replyArgs)
			if ok {
				replyChan<-AER{
					Term: replyArgs.Term,
					Success: replyArgs.Success,
					Id: peerId,
				}

				//DPrintf("[ID %d] [peerID %d] replyArgs %v.",rf.me,peerId,replyArgs)
			} else {
				replyChan<-AER{
					Term: -1,
					Success: false,
					Id: -1,
				}
			}
		} (id)
	}
	sucessCnt++ // count myself towards sucess
	for replyCnt<len(rf.peers)-1 && sucessCnt<=len(rf.peers)/2{
		replyArgs:=<-replyChan
		replyCnt++
		if replyArgs.Id == -1 {
			continue
		}
		receiveCnt++
		if replyArgs.Term > rf.currentTerm {
			rf.state = Follower
			rf.voted = false
			rf.recvHB = true
			continue
		}
		if replyArgs.Success {
			sucessCnt++
			if rf.leaderStates.nextIndex[replyArgs.Id] != rf.slice2Log(len(rf.logs)) {
				DPrintf("[ID %d] Update next index for %d, sucessCnt = %d",rf.me,replyArgs.Id,sucessCnt)
				rf.leaderStates.matchIndex[replyArgs.Id] = rf.slice2Log(len(rf.logs)-1)
				rf.leaderStates.nextIndex[replyArgs.Id] = rf.slice2Log(len(rf.logs))
			}
		} else {
			rf.leaderStates.nextIndex[replyArgs.Id] = max(rf.leaderStates.nextIndex[replyArgs.Id]-1,1)
		}
	}
	// if receiveCnt+1<len(rf.peers)/2 {
	// 	rf.state = Follower
	// }
	// DPrintf("[ID %d] successCnt = %d",rf.me,sucessCnt)
	if sucessCnt>len(rf.peers)/2 && rf.commitIndex != rf.slice2Log(len(rf.logs)-1) {
		DPrintf("[ID %d] Commit Log(%d)",rf.me,rf.slice2Log(len(rf.logs)-1))
		rf.commitIndex = rf.slice2Log(len(rf.logs)-1)
		rf.applierCond.Broadcast()
	}
}


//
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.state==Leader
	if !isLeader {
		return -1,-1,isLeader
	}
	//DPrintf("[ID %d] log append %v",rf.me,command)
	rf.logs = append(rf.logs, LogEntry{
		Term: rf.currentTerm,
		Command: command,
	})
	DPrintf("[ID %d - Leader] Append Logs to be %v at term %d",rf.me,rf.logs,rf.currentTerm)
	index := rf.slice2Log(len(rf.logs)-1)
	term := rf.currentTerm
	

	// Your code here (2B).


	return index, term, isLeader
}

//
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
	rand.Seed(time.Now().UnixNano()+int64(rf.me))
	for !rf.killed() {
		time.Sleep(time.Millisecond*time.Duration(100*(rand.Intn(4)+4)))
		rf.mu.Lock()
		needElect := !rf.recvHB && rf.state!=Leader
		rf.recvHB = false
		rf.mu.Unlock()
		if needElect {
			rf.startElection()
		}
	}
}

func (rf *Raft) applier(){
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied==rf.commitIndex {rf.applierCond.Wait()}
		{
		// safe applier section here
			startPoint := rf.lastApplied+1
			for i:=startPoint;i<=rf.commitIndex;i++ {
				DPrintf("[ID %d] Apply log #%d",rf.me,i)
				rf.applyCh<-ApplyMsg{
					CommandValid: true,
					Command: rf.logs[rf.log2Slice(i)].Command,
					CommandIndex: i,
				}
				rf.lastApplied = i
			}
		}
		rf.mu.Unlock()
	}
}


func (rf *Raft) ae_ticker() {
	for !rf.killed() {
		rf.blockingSendAppendEntries()
		time.Sleep(time.Millisecond*100)
	}
}

//
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
	rf.appendEntriesCond = sync.NewCond(&rf.mu)
	rf.logs = make([]LogEntry,0)
	rf.currentTerm = 0
	rf.voted = false
	rf.recvHB = false
	rf.state = Follower
	rf.applyCh = applyCh
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.firstIdx = 1
	rf.applierCond = sync.NewCond(&rf.mu)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.ae_ticker()
	go rf.applier()

	return rf
}


func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}

func max(a, b int) int {
    if a > b {
        return a
    }
    return b
}