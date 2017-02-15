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
	"labrpc"
	"math/rand"
	"sync"
	"time"

	ls "github.com/Sirupsen/logrus"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type eventMsg struct {
	eventType RaftEvent
	data      interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent stat on all servers
	currentTerm int
	votedFor    int
	logs        []Log

	//volatile state on all servers
	commitIndex int
	lastApplied int

	//Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg

	state RaftState

	eventMsgChan chan eventMsg //message of events

	electionTimer       *time.Timer  //timer for election timeout
	appendEntriesTicker *time.Ticker //ticker for background append entries
	electionTimeout     int          //ms

	rpcCallChan chan rpcReq
}

//rpcReq is struct for all RPCs
type rpcReq struct {
	peer      *labrpc.ClientEnd
	method    RaftRPCMethod
	args      interface{}
	reply     interface{}
	replyChan chan rpcReply
}

//rpcReply is struct for rpc replys
type rpcReply struct {
	ok    bool
	reply interface{}
}

//AppendEntriesArgs is struct for RPC of AppendEntries's args
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

//AppendEntriesReply is struct for RPC of AppendEntries's reply
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//ReqeustVoteArgs is struct for RPC of RequestVote's args
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//ReqeustVoteArgs is struct for RPC of RequestVote's reply
type RequestVoteReply struct {
	// Your data here.
	Me          int
	Term        int
	VoteGranted bool
}

//IsLeader returns if rf is a leader
func (rf *Raft) IsLeader() bool {
	return rf.state == stLeader
}

//IsCandidate returns if rf is in candidate state
func (rf *Raft) IsCandidate() bool {
	return rf.state == stCandidate
}

//IsFollower returns if rf is in follower state
func (rf *Raft) IsFollower() bool {
	return rf.state == stFollower
}

// GetState return rf's current term and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.IsLeader()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//TODO
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//TODO
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//RequestVote is this server's RPC handler for RPC of RequestVote
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	reply.VoteGranted = false
	reply.Me = rf.me
	ls.Debugf("Peer #%d current vote for %v.", rf.me, rf.votedFor)
	ls.Debugf("Peer #%d recv req vote from #%d. l,r: %d,%d %d,%d",
		rf.me, args.CandidateId,
		rf.currentTerm, args.Term,
		rf.lastApplied, args.LastLogIndex)
	if rf.currentTerm > args.Term {
		//remote term is smaller, reject
		ls.Debugf("Peer #%d reject vote from #%d for smaller term",
			rf.me, args.CandidateId)
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm && !rf.IsFollower() {
		//state to follower
		ls.Debugf("Peer #%d state to follower for smarller term.", rf.me)
		ev := eventMsg{}
		ev.eventType = evStateToFollower
		ev.data = args
		rf.eventMsgChan <- ev
		if args.LastLogIndex >= rf.lastApplied {
			reply.VoteGranted = true
			rf.mu.Lock()
			rf.votedFor = args.CandidateId
			rf.mu.Unlock()
		}
		return
	}
	if args.Term >= rf.currentTerm {
		if rf.votedFor == votedForNone || rf.votedFor == args.CandidateId {
			if args.LastLogIndex >= rf.lastApplied {
				reply.VoteGranted = true
				rf.mu.Lock()
				rf.votedFor = args.CandidateId
				rf.mu.Unlock()
			}
		}
	}
	ls.Debugf("Peer #%d vote for peer #%d, reply %v.",
		rf.me, args.CandidateId, reply.VoteGranted)
	return
}

//AppendEntries is this server's RPC handler for RPC of AppendEntries
func (rf *Raft) AppendEntries(
	args AppendEntriesArgs, reply *AppendEntriesReply,
) {
	ls.Debugf("Peer #%d in handler. args %v", rf.me, args)
	go rf.doUpdateFromAppendEntries(args)
	reply.Success = true
	reply.Term = args.Term
	return
}

func (rf *Raft) floodAppendEntries(args AppendEntriesArgs, c chan rpcReply) {
	for _, peer := range rf.peers {
		reply := new(AppendEntriesReply)
		rpc := rpcReq{
			peer:      peer,
			method:    "Raft.AppendEntries",
			args:      args,
			reply:     reply,
			replyChan: c,
		}
		rf.rpcCallChan <- rpc
	}
}

func (rf *Raft) floodRequestVote(args RequestVoteArgs, c chan rpcReply) {
	for _, peer := range rf.peers {
		reply := new(RequestVoteReply)
		//ok := peer.Call("Raft.RequestVote", args, reply)
		rpc := rpcReq{
			peer:      peer,
			method:    "Raft.RequestVote",
			args:      args,
			reply:     reply,
			replyChan: c,
		}
		rf.rpcCallChan <- rpc
	}
}

//floodRPCCalls floods all ClientEnd RPCs to rpcCallChan
//bgRPCCall func would receive these calls and make real calls
func (rf *Raft) floodRPCCalls(
	method RaftRPCMethod, args interface{}, c chan rpcReply,
) {
	var reply interface{}
	for _, peer := range rf.peers {
		switch method {
		case rpcMethodAppendEntries:
			reply = new(AppendEntriesReply)
		case rpcMethodRequestVote:
			reply = new(RequestVoteReply)
		}
		rpc := rpcReq{
			peer:      peer,
			method:    method,
			args:      args,
			reply:     reply,
			replyChan: c,
		}
		rf.rpcCallChan <- rpc
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
//NoUSE
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
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
//
//TODO
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	//index := -1
	//term := -1
	//isLeader := true

	if !rf.IsLeader() {
		return -1, -1, false
	} else {
		log := Log{
			Command: command,
			Term:    rf.currentTerm,
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.logs = append(rf.logs, log)
		rf.commitIndex++
		rf.lastApplied++
		return rf.commitIndex, rf.currentTerm, true
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//TODO
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
//Make makes a new raft server
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.applyCh = applyCh
	rf.logs = make([]Log, 0)
	rf.votedFor = votedForNone
	l := len(peers)
	rf.nextIndex = make([]int, l)
	rf.matchIndex = make([]int, l)
	rf.state = stFollower

	rf.eventMsgChan = make(chan eventMsg, defaultEventChannelSize) //magic number...
	rf.rpcCallChan = make(chan rpcReq, defaultRPCChannelSize)      //magic number...
	rf.electionTimeout = rf.getElectionTimeout()
	ls.Debugf("Peer #%d init with election timeout of %d ", me, rf.electionTimeout)

	go rf.eventLoop()
	go rf.bgElectionTimer()
	go rf.bgHeartbeatsSync()
	go rf.bgRPCCall()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

//eventLoop watches the eventMsgChan and call the relevant funcs
func (rf *Raft) eventLoop() {
	for {
		ev := <-rf.eventMsgChan
		switch ev.eventType {
		case evElectionTimeout:
			rf.doElection()
		case evLeaderGranted:
			rf.doWinLeaderElection()
		case evStateToFollower:
			rf.doStateToFollower(ev.data)
		}
	}
}

//doElection does election
func (rf *Raft) doElection() {
	rf.electionTimer.Stop()

	rf.mu.Lock()
	rf.currentTerm = rf.currentTerm + 1 // use currentTerm plus 1 for new election
	rf.votedFor = votedForNone
	rf.state = stCandidate
	rf.mu.Unlock()

	//Notice: this should after the increase currentTerm above
	args := rf.makeRequestVoteRPCArgs()

	l := len(rf.peers)
	majority := int(l/2) + 1
	replyChan := make(chan rpcReply, l)
	ls.Debugf("Peer #%d report that total %d peers ready to election", rf.me, l)
	rf.floodRequestVote(args, replyChan)

	votedCount := 0
	replyCount := 0
	for {
		r := <-replyChan
		ls.Debugf("Peer #%d got reply msg %v ", rf.me, r)
		replyCount++
		if r.ok && r.reply.(*RequestVoteReply).VoteGranted {
			votedCount++
			if votedCount >= majority && rf.state == stCandidate {
				//vote granted
				ls.Infof("Peer #%d granted for leader with %d votes, total %d.",
					rf.me, votedCount, l)
				rf.eventMsgChan <- eventMsg{eventType: evLeaderGranted}
				//TODO do some change to state
				break
			}
		}

		if replyCount >= l {
			ls.Infof("Peer #%d could not get majoriy. got %d votes, need %d.",
				rf.me, votedCount, majority)
			break
		}
	}
	rf.resetElectionTimer()
}

//doWinLeaderElection do staff after winning election
func (rf *Raft) doWinLeaderElection() {
	ls.Infof("Peer #%d win leader election. Term: %d.", rf.me, rf.currentTerm)
	rf.mu.Lock()
	rf.state = stLeader
	rf.mu.Unlock()
	//replyChan := make(chan AppendEntriesRPCReply, len(rf.peers))
	args := rf.makeAppendEntriesRPCArgs()
	ls.Debugf("Peer #%d send append entris rpc %v", rf.me, args)
	rf.floodAppendEntries(args, nil)
}

//doUpdateFromAppendEntries do staff after recv append entries rpc
func (rf *Raft) doUpdateFromAppendEntries(args AppendEntriesArgs) {
	rf.resetElectionTimer()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		ls.Warnf("Peer #%d invalid update term. local %d is greater %d",
			rf.me, rf.currentTerm, args.Term)
	}
	rf.currentTerm = args.Term //use leader's term
	ls.Debugf("Peer #%d update current term to args term: %d", rf.me, args.Term)
	if rf.state == stCandidate {
		rf.state = stFollower
	}
}

//doStateToFollower do staff after state transfered to follower
func (rf *Raft) doStateToFollower(args interface{}) {
	rf.mu.Lock()
	//do not modify currentTerm because voting candidate might not be leader?
	rf.state = stFollower
	rf.votedFor = votedForNone
	rf.mu.Unlock()
	rf.resetElectionTimer()
}

//doRPCCall is the real RPC
func doRPCCall(req rpcReq) {
	ok := req.peer.Call(string(req.method), req.args, req.reply)
	reply := rpcReply{}
	reply.ok = ok
	reply.reply = req.reply
	req.replyChan <- reply
}

//bgElectionTimer is a background timer for election timeout
func (rf *Raft) bgElectionTimer() {
	if rf.electionTimer == nil {
		rf.electionTimer = time.NewTimer(
			time.Duration(rf.electionTimeout) * time.Millisecond)
	}
	for {
		<-rf.electionTimer.C
		ls.Infof("BG Peer #%d election timeouted, make election.", rf.me)
		rf.eventMsgChan <- eventMsg{eventType: evElectionTimeout}
		//TODO do some change to state?
	}
}

//bgHeartbeatsSync is a background ticker for
//default heartbeats RPC from leader
func (rf *Raft) bgHeartbeatsSync() {
	if rf.appendEntriesTicker == nil {
		rf.appendEntriesTicker = time.NewTicker(
			time.Duration(defaultHeartbeatsInterval) * time.Millisecond)
	}
	for {
		//log.Printf("Peer #%d append entries trigger", rf.me)
		<-rf.appendEntriesTicker.C
		if rf.IsLeader() {
			args := rf.makeAppendEntriesRPCArgs()
			ls.Debugf("BG Peer #%d leader sending append entries %v.", rf.me, args)
			replyChan := make(chan rpcReply, len(rf.peers))
			rf.floodAppendEntries(args, replyChan)
		}
	}
}

//bgRPCCall is a background job to watch rpcCallChan and make a RPC
func (rf *Raft) bgRPCCall() {
	for {
		req := <-rf.rpcCallChan
		go doRPCCall(req)
	}
}

//resetElectionTimer resets election timeout.
//Don't need to consider thread safety?
func (rf *Raft) resetElectionTimer() {
	ls.Debugf("Peer #%d reset election timer", rf.me)
	rf.electionTimer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
}

//getElectionTimeout return a random election timeout number for this server
func (rf *Raft) getElectionTimeout() int {
	rand.Seed(int64(rf.me))
	interval := electionTimeoutUpper - electionTiemoutLower
	return rand.Intn(interval) + electionTiemoutLower
}

//makeAppendEntriesRPCArgs makes a AppendEntries RPC args
func (rf *Raft) makeAppendEntriesRPCArgs() AppendEntriesArgs {
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.lastApplied
	if len(rf.logs) > rf.lastApplied {
		args.PrevLogTerm = rf.logs[rf.lastApplied].Term
	}
	args.LeaderCommit = rf.commitIndex
	return args
}

//makeRequestVoteRPCArgs makes a RequestVote RPC args
func (rf *Raft) makeRequestVoteRPCArgs() RequestVoteArgs {
	args := RequestVoteArgs{}
	args.CandidateId = rf.me
	args.LastLogIndex = rf.lastApplied
	if len(rf.logs) > rf.lastApplied {
		args.LastLogTerm = rf.logs[rf.lastApplied].Term
	}
	args.Term = rf.currentTerm
	return args
}
