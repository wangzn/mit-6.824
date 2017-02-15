package raft

import "github.com/Sirupsen/logrus"

const (
	votedForNone = -1

	electionTimeoutUpper      = 500 //ms
	electionTiemoutLower      = 350 //ms
	defaultHeartbeatsInterval = 50  //ms
	defaultRPCChannelSize     = 1024
	defaultEventChannelSize   = 1024
	defaultLogLevel           = logrus.DebugLevel
)

const (
	stFollower = iota
	stCandidate
	stLeader
)

type RaftState int

type RaftEvent int

type RaftRPCMethod string

const (
	rpcMethodRequestVote   = "Raft.RequestVote"
	rpcMethodAppendEntries = "Raft.AppendEntries"
)

const (
	evElectionTimeout = iota
	evLeaderGranted
	evRecvAppendEntries
	evStateToFollower
)

func init() {
	logrus.SetLevel(defaultLogLevel)
}
