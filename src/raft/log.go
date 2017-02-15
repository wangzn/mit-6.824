package raft

//Log defines the log structure used in raft
type Log struct {
	Command interface{}
	Term    int
}
