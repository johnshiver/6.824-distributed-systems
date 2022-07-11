package raft

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestElectionVotingOdd(t *testing.T) {

	// start new election, started by server 0 with odd number
	testElection := NewElection(0, 0, 5)
	testElection.AddVote(1)
	testElection.AddVote(2)

	require.False(t, testElection.CandidateHasMajority())
	testElection.AddVote(3)
	require.True(t, testElection.CandidateHasMajority())

}

func TestElectionVotingEven(t *testing.T) {
	// start new election, started by server 0 with odd number
	testElection := NewElection(0, 0, 6)
	testElection.AddVote(1)
	testElection.AddVote(2)
	require.False(t, testElection.CandidateHasMajority())
	testElection.AddVote(3)
	require.False(t, testElection.CandidateHasMajority())
	testElection.AddVote(4)
	require.True(t, testElection.CandidateHasMajority())
}
