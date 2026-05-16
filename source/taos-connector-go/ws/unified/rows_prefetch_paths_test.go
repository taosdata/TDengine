package unified

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestResultSetNextRawBlockUsesPrefetchedResult verifies prefetched block path resets prefetch state.
func TestResultSetNextRawBlockUsesPrefetchedResult(t *testing.T) {
	ch := make(chan fetchRawBlockResult, 1)
	ch <- fetchRawBlockResult{
		block:     []byte{1, 2, 3},
		completed: false,
	}

	rs := &ResultSet{
		prefetching: true,
		prefetchCh:  ch,
	}

	block, completed, err := rs.nextRawBlock()
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3}, block)
	require.False(t, completed)
	require.False(t, rs.prefetching)
	require.Nil(t, rs.prefetchCh)
}

// TestResultSetFetchBlockPropagatesPrefetchError verifies fetchBlock returns prefetch error.
func TestResultSetFetchBlockPropagatesPrefetchError(t *testing.T) {
	wantErr := errors.New("prefetch failed")
	ch := make(chan fetchRawBlockResult, 1)
	ch <- fetchRawBlockResult{
		err: wantErr,
	}

	rs := &ResultSet{
		prefetching: true,
		prefetchCh:  ch,
	}

	err := rs.fetchBlock()
	require.ErrorIs(t, err, wantErr)
	require.False(t, rs.prefetching)
	require.Nil(t, rs.prefetchCh)
}

// TestResultSetWaitPrefetchLockedMarksCompleted verifies waitPrefetchLocked drains channel and marks completion.
func TestResultSetWaitPrefetchLockedMarksCompleted(t *testing.T) {
	ch := make(chan fetchRawBlockResult, 1)
	ch <- fetchRawBlockResult{
		completed: true,
	}

	rs := &ResultSet{
		prefetching: true,
		prefetchCh:  ch,
	}

	rs.waitPrefetchLocked()
	require.True(t, rs.completed)
	require.False(t, rs.prefetching)
	require.Nil(t, rs.prefetchCh)
}
