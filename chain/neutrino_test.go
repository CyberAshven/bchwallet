package chain

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/gcash/bchd/chaincfg"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil"
	"github.com/gcash/bchwallet/waddrmgr"
	"github.com/gcash/neutrino"
	"github.com/gcash/neutrino/blockntfns"
)

// TestRescanFailed checks that a rescan that stopped, as one does when a new
// rescan replaces it, isn't taken for one that failed, which the client logged
// as an error after every rescan from the wallet's birthday.
func TestRescanFailed(t *testing.T) {
	if rescanFailed(nil) {
		t.Error("a rescan that ended without an error failed")
	}
	if rescanFailed(neutrino.ErrRescanExit) {
		t.Error("a rescan that stopped failed")
	}
	if rescanFailed(fmt.Errorf("replaced: %w", neutrino.ErrRescanExit)) {
		t.Error("a rescan that stopped, with the error wrapped, failed")
	}
	if !rescanFailed(errors.New("no filter for block")) {
		t.Error("a rescan that ended with another error didn't fail")
	}
}

// TestRetryFetch checks that a fetch that fails, as one of a filter that no
// peer sends, is tried again until it succeeds rather than given up on after a
// few tries, and that the retries end when neutrino shuts down, the client
// stops or the request is interrupted.
func TestRetryFetch(t *testing.T) {
	calls := 0
	err := retryFetch("filter", func() error {
		calls++
		if calls < 4 {
			return errors.New("no peer sent the filter")
		}
		return nil
	}, nil, nil, time.Millisecond, 2*time.Millisecond)
	if err != nil || calls != 4 {
		t.Fatalf("a fetch that failed three times gave %v after %d "+
			"calls, want success after 4", err, calls)
	}

	calls = 0
	err = retryFetch("filter", func() error {
		calls++
		return fmt.Errorf("query ended: %w", neutrino.ErrShuttingDown)
	}, nil, nil, time.Millisecond, time.Millisecond)
	if !errors.Is(err, neutrino.ErrShuttingDown) || calls != 1 {
		t.Fatalf("a fetch that failed as neutrino shut down gave %v "+
			"after %d calls, want its error after 1", err, calls)
	}

	quit := make(chan struct{})
	close(quit)
	err = retryFetch("filter", func() error {
		return errors.New("no peer sent the filter")
	}, nil, quit, time.Hour, time.Hour)
	if !errors.Is(err, errClientStopped) {
		t.Fatalf("a fetch retried after the client stopped gave %v, "+
			"want errClientStopped", err)
	}

	interrupt := make(chan struct{})
	close(interrupt)
	err = retryFetch("filter", func() error {
		return errors.New("no peer sent the filter")
	}, interrupt, nil, time.Hour, time.Hour)
	if !errors.Is(err, ErrFilterReqInterrupt) {
		t.Fatalf("a fetch retried after the request was interrupted "+
			"gave %v, want ErrFilterReqInterrupt", err)
	}
}

// TestForwardFilterRollbacks checks that the client tells the wallet of each
// rollback of the filter headers, and that it ends its subscription to them
// once it stops.
func TestForwardFilterRollbacks(t *testing.T) {
	s := &NeutrinoClient{enqueueNotification: make(chan interface{})}
	rollbacks := make(chan uint32, 1)
	cancelled := make(chan struct{})
	quit := make(chan struct{})
	s.wg.Add(1)
	go s.forwardFilterRollbacks(rollbacks, func() { close(cancelled) }, quit)

	rollbacks <- 500
	select {
	case n := <-s.enqueueNotification:
		if n != (FilterHeadersRolledBack{Height: 500}) {
			t.Fatalf("got notification %#v, want a rollback to block "+
				"500", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the rollback wasn't forwarded")
	}

	close(quit)
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("the subscription wasn't ended once the client stopped")
	}
	s.wg.Wait()
}

// fakeRescanSource is a chain source that refuses the first refusals block
// subscriptions, as the chain service refuses one while its block
// notifications haven't reached the height asked for, and whose best block is
// at best.
type fakeRescanSource struct {
	neutrino.ChainSource
	refusals int
	calls    int
	best     int32
}

func (f *fakeRescanSource) FilterType() wire.FilterType {
	return wire.GCSFilterScripts
}

func (f *fakeRescanSource) BestBlock() (*waddrmgr.BlockStamp, error) {
	return &waddrmgr.BlockStamp{Height: f.best}, nil
}

func (f *fakeRescanSource) Subscribe(bestHeight uint32) (*blockntfns.Subscription, error) {
	f.calls++
	if f.calls <= f.refusals {
		return nil, fmt.Errorf("request with height %d is greater than "+
			"best height known 0", bestHeight)
	}
	return &blockntfns.Subscription{}, nil
}

// TestRescanSubscribeWaits checks that a rescan's block subscription is asked
// for again while the chain service refuses it for a block whose filter header
// it has stored, as it does until its block notifications reach the block,
// rather than the refusal ending the rescan.  The refusal of a block the store
// doesn't hold is returned, and so is one that comes once the rescan or the
// client quits.
func TestRescanSubscribeWaits(t *testing.T) {
	source := &fakeRescanSource{refusals: 3, best: 110}
	sub, err := (&rescanChainSource{rescanSource: source}).Subscribe(110)
	if err != nil || sub == nil || source.calls != 4 {
		t.Fatalf("a subscription refused 3 times gave %v, %v after %d "+
			"calls, want it after 4", sub, err, source.calls)
	}

	source = &fakeRescanSource{refusals: 3, best: 109}
	if _, err := (&rescanChainSource{rescanSource: source}).Subscribe(110); err == nil ||
		source.calls != 1 {

		t.Fatalf("a subscription above the best block gave %v after %d "+
			"calls, want its refusal after 1", err, source.calls)
	}

	quit := make(chan struct{})
	close(quit)
	for _, c := range []*rescanChainSource{{quit: quit}, {stop: quit}} {
		source = &fakeRescanSource{refusals: 3, best: 110}
		c.rescanSource = source
		if _, err := c.Subscribe(110); err == nil || source.calls != 1 {
			t.Fatalf("a subscription after a quit gave %v after %d "+
				"calls, want its refusal after 1", err, source.calls)
		}
	}
}

// TestMempoolAskerMerges checks that the requests for the filter of the whole
// mempool that come before the gap after the last one ends are merged into one
// at its end, as a payment can have the wallet take up addresses in several
// calls, and that a request after a gap without any is asked for at once.
func TestMempoolAskerMerges(t *testing.T) {
	var asked [][]bchutil.Address
	m := &mempoolAsker{gap: time.Hour, ask: func(addrs []bchutil.Address) {
		asked = append(asked, addrs)
	}}
	addrs := make([]bchutil.Address, 4)
	for i := range addrs {
		addr, err := bchutil.NewAddressPubKeyHash(
			bytes.Repeat([]byte{byte(i)}, 20), &chaincfg.SimNetParams,
		)
		if err != nil {
			t.Fatal(err)
		}
		addrs[i] = addr
	}

	m.request(addrs[:1])
	m.request(addrs[1:2])
	m.request(addrs[2:3])
	if !reflect.DeepEqual(asked, [][]bchutil.Address{addrs[:1]}) {
		t.Fatalf("asked for %v before the gap ended, want only the "+
			"first request", asked)
	}

	// The end of the gap, as its timer has it.
	m.askPending()
	if !reflect.DeepEqual(asked, [][]bchutil.Address{addrs[:1], addrs[1:3]}) {
		t.Fatalf("asked for %v at the end of the gap, want the two "+
			"requests that waited together", asked)
	}

	// The end of the next gap, which had no request.
	m.askPending()
	m.request(addrs[3:])
	if !reflect.DeepEqual(asked, [][]bchutil.Address{addrs[:1], addrs[1:3], addrs[3:]}) {
		t.Fatalf("asked for %v, want a request after a gap without "+
			"any asked for at once", asked)
	}
}
