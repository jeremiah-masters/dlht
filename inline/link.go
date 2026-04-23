package inline

import (
	"sync/atomic"
)

func (idx *index[V]) allocateLink() uint32 {
	start := atomic.AddUint32(&idx.linkNext, 1)
	// Plain load is safe: FAA provides acquire barrier, and we can only see
	// monotonically increasing linkCapacity values due to cache coherence.
	// Stale reads may only cause premature NO_LINK (safe), never over-allocation.
	if start > idx.linkCapacity {
		return NO_LINK
	}
	return start
}

func (idx *index[V]) allocateLinkPair() uint32 {
	start := atomic.AddUint32(&idx.linkNext, 2) - 1
	if start+1 > idx.linkCapacity {
		return NO_LINK
	}
	return start
}

func (idx *index[V]) attachSingle(pb *PrimaryBucket[V]) uint32 {
	linkIdx := idx.allocateLink()
	if linkIdx == NO_LINK {
		return NO_LINK
	}
	for {
		cur := atomicLoadLinkMeta(&pb.LinkMeta)
		if cur.getSingle() != NO_LINK {
			return cur.getSingle()
		}
		new := cur.setSingle(linkIdx)
		if atomicCASLinkMeta(&pb.LinkMeta, cur, new) {
			return linkIdx
		}
	}
}

func (idx *index[V]) attachPair(pb *PrimaryBucket[V]) uint32 {
	ps := idx.allocateLinkPair()
	if ps == NO_LINK {
		return NO_LINK
	}
	for {
		cur := atomicLoadLinkMeta(&pb.LinkMeta)
		if cur.getPairStart() != NO_LINK {
			return cur.getPairStart()
		}
		new := cur.setPairStart(ps)
		if atomicCASLinkMeta(&pb.LinkMeta, cur, new) {
			return ps
		}
	}
}
