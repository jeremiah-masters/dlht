package inline

import (
	"sync/atomic"
)

const (
	pairStartShift = 32
	NO_LINK        = 0
)

func newLinkMeta(pairStart, single uint32) LinkMeta {
	return LinkMeta((uint64(pairStart) << pairStartShift) | uint64(single))
}

func (lm LinkMeta) getPairStart() uint32 { return uint32(uint64(lm) >> pairStartShift) }

func (lm LinkMeta) getSingle() uint32 { return uint32(uint64(lm) & 0xFFFFFFFF) }

func (lm LinkMeta) setPairStart(ps uint32) LinkMeta { return newLinkMeta(ps, lm.getSingle()) }

func (lm LinkMeta) setSingle(s uint32) LinkMeta { return newLinkMeta(lm.getPairStart(), s) }

func (lm LinkMeta) resolveLink(linkIdx int) uint32 {
	switch linkIdx {
	case 0:
		return lm.getSingle()
	case 1:
		return lm.getPairStart()
	case 2:
		ps := lm.getPairStart()
		if ps == NO_LINK {
			return NO_LINK
		}
		return ps + 1
	default:
		return NO_LINK
	}
}

func (lm LinkMeta) getAttachedLinkCount() int {
	count := 0
	if lm.getSingle() != NO_LINK {
		count++
	}
	if lm.getPairStart() != NO_LINK {
		count += 2
	}
	return count
}

func atomicLoadLinkMeta(addr *LinkMeta) LinkMeta { return LinkMeta(atomic.LoadUint64((*uint64)(addr))) }

func atomicCASLinkMeta(addr *LinkMeta, old, neu LinkMeta) bool {
	return atomic.CompareAndSwapUint64((*uint64)(addr), uint64(old), uint64(neu))
}
