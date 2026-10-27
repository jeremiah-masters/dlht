package allocator

import (
	"math/rand/v2"
	"testing"
)

func TestHeaderSlotStates(t *testing.T) {
	for i := range MAX_SLOTS_PER_BIN {
		for state := uint8(0); state <= SlotValid; state++ {
			// Set every surrounding bit, including version and bin state.
			h := Header(^uint64(0)-(1<<versionShift)).setSlotStateAndVersion(i, state)
			if got := h.getSlotState(i); got != state {
				t.Fatalf("slot %d: got %d, want %d", i, got, state)
			}
		}
	}
}

func TestHeaderInvalidMasks(t *testing.T) {
	for states := uint32(0); states < 256; states++ {
		var want uint32
		for i := range LINK_SLOTS {
			if (states>>(2*i))&3 == SlotInvalid {
				want |= 1 << (2 * i)
			}
		}
		// Vary surrounding bits to check that they cannot affect the masks.
		for _, fill := range []uint64{0, ^uint64(0)} {
			h := Header((fill &^ 0x3f) | uint64(states&0x3f))
			if got := h.invalidMask3(); got != want&0x15 {
				t.Fatalf("invalidMask3(%#x): got %#x, want %#x", h, got, want&0x15)
			}
			for _, base := range []uint{PRIMARY_SLOTS, PRIMARY_SLOTS + LINK_SLOTS, PRIMARY_SLOTS + 2*LINK_SLOTS} {
				shift := 2 * base
				h := Header((fill &^ (0xff << shift)) | (uint64(states) << shift))
				if got := h.invalidMask4(base); got != want {
					t.Fatalf("invalidMask4(%#x, %d): got %#x, want %#x", h, base, got, want)
				}
			}
		}
	}
}

func TestHeaderBinStateMask(t *testing.T) {
	for state := uint8(0); state < 4; state++ {
		for bit := range 32 {
			version := Header(1) << (versionShift + bit)
			for _, slots := range []Header{0, 1<<binStateShift - 1} {
				h := version | Header(state)<<binStateShift | slots
				if got := h.getBinState(); got != state {
					t.Fatalf("getBinState(%#x): got %d, want %d", h, got, state)
				}
			}
		}
	}
}

// Valid (0b11) and Shadow (0b10) share their high bit: every mask must match
// exactly, and the bin-state bits must never leak into the slot masks.
func TestHeaderMasks_ShadowExactness(t *testing.T) {
	var h Header
	h = h.setSlotStateAndVersion(0, SlotValid)
	h = h.setSlotStateAndVersion(1, SlotShadow)
	h = h.setSlotStateAndVersion(2, SlotTrying)
	h = h.setSlotStateAndVersion(3, SlotShadow)  // link slot (single, first)
	h = h.setSlotStateAndVersion(7, SlotValid)   // link slot (pair0, first)
	h = h.setSlotStateAndVersion(14, SlotShadow) // last slot
	h = h.setBinState(BinInTransfer)             // must not contaminate any mask

	if got := h.getSlotState(1); got != SlotShadow {
		t.Errorf("getSlotState(1): got %d, want SlotShadow", got)
	}
	if got, want := h.validMask3(), uint32(0x01); got != want {
		t.Errorf("validMask3: got %#x, want %#x (Shadow must not read as Valid)", got, want)
	}
	if got, want := h.shadowMask3(), uint32(0x04); got != want {
		t.Errorf("shadowMask3: got %#x, want %#x", got, want)
	}
	if got, want := h.invalidMask3(), uint32(0x00); got != want {
		t.Errorf("invalidMask3: got %#x, want %#x", got, want)
	}
	if got, want := h.validMask4(PRIMARY_SLOTS), uint32(0x00); got != want {
		t.Errorf("validMask4(3): got %#x, want %#x", got, want)
	}
	if got, want := h.shadowMask4(PRIMARY_SLOTS), uint32(0x01); got != want {
		t.Errorf("shadowMask4(3): got %#x, want %#x", got, want)
	}
	if got, want := h.validMask4(PRIMARY_SLOTS+LINK_SLOTS), uint32(0x01); got != want {
		t.Errorf("validMask4(7): got %#x, want %#x", got, want)
	}
	// Spread positions for slots 1, 3, 14: bits 2, 6, 28.
	if got, want := h.shadowMaskAll(), uint32(1<<2|1<<6|1<<28); got != want {
		t.Errorf("shadowMaskAll: got %#x, want %#x", got, want)
	}

	// A Valid slot 14 next to Done bin-state bits: neither mask may see
	// phantom bits from the header's [31:30] region.
	var h2 Header
	h2 = h2.setSlotStateAndVersion(14, SlotValid)
	h2 = h2.setBinState(BinDoneTransfer)
	if got, want := h2.shadowMaskAll(), uint32(0); got != want {
		t.Errorf("shadowMaskAll with Valid slot 14 + Done: got %#x, want %#x", got, want)
	}
	if got, want := h2.validMask4(PRIMARY_SLOTS+2*LINK_SLOTS), uint32(0x40); got != want {
		t.Errorf("validMask4(11) for slot 14: got %#x, want %#x", got, want)
	}
	var h3 Header
	h3 = h3.setSlotStateAndVersion(14, SlotShadow)
	h3 = h3.setBinState(BinInTransfer)
	if got, want := h3.shadowMaskAll(), uint32(1<<28); got != want {
		t.Errorf("shadowMaskAll slot14 Shadow + InTransfer: got %#x, want %#x", got, want)
	}
	if got, want := h3.validMask4(PRIMARY_SLOTS+2*LINK_SLOTS), uint32(0x00); got != want {
		t.Errorf("validMask4(11) slot14 Shadow: got %#x, want %#x", got, want)
	}
}

// Range slices validMaskAll per bucket, so it must agree with the bucket masks.
func TestHeaderValidMaskAll(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for range 100_000 {
		h := Header(rng.Uint64()) // any slot states, bin state and version
		want := h.validMask3() |
			h.validMask4(PRIMARY_SLOTS)<<(2*PRIMARY_SLOTS) |
			h.validMask4(PRIMARY_SLOTS+LINK_SLOTS)<<(2*(PRIMARY_SLOTS+LINK_SLOTS)) |
			h.validMask4(PRIMARY_SLOTS+2*LINK_SLOTS)<<(2*(PRIMARY_SLOTS+2*LINK_SLOTS))
		if got := h.validMaskAll(); got != want {
			t.Fatalf("validMaskAll(%#x): got %#x, want %#x", h, got, want)
		}
	}
}
