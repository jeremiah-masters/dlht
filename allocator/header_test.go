package allocator

import "testing"

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
