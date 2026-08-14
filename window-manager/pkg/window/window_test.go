package window

import "testing"

func TestStartStop(t *testing.T) {
	wm := New()
	if wm.Running() {
		t.Fatal("expected not running initially")
	}
	wm.Start()
	if !wm.Running() {
		t.Fatal("expected running after Start")
	}
	wm.Stop()
	if wm.Running() {
		t.Fatal("expected not running after Stop")
	}
}
