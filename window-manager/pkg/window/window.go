package window

import "sync"

type WindowManager struct {
	mu      sync.Mutex
	running bool
}

func New() *WindowManager {
	return &WindowManager{}
}

func (w *WindowManager) Start() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.running = true
}

func (w *WindowManager) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.running = false
}

func (w *WindowManager) Running() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.running
}
