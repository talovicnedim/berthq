package berthq

// Queued reports how many messages were queued so far; tests use it to queue in a known order.
func (p *Publisher) Queued() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.seq
}
