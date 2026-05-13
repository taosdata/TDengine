package thread

type Locker struct {
	c chan struct{}
}

var nothing = struct{}{}

func NewLocker(count int) *Locker {
	return &Locker{c: make(chan struct{}, count)}
}

func (l *Locker) Lock() {
	l.c <- nothing
}

func (l *Locker) Unlock() {
	<-l.c
}
