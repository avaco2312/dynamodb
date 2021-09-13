package main

import (
	"fmt"
	"dynamodb/locks/locke"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testLock struct {
	operation string
	alock     int
	duration  time.Duration
	wait      time.Duration
	esperado  bool
}

var test1 []testLock = []testLock{
	{
		operation: "Lock",
		duration:  3 * time.Second,
		wait:      0,
		esperado:  true,
	},
	// 0 lock 0 tlock 3
	{
		operation: "Lock",
		duration:  5 * time.Second,
		wait:      1,
		esperado:  false,
	},
	// 1 lock 0 tlock 3
	{
		operation: "Lock",
		duration:  3 * time.Second,
		wait:      4,
		esperado:  true,
	},
	// 2 lock 2 tlock 7
	{
		operation: "Unlock",
		alock:     2,
		wait:      5,
		esperado:  true,
	},
	// 3 lock - tlock 5
	{
		operation: "Lock",
		duration:  4 * time.Second,
		wait:      7,
		esperado:  true,
	},
	// 4 lock 4 tlock 10
	{
		operation: "Unlock",
		alock:     0,
		wait:      9,
		esperado:  false,
	},
	// 5 lock - tlock 5
}

func TestInitial(t *testing.T) {
	var locks []locke.Lock
	var ch = make(chan struct{}, len(test1))
	for i, test := range test1 {
		go func(i int, l testLock) {
			time.Sleep(l.wait * time.Second)
			var res error
			switch l.operation {
			case "Lock":
				locku, _ := locke.NewLock("dynamo", svc, "Usuarios", "Pepe", fmt.Sprintf("Lock%d",i), l.duration)
				locks = append(locks, locku)
				res = locku.Acquire()
			case "Unlock":
				res = locks[l.alock].Release()
			}
			assert.Equal(t, l.esperado, res == nil, fmt.Sprintf("Indice: %d Funcion: %s", i, l.operation))
			ch <- struct{}{}
		}(i, test)
	}
	for i := 0; i < len(test1); i++ {
		<-ch
	}
}
