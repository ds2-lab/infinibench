package helpers

import (
	"bufio"
	"os"
	"strconv"
)

const MaxDoubleSize = 100000

type Checkpoint struct {
	file     *os.File
	executed []bool
	tracker  chan int64
	frontier int64
}

func NewCheckpoint(path string) *Checkpoint {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	checkpoint := &Checkpoint{
		file:     file,
		executed: make([]bool, 10000),
		tracker:  make(chan int64, 1000),
		frontier: 0,
	}
	checkpoint.Load()
	go checkpoint.Track()
	return checkpoint
}

func (c *Checkpoint) Load() {
	scanner := bufio.NewScanner(c.file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		index, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			continue
		}
		if index >= int64(cap(c.executed)) {
			newCap := cap(c.executed) * 2
			// Size larger than MaxDoubleSize will be expand by MaxDoubleSize.
			if cap(c.executed) > MaxDoubleSize {
				newCap = cap(c.executed) + MaxDoubleSize
			}
			newExecuted := make([]bool, newCap)
			copy(newExecuted[:len(c.executed)], c.executed)
			c.executed = newExecuted
		}
		c.executed[index] = true
		if index > c.frontier {
			c.frontier = index
		}
	}
}

func (c *Checkpoint) Track() {
	for index := range c.tracker {
		if index < 0 {
			return
		}
		c.file.WriteString(strconv.FormatInt(index, 10))
		c.file.WriteString("\n")
		if index > c.frontier {
			c.frontier = index
		}
	}
}

func (c *Checkpoint) CheckSync(index int64) {
	c.tracker <- index
}

// Seen checks if the index is seen.
func (c *Checkpoint) Seen(index int64) bool {
	if index < int64(len(c.executed)) {
		return c.executed[index]
	}
	return false
}

// Frontier returns the max index of checked indexes.
func (c *Checkpoint) Frontier() int64 {
	return c.frontier
}

func (c *Checkpoint) Close() {
	c.tracker <- -1
	c.file.Close()
	c.file = nil
}
