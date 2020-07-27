// Copyright (c) of parts are held by the various contributors (see the CLA)
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

package mining

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"time"

	lxr "github.com/pegnet/LXRHash"
	log "github.com/sirupsen/logrus"
	"go.uber.org/ratelimit"
)

const (
	_ = iota
	BatchCommand
	CurrentHashRate
	NewNoncePrefix
	NewOPRHash
	ResetRecords
	MinimumAccept
	SubmitStats
	PauseMining
	ResumeMining
)

type MinerCommand struct {
	Command int
	Data    interface{}
}

type Winner struct {
	OPRHash string
	Nonce   string
	Target  string
}

// PegnetMiner mines an OPRhash
type PegnetMiner struct {
	// ID is the miner number, starting with "1".
	ID         uint32 // The process id to the pool
	PersonalID uint32 // The miner thread id

	// Miner commands
	commands chan *MinerCommand

	successes chan *Winner

	// All the state variables PER oprhash.
	MiningState oprMiningState

	// Tells us we are paused
	paused bool

	// Fake Mining for testing
	ratelimit.Limiter

	// Used to compute difficulties
	ComputeDifficulty func(oprhash, nonce []byte) (difficulty uint64)

	// A handle for the LXR hash
	sharedLXR *lxr.LXRHash
}

type oprMiningState struct {
	// Used to compute new hashes
	oprhash []byte
	static  []byte

	// Used to track noncing
	*NonceIncrementer
	start uint32 // For batch mining

	stats *SingleMinerStats

	// Used to return hashes
	minimumDifficulty uint64
}

// NonceIncrementer is just simple to increment nonces
type NonceIncrementer struct {
	Nonce          []byte
	lastNonceByte  int
	lastPrefixByte int
}

func NewNonceIncrementer(id uint32, personalid uint32) *NonceIncrementer {
	n := new(NonceIncrementer)

	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, id)
	buf = append(buf, byte(personalid))

	n.lastPrefixByte = len(buf) - 1
	n.Nonce = append(buf, 0)
	n.lastNonceByte = 1
	return n
}

func (i *NonceIncrementer) Prefix() []byte {
	return i.Nonce[:i.lastPrefixByte+1]
}

// NextNonce is just counting to get the next nonce. We preserve
// the first byte, as that is our ID and give us our nonce space
//	So []byte(ID, 255) -> []byte(ID, 1, 0) -> []byte(ID, 1, 1)
func (i *NonceIncrementer) NextNonce() {
	idx := len(i.Nonce) - i.lastNonceByte
	for {
		i.Nonce[idx]++
		if i.Nonce[idx] == 0 {
			idx--
			if idx == i.lastPrefixByte { // This is my prefix, don't touch it!
				rest := append([]byte{1}, i.Nonce[i.lastPrefixByte+1:]...)
				i.Nonce = append(i.Nonce[:i.lastPrefixByte+1], rest...)
				break
			}
		} else {
			break
		}
	}
}

func (p *PegnetMiner) ResetNonce() {
	p.MiningState.NonceIncrementer = NewNonceIncrementer(p.ID, p.PersonalID)
	p.MiningState.start = 0
	p.resetStatic()
}

func NewPegnetMiner(id uint32, commands chan *MinerCommand, successes chan *Winner, sharedLXR *lxr.LXRHash) *PegnetMiner {
	p := new(PegnetMiner)
	p.ID = id
	p.PersonalID = id
	p.commands = commands
	p.successes = successes

	// Some inits
	p.MiningState.NonceIncrementer = NewNonceIncrementer(p.ID, p.PersonalID)
	p.ResetNonce()
	p.MiningState.stats = NewSingleMinerStats(p.PersonalID)

	p.ComputeDifficulty = wrapComputeDifficulty(sharedLXR)
	p.sharedLXR = sharedLXR

	return p
}

func (p *PegnetMiner) Close() {
	p.paused = true
	// Throw away the references to sharedLXR
	p.ComputeDifficulty = nil
	p.sharedLXR = nil
}

// SetFakeHashRate sets the miner to "fake" a hashrate. All targets are invalid
// The rate is in hashes/s
func (p *PegnetMiner) SetFakeHashRate(rate int) {
	// For fake mining
	p.Limiter = ratelimit.New(rate)
	p.ComputeDifficulty = p.FakeComputeDifficulty
}

func (p *PegnetMiner) IsPaused() bool {
	return p.paused
}

func (p *PegnetMiner) resetStatic() {
	p.MiningState.static = make([]byte, len(p.MiningState.oprhash)+len(p.MiningState.Prefix()))
	i := copy(p.MiningState.static, p.MiningState.oprhash)
	copy(p.MiningState.static[i:], p.MiningState.Prefix())
}

func (p *PegnetMiner) MineBatch(ctx context.Context, batchsize int) {
	limit := uint32(math.MaxUint32) - uint32(batchsize)
	mineLog := log.WithFields(log.Fields{"pid": p.PersonalID})
	select {
	// Wait for the first command to start
	// We start 'paused'. Any command will knock us out of this init phase
	case c := <-p.commands:
		p.HandleCommand(c)
	case <-ctx.Done():
		mineLog.Debugf("Mining init cancelled for miner %d\n", p.ID)
		return // Cancelled
	}

	for {
		select {
		case <-ctx.Done():
			mineLog.Debugf("Mining cancelled for miner: %d\n", p.ID)
			return // Mining cancelled
		case c := <-p.commands:
			p.HandleCommand(c)
		default:
		}

		if len(p.MiningState.oprhash) == 0 {
			p.paused = true
		}

		if p.paused {
			// Waiting on a resume command
			p.waitForResume(ctx)
			continue
		}

		batch := make([][]byte, batchsize)

		for i := range batch {
			batch[i] = make([]byte, 4)
			binary.BigEndian.PutUint32(batch[i], p.MiningState.start+uint32(i))
		}
		p.MiningState.start += uint32(batchsize)
		if p.MiningState.start > limit {
			mineLog.Warnf("repeating nonces, hit the cycle's limit")
		}

		var results [][]byte
		results = p.sharedLXR.HashParallel(p.MiningState.static, batch)
		for i := range results {
			// do something with the result here
			// nonce = batch[i]
			// input = append(base, batch[i]...)
			// hash = results[i]
			h := results[i]

			diff := ComputeHashDifficulty(h)
			p.MiningState.stats.NewDifficulty(diff)
			p.MiningState.stats.TotalHashes++

			if diff > p.MiningState.minimumDifficulty {
				success := &Winner{
					OPRHash: hex.EncodeToString(p.MiningState.oprhash),
					Nonce:   hex.EncodeToString(append(p.MiningState.static[32:], batch[i]...)),
					Target:  fmt.Sprintf("%x", diff),
				}
				p.MiningState.stats.TotalSubmissions++
				select {
				case p.successes <- success:
					mineLog.WithFields(log.Fields{
						"nonce":        batch[i],
						"id":           p.ID,
						"staticPrefix": p.MiningState.static[32:],
						"target":       success.Target,
					}).Trace("Submitted share")
				default:
					mineLog.WithField("channel", fmt.Sprintf("%p", p.successes)).Errorf("failed to submit, %d/%d", len(p.successes), cap(p.successes))
				}
			}
		}
	}
}

func ComputeHashDifficulty(b []byte) (difficulty uint64) {
	// The high eight bytes of the hash(hash(entry.Content) + nonce) is the difficulty.
	// Because we don't have a difficulty bar, we can define difficulty as the greatest
	// value, rather than the minimum value.  Our bar is the greatest difficulty found
	// within a 10 minute period.  We compute difficulty as Big Endian.
	return uint64(b[7]) | uint64(b[6])<<8 | uint64(b[5])<<16 | uint64(b[4])<<24 |
		uint64(b[3])<<32 | uint64(b[2])<<40 | uint64(b[1])<<48 | uint64(b[0])<<56
}

func (p *PegnetMiner) Mine(ctx context.Context) {
	mineLog := log.WithFields(log.Fields{"pid": p.PersonalID})
	select {
	// Wait for the first command to start
	// We start 'paused'. Any command will knock us out of this init phase
	case c := <-p.commands:
		p.HandleCommand(c)
	case <-ctx.Done():
		mineLog.Debugf("Mining init cancelled for miner %d\n", p.ID)
		return // Cancelled
	}

	for {
		select {
		case <-ctx.Done():
			mineLog.Debugf("Mining cancelled for miner: %d\n", p.ID)
			return // Mining cancelled
		case c := <-p.commands:
			p.HandleCommand(c)
		default:
		}

		if len(p.MiningState.oprhash) == 0 {
			p.paused = true
		}

		if p.paused {
			// Waiting on a resume command
			p.waitForResume(ctx)
			continue
		}

		p.MiningState.NextNonce()
		diff := p.ComputeDifficulty(p.MiningState.oprhash, p.MiningState.Nonce)

		p.MiningState.stats.TotalHashes++
		p.MiningState.stats.NewDifficulty(diff)
		if diff > p.MiningState.minimumDifficulty {
			success := &Winner{
				OPRHash: hex.EncodeToString(p.MiningState.oprhash),
				Nonce:   hex.EncodeToString(p.MiningState.Nonce),
				Target:  fmt.Sprintf("%x", diff),
			}
			p.MiningState.stats.TotalSubmissions++
			select {
			case p.successes <- success:
			default:
				mineLog.WithField("channel", fmt.Sprintf("%p", p.successes)).Errorf("failed to submit, %d/%d", len(p.successes), cap(p.successes))
			}
		}
	}
}

func (p *PegnetMiner) FakeComputeDifficulty(_, _ []byte) uint64 {
	p.Limiter.Take()
	return rand.Uint64()
}

func (p *PegnetMiner) SendCommand(mc *MinerCommand) {
	p.commands <- mc
}

func (p *PegnetMiner) HandleCommand(c *MinerCommand) {
	switch c.Command {
	case BatchCommand:
		commands := c.Data.([]*MinerCommand)
		for _, c := range commands {
			p.HandleCommand(c)
		}
	case CurrentHashRate:
		currentStats := *p.MiningState.stats
		currentStats.Stop = time.Now()
		w := c.Data.(chan *SingleMinerStats)
		select {
		case w <- &currentStats:
		default:
		}
	case NewNoncePrefix:
		p.ID = c.Data.(uint32)
		p.ResetNonce()
	case NewOPRHash:
		p.MiningState.oprhash = c.Data.([]byte)
		p.resetStatic()
	case ResetRecords:
		p.ResetNonce()
		p.MiningState.stats = NewSingleMinerStats(p.PersonalID)
		p.MiningState.stats.Start = time.Now()
	case SubmitStats:
		p.MiningState.stats.Stop = time.Now()
		w := c.Data.(chan *SingleMinerStats)
		select {
		case w <- p.MiningState.stats:
		default:
		}
	case MinimumAccept:
		p.MiningState.minimumDifficulty = c.Data.(uint64)
	case PauseMining:
		p.paused = true
	case ResumeMining:
		p.paused = false
	}
}

func (p *PegnetMiner) waitForResume(ctx context.Context) {
	log.WithField("pid", p.PersonalID).Debugf("waiting to be resumed")
	// Pause until we get a new start or are cancelled
	for {
		select {
		case <-ctx.Done(): // Mining cancelled
			return
		case c := <-p.commands:
			p.HandleCommand(c)
			if !p.paused {
				log.WithField("pid", p.PersonalID).Debug("resumed")
				return
			}
		}
	}
}

func wrapComputeDifficulty(sharedLXR *lxr.LXRHash) func([]byte, []byte) uint64 {
	if sharedLXR == nil {
		log.Fatal("Cannot calculate difficulty with nil LXRHash")
	}
	return func(oprhash, nonce []byte) (difficulty uint64) {
		no := make([]byte, len(oprhash)+len(nonce))
		i := copy(no, oprhash)
		copy(no[i:], nonce)
		b := sharedLXR.Hash(no)

		// The high eight bytes of the hash(hash(entry.Content) + nonce) is the difficulty.
		// Because we don't have a difficulty bar, we can define difficulty as the greatest
		// value, rather than the minimum value.  Our bar is the greatest difficulty found
		// within a 10 minute period.  We compute difficulty as Big Endian.
		return uint64(b[7]) | uint64(b[6])<<8 | uint64(b[5])<<16 | uint64(b[4])<<24 |
			uint64(b[3])<<32 | uint64(b[2])<<40 | uint64(b[1])<<48 | uint64(b[0])<<56
	}
}

// CommandBuilder just let's me use building syntax to build commands
type CommandBuilder struct {
	command  *MinerCommand
	commands []*MinerCommand
}

func BuildCommand() *CommandBuilder {
	c := new(CommandBuilder)
	c.command = new(MinerCommand)
	c.command.Command = BatchCommand
	return c
}

func (b *CommandBuilder) CurrentHashRate(w chan *SingleMinerStats) *CommandBuilder {
	b.commands = append(b.commands, &MinerCommand{Command: CurrentHashRate, Data: w})
	return b
}

func (b *CommandBuilder) SubmitStats(w chan *SingleMinerStats) *CommandBuilder {
	b.commands = append(b.commands, &MinerCommand{Command: SubmitStats, Data: w})
	return b
}
func (b *CommandBuilder) NewOPRHash(oprhash []byte) *CommandBuilder {
	b.commands = append(b.commands, &MinerCommand{Command: NewOPRHash, Data: oprhash})
	return b
}

func (b *CommandBuilder) NewNoncePrefix(prefix uint32) *CommandBuilder {
	b.commands = append(b.commands, &MinerCommand{Command: NewNoncePrefix, Data: prefix})
	return b
}

func (b *CommandBuilder) ResetRecords() *CommandBuilder {
	b.commands = append(b.commands, &MinerCommand{Command: ResetRecords, Data: nil})
	return b
}

func (b *CommandBuilder) MinimumDifficulty(min uint64) *CommandBuilder {
	b.commands = append(b.commands, &MinerCommand{Command: MinimumAccept, Data: min})
	return b
}

func (b *CommandBuilder) PauseMining() *CommandBuilder {
	b.commands = append(b.commands, &MinerCommand{Command: PauseMining, Data: nil})
	return b
}

func (b *CommandBuilder) ResumeMining() *CommandBuilder {
	b.commands = append(b.commands, &MinerCommand{Command: ResumeMining, Data: nil})
	return b
}

func (b *CommandBuilder) Build() *MinerCommand {
	b.command.Data = b.commands
	return b.command
}
