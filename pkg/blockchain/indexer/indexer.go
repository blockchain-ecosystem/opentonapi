package indexer

import (
	"context"
	"encoding/hex"
	"sort"
	"strings"
	"time"

	"github.com/sourcegraph/conc/iter"
	"github.com/tonkeeper/tongo"
	"github.com/tonkeeper/tongo/liteapi"
	"github.com/tonkeeper/tongo/tlb"
	"go.uber.org/zap"
)

type chunk struct {
	masterID tongo.BlockID
	ids      map[tongo.BlockIDExt]struct{}
	blocks   []IDandBlock
}

// Indexer tracks the blockchain and notifies subscribers about new blocks.
type Indexer struct {
	logger *zap.Logger
	cli    *liteapi.Client
}

func New(logger *zap.Logger, cli *liteapi.Client) *Indexer {
	return &Indexer{
		cli:    cli,
		logger: logger,
	}
}

type IDandBlock struct {
	ID    tongo.BlockIDExt
	Block *tlb.Block
}

func (idx *Indexer) Run(ctx context.Context, channels []chan IDandBlock) {
	// Wait for initial sync
	for {
		select {
		case <-ctx.Done():
			return
		default:
			info, err := idx.cli.GetMasterchainInfo(ctx)
			if err != nil {
				idx.logger.Error("failed to get masterchain info", zap.Error(err))
				time.Sleep(time.Second * 5)
				continue
			}

			idx.logger.Debug("masterchain info",
				zap.Uint32("last_seqno", info.Last.Seqno),
				zap.String("last_root_hash", hex.EncodeToString(info.Last.RootHash[:])),
				zap.String("last_file_hash", hex.EncodeToString(info.Last.FileHash[:])))

			// Get current masterchain state
			_, err = idx.cli.GetMasterchainInfoExt(ctx, 0)
			if err != nil {
				idx.logger.Error("failed to get masterchain info ext", zap.Error(err))
				time.Sleep(time.Second * 5)
				continue
			}

			// Check if block is ready and applied
			_, err = idx.cli.GetBlockHeader(ctx, tongo.BlockIDExt{
				BlockID: tongo.BlockID{
					Workchain: -1,
					Shard:     uint64(tongo.MustParseShardID(-0x8000000000000000).Encode()),
					Seqno:     info.Last.Seqno,
				},
				RootHash: tongo.Bits256(info.Last.RootHash),
				FileHash: tongo.Bits256(info.Last.FileHash),
			}, 1)
			if err != nil {
				if isBlockNotReadyError(err) || isBlockNotResolved(err) {
					idx.logger.Warn("waiting for block to be applied",
						zap.Uint32("seqno", info.Last.Seqno))
					time.Sleep(time.Second * 5)
					continue
				}
				idx.logger.Error("failed to get block header", zap.Error(err))
				time.Sleep(time.Second * 5)
				continue
			}

			idx.logger.Info("lite server synced",
				zap.Uint32("seqno", info.Last.Seqno))
			break
		}
	}

	// Process blocks with backoff
	chunk := &chunk{}
	backoff := time.Second
	maxBackoff := time.Minute * 2

	for {
		select {
		case <-ctx.Done():
			return
		default:
			next, err := idx.next(chunk)
			if err != nil {
				if isBlockNotReadyError(err) || isBlockNotResolved(err) {
					time.Sleep(backoff)
					backoff = min(backoff*2, maxBackoff)
					idx.logger.Warn("block not ready, waiting",
						zap.Duration("backoff", backoff),
						zap.Error(err))
					continue
				}
				idx.logger.Error("failed to get next chunk", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}

			// Reset backoff on success
			backoff = time.Second
			chunk = next

			// Process blocks with timeout
			for _, block := range next.blocks {
				for _, ch := range channels {
					select {
					case ch <- block:
					case <-ctx.Done():
						return
					case <-time.After(5 * time.Second):
						idx.logger.Warn("channel full, skipping block",
							zap.String("block_id", block.ID.String()))
						continue
					}
				}
			}
		}
	}
}

func (idx *Indexer) processBlocks(ctx context.Context, chunk *chunk, channels []chan IDandBlock) {
	blockTicker := time.NewTicker(500 * time.Millisecond)
	defer blockTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-blockTicker.C:
			next, err := idx.next(chunk)
			if err != nil {
				if isBlockNotReadyError(err) {
					continue
				}
				idx.logger.Error("failed to get next chunk", zap.Error(err))
				continue
			}

			for _, block := range next.blocks {
				for _, ch := range channels {
					select {
					case ch <- block:
					case <-ctx.Done():
						return
					case <-time.After(5 * time.Second):
						// Add timeout instead of skipping immediately
						idx.logger.Warn("channel full, timeout reached")
						continue
					}
				}
			}
			chunk = next
		}
	}
}

func (idx *Indexer) next(prevChunk *chunk) (*chunk, error) {
	nextMasterID := prevChunk.masterID
	nextMasterID.Seqno += 1
	masterBlockID, _, err := idx.cli.LookupBlock(context.Background(), nextMasterID, 1, nil, nil)
	if err != nil {
		return nil, err
	}
	masterBlock, err := idx.cli.GetBlock(context.Background(), masterBlockID)
	if err != nil {
		return nil, err
	}
	shards := tongo.ShardIDs(&masterBlock)
	currentChunk := chunk{
		masterID: nextMasterID,
		ids:      make(map[tongo.BlockIDExt]struct{}, len(shards)+1),
	}
	for _, shardID := range shards {
		currentChunk.ids[shardID] = struct{}{}
	}

	var chunkBlocks []IDandBlock
	// queue contains IDs to resolve
	// and we are going to resolve each ID to a corresponding block
	queue := shards
	for {
		if len(queue) == 0 {
			break
		}
		blocks, err := iter.MapErr[tongo.BlockIDExt, *tlb.Block](queue, func(t *tongo.BlockIDExt) (*tlb.Block, error) {
			if _, ok := prevChunk.ids[*t]; ok {
				return nil, nil
			}
			block, err := idx.cli.GetBlock(context.Background(), *t)
			if err != nil {
				if strings.Contains(err.Error(), "not in db") {
					return nil, nil
				}
				return nil, err
			}
			return &block, nil
		})
		if err != nil {
			return nil, err
		}
		var newQueue []tongo.BlockIDExt
		for i, block := range blocks {
			if block == nil {
				continue
			}
			chunkBlocks = append(chunkBlocks, IDandBlock{ID: queue[i], Block: block})
			parents, err := tongo.GetParents(block.Info)
			if err != nil {
				return nil, err
			}
			for _, parent := range parents {
				if _, ok := prevChunk.ids[parent]; ok {
					continue
				}
				if _, ok := currentChunk.ids[parent]; ok {
					continue
				}
				// we need to get block of this parent and its parents
				newQueue = append(newQueue, parent)
			}
		}
		queue = newQueue
	}
	for i := len(chunkBlocks)/2 - 1; i >= 0; i-- {
		opp := len(chunkBlocks) - 1 - i
		chunkBlocks[i], chunkBlocks[opp] = chunkBlocks[opp], chunkBlocks[i]
	}
	chunkBlocks = append(chunkBlocks, IDandBlock{ID: masterBlockID, Block: &masterBlock})
	sort.Slice(chunkBlocks, func(i, j int) bool {
		return chunkBlocks[i].Block.Info.StartLt < chunkBlocks[j].Block.Info.StartLt
	})
	currentChunk.blocks = chunkBlocks
	return &currentChunk, nil
}

func (idx *Indexer) initChunk(seqno uint32) (*chunk, error) {
	init := tongo.BlockID{
		Workchain: -1,
		Shard:     9223372036854775808,
		Seqno:     seqno - 1,
	}
	id, _, err := idx.cli.LookupBlock(context.Background(), init, 1, nil, nil)
	if err != nil {
		return nil, err
	}
	block, err := idx.cli.GetBlock(context.Background(), id)
	if err != nil {
		return nil, err
	}
	ch := &chunk{
		masterID: init,
		ids: map[tongo.BlockIDExt]struct{}{
			id: {},
		},
		blocks: []IDandBlock{
			{ID: id, Block: &block},
		},
	}
	for _, shard := range tongo.ShardIDs(&block) {
		ch.ids[shard] = struct{}{}
	}
	return ch, nil
}

func isBlockNotResolved(err error) bool {
	return strings.Contains(err.Error(), "failed to resolve block")
}

func isBlockNotReadyError(err error) bool {
	return strings.Contains(err.Error(), "ltdb: block not found") ||
		strings.Contains(err.Error(), "block is not applied") ||
		strings.Contains(err.Error(), "is not in db") ||
		strings.Contains(err.Error(), "is not applied")
}
