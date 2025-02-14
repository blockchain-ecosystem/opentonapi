package litestorage

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"time"

	"github.com/tonkeeper/tongo/abi"

	"github.com/tonkeeper/tongo/tlb"
	tongoWallet "github.com/tonkeeper/tongo/wallet"

	"github.com/tonkeeper/opentonapi/pkg/core"
	"github.com/tonkeeper/tongo"
)

func (s *LiteStorage) GetSubscriptions(ctx context.Context, address tongo.AccountID) ([]core.Subscription, error) {
	return []core.Subscription{}, nil
}

func (s *LiteStorage) GetSeqno(ctx context.Context, account tongo.AccountID) (uint32, error) {
	return s.client.GetSeqno(ctx, account)
}

func (s *LiteStorage) GetAccountState(ctx context.Context, a tongo.AccountID) (tlb.ShardAccount, error) {
	return s.client.GetAccountState(ctx, a)
}

func (s *LiteStorage) AccountStatusAndInterfaces(ctx context.Context, addr tongo.AccountID) (tlb.AccountStatus, []abi.ContractInterface, error) {
	if s == nil {
		return tlb.AccountNone, nil, fmt.Errorf("storage is nil")
	}

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	client, release := s.getClient()
	if client == nil {
		return tlb.AccountNone, nil, fmt.Errorf("failed to get lite client")
	}
	defer release()

	account, err := s.GetRawAccount(ctx, addr)
	if errors.Is(err, core.ErrEntityNotFound) {
		return tlb.AccountNone, nil, nil
	}
	if err != nil {
		return tlb.AccountNone, nil, fmt.Errorf("get raw account: %w", err)
	}
	if account == nil {
		return tlb.AccountNone, nil, nil
	}

	// Get interfaces from cache or compute them
	interfaces, _ := s.accountInterfacesCache.LoadOrCompute(addr, func() []abi.ContractInterface {
		if account.Code == nil {
			return nil
		}
		inspector := abi.NewContractInspector(abi.InspectWithLibraryResolver(s))
		cd, err := inspector.InspectContract(ctx, account.Code, s.executor, addr)
		if err != nil {
			return nil
		}
		return cd.ContractInterfaces
	})

	return account.Status, interfaces, nil
}

func (s *LiteStorage) SearchAccountsByPubKey(ctx context.Context, pubKey ed25519.PublicKey) ([]tongo.AccountID, error) {
	versions := []tongoWallet.Version{
		tongoWallet.V1R1, tongoWallet.V1R2, tongoWallet.V1R3,
		tongoWallet.V2R1, tongoWallet.V2R2,
		tongoWallet.V3R1, tongoWallet.V3R2,
		tongoWallet.V4R1, tongoWallet.V4R2,
		tongoWallet.V5Beta, tongoWallet.V5R1,
	}
	var walletAddresses []tongo.AccountID
	for _, version := range versions {
		walletAddress, err := tongoWallet.GenerateWalletAddress(pubKey, version, nil, 0, nil)
		if err != nil {
			continue
		}
		walletAddresses = append(walletAddresses, walletAddress)
		s.pubKeyByAccountID.Store(walletAddress, pubKey)
	}
	return walletAddresses, nil
}

func (s *LiteStorage) GetAccountDiff(ctx context.Context, account tongo.AccountID, startTime int64, endTime int64) (int64, error) {
	return 0, errors.New("not implemented")
}

func (s *LiteStorage) GetLatencyAndLastMasterchainSeqno(ctx context.Context) (int64, uint32, error) {
	blockHeader, err := s.LastMasterchainBlockHeader(ctx)
	if err != nil {
		return 0, 0, err
	}
	latency := time.Now().Unix() - int64(blockHeader.GenUtime)
	return latency, blockHeader.Seqno, nil
}
