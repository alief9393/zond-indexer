package token

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"zond-indexer/internal/utils"

	"github.com/theQRL/go-zond/common"
	"github.com/theQRL/go-zond/common/hexutil"
	"github.com/theQRL/go-zond/rpc"
)

func DetectTokenContract(ctx context.Context, client *rpc.Client, addr common.Address) (bool, string, error) {
	// Try ERC20
	erc20Sig := []string{"name()", "symbol()", "decimals()", "totalSupply()"}
	matches := 0
	for _, sig := range erc20Sig {
		res, err := callContractRaw(ctx, client, addr, sig)
		if err == nil && len(res) > 0 {
			matches++
		}
	}
	if matches >= 2 {
		return true, "ERC20", nil
	}

	// Try ERC721
	dummyAddr, _ := utils.HexToAddress("0x0000000000000000000000000000000000000001")
	balanceRes, _ := callContractRaw(ctx, client, addr, "balanceOf(address)", dummyAddr)
	ownerRes, _ := callContractRaw(ctx, client, addr, "ownerOf(uint256)", "1")
	support721, _ := callContractRaw(ctx, client, addr, "supportsInterface(bytes4)", "80ac58cd")

	if len(balanceRes) > 0 && len(ownerRes) > 0 && hex.EncodeToString(support721) == "01" {
		return true, "ERC721", nil
	}

	// Try ERC1155
	support1155, _ := callContractRaw(ctx, client, addr, "supportsInterface(bytes4)", "d9b67a26")
	if hex.EncodeToString(support1155) == "01" {
		return true, "ERC1155", nil
	}

	return false, "", nil
}

func callContractRaw(ctx context.Context, client *rpc.Client, contractAddr common.Address, methodSig string, params ...interface{}) ([]byte, error) {
	var methodID string
	var args []interface{}

	if strings.Contains(methodSig, "(") {
		sigHash := utils.Keccak256([]byte(methodSig))
		methodID = hex.EncodeToString(sigHash[:4])
		args = params
	} else {
		methodID = strings.TrimPrefix(methodSig, "0x")
		args = params
	}

	data := methodID
	for _, param := range args {
		switch v := param.(type) {
		case common.Address:
			data += fmt.Sprintf("%064s", hex.EncodeToString(v.Bytes()))
		case string:
			data += fmt.Sprintf("%064s", strings.TrimPrefix(v, "0x"))
		default:
			return nil, fmt.Errorf("unsupported param type: %T", v)
		}
	}

	var result hexutil.Bytes
	err := client.CallContext(ctx, &result, "zond_call", map[string]interface{}{
		"to":   contractAddr,
		"data": "0x" + data,
	}, "latest")
	if err != nil {
		return nil, fmt.Errorf("call %s: %w", methodSig, err)
	}
	return result, nil
}
