package token

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"ZOND-INDEXER/internal/utils"

	"github.com/theQRL/go-zond/common"
	"github.com/theQRL/go-zond/common/hexutil"
	"github.com/theQRL/go-zond/rpc"
)

// DetectTokenContract checks if the contract at the given address is an ERC20, ERC721, or ERC1155 token
func DetectTokenContract(ctx context.Context, client *rpc.Client, addr common.Address) (bool, string, error) {
	// Check for ERC20: supports name(), symbol(), decimals(), totalSupply()
	erc20Signatures := []string{
		"name()",        // 0x06fdde03
		"symbol()",      // 0x95d89b41
		"decimals()",    // 0x313ce567
		"totalSupply()", // 0x18160ddd
	}

	erc20Supported := true
	for _, sig := range erc20Signatures {
		result, err := callContractRaw(ctx, client, addr, sig)
		if err != nil || len(result) == 0 {
			erc20Supported = false
			break
		}
	}

	if erc20Supported {
		return true, "ERC20", nil
	}

	// Check for ERC721: supports balanceOf(), ownerOf(), supportsInterface(0x80ac58cd)
	erc721InterfaceID := "0x80ac58cd"
	erc721Signatures := []string{
		"balanceOf(address)",        // 0x70a08231
		"ownerOf(uint256)",          // 0x6352211e
		"supportsInterface(bytes4)", // 0x01ffc9a7
	}

	erc721Supported := true
	for _, sig := range erc721Signatures[:2] { // Check balanceOf and ownerOf
		// Use utils.HexToAddress instead of indexer.HexToAddress
		dummyAddr, err := utils.HexToAddress("0x0000000000000000000000000000000000000001")
		if err != nil {
			return false, "", fmt.Errorf("convert dummy address: %w", err)
		}
		result, err := callContractRaw(ctx, client, addr, sig, dummyAddr)
		if err != nil || len(result) == 0 {
			erc721Supported = false
			break
		}
	}

	// Check supportsInterface for ERC721
	if erc721Supported {
		result, err := callContractRaw(ctx, client, addr, "supportsInterface(bytes4)", erc721InterfaceID)
		if err != nil || len(result) == 0 || hex.EncodeToString(result) != "01" {
			erc721Supported = false
		}
	}

	if erc721Supported {
		return true, "ERC721", nil
	}

	// Check for ERC1155: supports supportsInterface(0xd9b67a26)
	erc1155InterfaceID := "0xd9b67a26"
	result, err := callContractRaw(ctx, client, addr, "supportsInterface(bytes4)", erc1155InterfaceID)
	if err != nil || len(result) == 0 || hex.EncodeToString(result) != "01" {
		return false, "", nil
	}

	return true, "ERC1155", nil
}

// callContractRaw calls a contract method with raw parameters
func callContractRaw(ctx context.Context, client *rpc.Client, contractAddr common.Address, methodSig string, params ...interface{}) ([]byte, error) {
	var methodID string
	var args []interface{}

	if strings.Contains(methodSig, "(") {
		// Compute the method ID (first 4 bytes of keccak256 hash of the signature)
		sigHash := common.BytesToHash(keccak256([]byte(methodSig)))
		methodID = hex.EncodeToString(sigHash[:4])
		args = params
	} else {
		// Method signature is already a hex string (e.g., "0x01ffc9a7")
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
			return nil, fmt.Errorf("unsupported parameter type: %T", v)
		}
	}

	var result hexutil.Bytes
	err := client.CallContext(ctx, &result, "zond_call", map[string]interface{}{
		"to":   contractAddr,
		"data": "0x" + data,
	}, "latest")
	if err != nil {
		return nil, fmt.Errorf("call contract method %s: %w", methodSig, err)
	}

	return result, nil
}

func keccak256(data []byte) []byte {
	// use common.BytesToHash as a simple Keccak256 implementation
	return common.BytesToHash(data).Bytes()
}
