package utils

import (
	"math/big"
	"sort"
)

func SortGasPrices(prices []*big.Int) []*big.Int {
	sort.Slice(prices, func(i, j int) bool {
		return prices[i].Cmp(prices[j]) < 0
	})
	return prices
}

func CalculateAverageGasPrice(prices []*big.Int) *big.Int {
	sum := big.NewInt(0)
	for _, price := range prices {
		sum.Add(sum, price)
	}
	return sum.Div(sum, big.NewInt(int64(len(prices))))
}
