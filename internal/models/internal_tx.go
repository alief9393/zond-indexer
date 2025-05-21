package models

type InternalTransaction struct {
	TxHash      string
	From        string
	To          string
	Value       string
	Input       string
	Output      string
	Type        string
	Gas         string
	GasUsed     string
	BlockNumber uint64
	Depth       int
}
