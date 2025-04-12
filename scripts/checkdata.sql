-- checks_data.sql
-- Script to check data in all tables after running the indexer

-- Query for Blocks table
SELECT 
    block_number,
    encode(block_hash, 'hex') AS block_hash,
    timestamp,
    'Z' || encode(miner_address, 'hex') AS miner_address,
    encode(parent_hash, 'hex') AS parent_hash,
    gas_used,
    gas_limit,
    size,
    transaction_count,
    encode(extra_data, 'hex') AS extra_data,
    base_fee_per_gas,
    encode(transactions_root, 'hex') AS transactions_root,
    encode(state_root, 'hex') AS state_root,
    encode(receipts_root, 'hex') AS receipts_root,
    encode(logs_bloom, 'hex') AS logs_bloom,
    chain_id,
    retrieved_from
FROM Blocks
ORDER BY block_number DESC
LIMIT 5;

-- Query for Accounts table
SELECT 
    'Z' || encode(address, 'hex') AS zond_address,
    balance,
    nonce,
    is_contract,
    code,
    first_seen,
    last_seen,
    retrieved_from
FROM Accounts
ORDER BY last_seen DESC
LIMIT 5;

-- Query for Transactions table
SELECT 
    encode(tx_hash, 'hex') AS tx_hash,
    block_number,
    'Z' || encode(from_address, 'hex') AS from_address,
    CASE 
        WHEN to_address IS NOT NULL THEN 'Z' || encode(to_address, 'hex')
        ELSE NULL
    END AS to_address,
    value,
    gas,
    gas_price,
    type,
    chain_id,
    access_list,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_index,
    cumulative_gas_used,
    is_successful,
    retrieved_from
FROM Transactions
ORDER BY block_number DESC
LIMIT 5;

-- Query for Contracts table
SELECT 
    'Z' || encode(address, 'hex') AS contract_address,
    contract_name,
    compiler_version,
    abi,
    source_code,
    optimization_enabled,
    runs,
    constructor_arguments,
    verified_date,
    license,
    is_canonical,
    retrieved_at,
    retrieved_from,
    reverted_at
FROM Contracts
ORDER BY retrieved_at DESC
LIMIT 5;

-- Query for Tokens table
SELECT 
    'Z' || encode(contract_address, 'hex') AS token_address,
    token_name,
    token_symbol,
    total_supply,
    decimals,
    token_type,
    website,
    logo,
    is_canonical,
    retrieved_at,
    retrieved_from,
    reverted_at
FROM Tokens
ORDER BY retrieved_at DESC
LIMIT 5;

-- Query for TokenTransactions table
SELECT 
    encode(tx_hash, 'hex') AS tx_hash,
    'Z' || encode(contract_address, 'hex') AS contract_address,
    'Z' || encode(from_address, 'hex') AS from_address,
    'Z' || encode(to_address, 'hex') AS to_address,
    token_id,
    value,
    is_canonical,
    retrieved_at,
    retrieved_from,
    reverted_at
FROM TokenTransactions
ORDER BY retrieved_at DESC
LIMIT 5;

-- Query for NFTs table
SELECT 
    'Z' || encode(contract_address, 'hex') AS contract_address,
    token_id,
    token_uri,
    'Z' || encode(owner, 'hex') AS owner_address,
    metadata,
    is_canonical,
    retrieved_at,
    retrieved_from,
    reverted_at
FROM NFTs
ORDER BY retrieved_at DESC
LIMIT 5;

-- Query for Validators table
SELECT 
    validator_index,
    encode(public_key, 'hex') AS public_key,
    deposit_amount,
    encode(withdrawal_credentials, 'hex') AS withdrawal_credentials,
    effective_balance,
    status,
    retrieved_at,
    retrieved_from,
    reverted_at
FROM Validators
ORDER BY retrieved_at DESC
LIMIT 5;

-- Query for GasPrices table
SELECT 
    timestamp,
    low_price,
    average_price,
    high_price,
    block_number,
    retrieved_at,
    retrieved_from,
    reverted_at
FROM GasPrices
ORDER BY timestamp DESC
LIMIT 5;

-- Query for ZondNodes table
SELECT 
    node_id,
    version,
    location,
    last_seen,
    latency,
    peers,
    retrieved_at,
    retrieved_from,
    reverted_at
FROM ZondNodes
ORDER BY last_seen DESC
LIMIT 5;