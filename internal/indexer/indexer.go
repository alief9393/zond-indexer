package indexer

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"strconv"
	"time"

	"zond-indexer/internal/config"
	"zond-indexer/internal/utils"

	logger "zond-indexer/internal/log"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/theQRL/go-zond/core/types"
	"github.com/theQRL/go-zond/rpc"
	"github.com/theQRL/go-zond/zondclient"
)

// Indexer holds the state for indexing Zond blocks
type Indexer struct {
	config             config.Config
	client             *zondclient.Client
	rpcClient          *rpc.Client
	db                 *pgxpool.Pool
	chainID            *big.Int
	rateLimit          time.Duration
	latest             uint64
	historical         uint64
	lastValidatorIndex uint64
	epochLength        uint64
	lastHead           struct {
		blockNumber int64
		blockHash   []byte
	}
	validatorIndexer *ValidatorIndexer
	amqpChannel      *amqp.Channel
	amqpConn         *amqp.Connection
	isStatsJobStarted  bool
}

func NewIndexer(config config.Config) (*Indexer, error) {
	log := logger.Logger

	log.Infof("Connecting to WS endpoint: %s", config.WSEndpoint)
	rpcClient, err := rpc.Dial(config.WSEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to WS endpoint: %w", err)
	}
	client := zondclient.NewClient(rpcClient)

	log.Info("Fetching chain ID")
	chainID, err := client.ChainID(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to fetch chain ID: %w", err)
	}

	log.Info("Connecting to database")
	dbConfig, err := pgxpool.ParseConfig(config.PostgresConn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Postgres connection string: %w", err)
	}
	db, err := pgxpool.NewWithConfig(context.Background(), dbConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	log.Info("Pinging database")
	err = db.Ping(context.Background())
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	log.Println("Connecting to RabbitMQ...")
	amqpConn, err := amqp.Dial(config.RABBITMQ_URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	amqpChannel, err := amqpConn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open RabbitMQ channel: %w", err)
	}

	_, err = amqpChannel.QueueDeclare(
		"zond_blocks_queue",
		true, false, false, false, nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare RabbitMQ queue: %w", err)
	}
	log.Println(" RabbitMQ channel and queue are ready.")

	var lastIndexedBlock int64
	err = db.QueryRow(context.Background(), "SELECT COALESCE(MAX(block_number), -1) FROM blocks").Scan(&lastIndexedBlock)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to fetch last indexed block: %w", err)
	}

	start := uint64(lastIndexedBlock + 1)

	log.Infof("✅ Indexer successfully initialized. Starting from block #%d", start)

	validatorIndexer := &ValidatorIndexer{
		Client:               client,
		DB:                   db,
		Config:               config,
		EpochLength:          32,
		LastValidatorIndexed: 0,
	}

	return &Indexer{
		config:             config,
		client:             client,
		rpcClient:          rpcClient,
		db:                 db,
		chainID:            chainID,
		rateLimit:          config.RateLimit,
		latest:             start,
		historical:         start,
		lastValidatorIndex: 0,
		epochLength:        32,
		validatorIndexer:   validatorIndexer,
		amqpChannel:        amqpChannel,
		amqpConn:           amqpConn,
	}, nil
}

func (i *Indexer) Close() {
	i.db.Close()
	i.rpcClient.Close()
	if i.amqpChannel != nil {
		i.amqpChannel.Close()
	}
	if i.amqpConn != nil {
		i.amqpConn.Close()
	}
}

func (i *Indexer) Config() config.Config {
	return i.config
}

func (i *Indexer) Client() *zondclient.Client {
	return i.client
}

func (i *Indexer) RPCClient() *rpc.Client {
	return i.rpcClient
}

func (i *Indexer) DB() *pgxpool.Pool {
	return i.db
}

func (i *Indexer) ChainID() *big.Int {
	return i.chainID
}

func (i *Indexer) RunConsumer(ctx context.Context) error {
	go i.StartDailyStatsJob(ctx)
	msgs, err := i.amqpChannel.Consume(
		"zond_blocks_queue",
		"", false, false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("failed to register a consumer: %w", err)
	}

	log.Println(" Consumer is waiting for block messages. To exit press CTRL+C")

	done := make(chan struct{})

	go func() {
		defer func() {
			log.Println("AMQP message loop exited.")
			close(done)
		}()

		for d := range msgs {
			blockNumberStr := string(d.Body)
			blockNumber, err := strconv.ParseUint(blockNumberStr, 10, 64)
			if err != nil {
				log.Printf("Invalid block number received: %s. Rejecting.", blockNumberStr)
				d.Nack(false, false)
				continue
			}

			log.Printf("📬 Received block #%d. Processing...", blockNumber)

			err = indexBlock(ctx, i.config, i.client, i.rpcClient, i.db, blockNumber, i.chainID, true)
			if err != nil {
				log.Printf(" Failed to index block #%d: %v. Re-queuing.", blockNumber, err)
				d.Nack(false, true)
				time.Sleep(1 * time.Second)
				continue
			}

			d.Ack(false)
			log.Printf(" Successfully indexed block #%d.", blockNumber)
		}
	}()

	select {
	case <-ctx.Done():
		log.Println("Consumer shutting down (signal received)...")
		<-done
		log.Println("AMQP processing finished.")
		return nil

	case <-done:
		return fmt.Errorf("amqp consumer channel closed unexpectedly (connection lost?)")
	}
}

func (i *Indexer) StartDailyStatsJob(ctx context.Context) {
    if i.isStatsJobStarted {
        return
    }
    i.isStatsJobStarted = true
    
    go RunDailyStatsScheduler(ctx, i.db)
}

func (i *Indexer) Run(ctx context.Context) error {
	syncing, err := i.client.SyncProgress(ctx)
	if err != nil {
		return fmt.Errorf(" failed to check sync progress: %w", err)
	}
	if syncing != nil {
		return fmt.Errorf("🚧 node is not fully synced: current=%d highest=%d", syncing.CurrentBlock, syncing.HighestBlock)
	}
	logger.Logger.Info(" Node is fully synced")
	go i.StartDailyStatsJob(ctx)
	go i.StartPendingTxWatcher(ctx)
	var lastIndexedBlock int64
	err = i.db.QueryRow(ctx, `SELECT COALESCE(MAX(block_number), -1) FROM Blocks WHERE canonical = TRUE`).Scan(&lastIndexedBlock)
	if err != nil {
		return fmt.Errorf(" failed to fetch last indexed block: %w", err)
	}
	startBlock := uint64(lastIndexedBlock + 1)

	latestBlock, err := i.client.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf(" failed to get latest block number from node: %w", err)
	}
	i.historical = latestBlock

	logger.Logger.Infof(" Resuming block indexing from #%d up to cap #%d", startBlock, i.historical)

	for blockNum := startBlock; blockNum <= i.historical; blockNum++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			logger.Logger.Infof("  Indexing historical block #%d", blockNum)

			if err := i.validatorIndexer.IndexPeriodically(ctx, blockNum); err != nil {
				logger.Logger.WithFields(logrus.Fields{
					"block": blockNum,
					"err":   err,
				}).Warn("  Validator indexing failed")
				continue
			}

			if err := indexBlock(ctx, i.config, i.client, i.rpcClient, i.db, blockNum, i.chainID, true); err != nil {
				logger.Logger.WithFields(logrus.Fields{
					"block": blockNum,
					"err":   err,
				}).Error(" Failed to index block")
				continue
			}

			block, err := i.client.BlockByNumber(ctx, big.NewInt(int64(blockNum)))
			if err != nil {
				logger.Logger.WithFields(logrus.Fields{
					"block": blockNum,
					"err":   err,
				}).Error(" Failed to fetch block for head tracking")
				continue
			}

			i.lastHead.blockNumber = block.Number().Int64()
			i.lastHead.blockHash = block.Hash().Bytes()
			time.Sleep(i.rateLimit)
		}
	}

	logger.Logger.Info(" Switching to real-time block indexing")

	headers := make(chan *types.Header)
	sub, err := i.client.SubscribeNewHead(ctx, headers)
	if err != nil {
		return fmt.Errorf(" failed to subscribe to new headers: %w", err)
	}
	defer sub.Unsubscribe()

	go func() {
		for {
			time.Sleep(5 * time.Second)
			logger.Logger.Info("⏳ Waiting for new block...")
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sub.Err():
			return fmt.Errorf(" subscription error: %w", err)
		case header := <-headers:
			blockNum := header.Number.Uint64()
			logger.Logger.Infof(" New block received: #%d", blockNum)

			block, err := i.client.BlockByNumber(ctx, big.NewInt(int64(blockNum)))
			if err != nil {
				logger.Logger.WithField("block", blockNum).WithError(err).Error(" Failed to fetch block")
				continue
			}

			parentHash := block.ParentHash().Bytes()
			if i.lastHead.blockNumber > 0 && i.lastHead.blockNumber == int64(blockNum)-1 {
				if !utils.BytesEqual(parentHash, i.lastHead.blockHash) {
					logger.Logger.Warnf(" Reorg detected at block #%d", blockNum)
					if err := i.handleReorg(ctx, block); err != nil {
						logger.Logger.WithField("block", blockNum).WithError(err).Error(" Failed to handle reorg")
						continue
					}
				}
			}

			if err := i.validatorIndexer.IndexPeriodically(ctx, blockNum); err != nil {
				logger.Logger.WithFields(logrus.Fields{
					"block": blockNum,
					"err":   err,
				}).Warn("⚠️  Validator indexing failed")
				continue
			}

			if err := indexBlock(ctx, i.config, i.client, i.rpcClient, i.db, blockNum, i.chainID, true); err != nil {
				logger.Logger.WithFields(logrus.Fields{
					"block": blockNum,
					"err":   err,
				}).Error(" Failed to index block")
				continue
			}

			i.lastHead.blockNumber = block.Number().Int64()
			i.lastHead.blockHash = block.Hash().Bytes()
			i.latest = blockNum
		}
	}
}
