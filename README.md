# Zond Indexer

The Zond Indexer is a Go-based application designed to index blockchain data from a Zond node (a fork of Ethereum) and store it in a PostgreSQL database for efficient querying and analysis. It processes historical and real-time blockchain data, including blocks, transactions, accounts, contracts, tokens, NFTs, validators, gas prices, and Zond nodes. The indexer is built to be reliable, scalable, and easy to deploy, with support for graceful shutdown and database migrations.

## Features
- Indexes blockchain data from a Zond node (blocks, transactions, accounts, etc.).
- Stores data in a PostgreSQL database with a well-defined schema.
- Supports historical and real-time indexing.
- Handles validators and gas prices for network monitoring.
- Includes database migrations for schema management.
- Provides a script to verify indexed data (`checks_data.sql`).
- Graceful shutdown on interrupt signals (SIGINT, SIGTERM).
- Configurable via environment variables (`.env` file).

## Prerequisites
Before setting up the Zond Indexer, ensure you have the following:

- **Go**: Version 1.17 or later (recommended: 1.22 or 1.23 as of April 2025). Install from [golang.org](https://golang.org/doc/install).
- **PostgreSQL**: Version 12 or later. Install on your system (e.g., via `apt` on Ubuntu, `brew` on macOS, or a Docker container).
- **Zond Node**: A running Zond node with WebSocket RPC (`ws://`) and Beacon API endpoints. You can run a Zond node locally or connect to a remote node.
- **Git**: To clone the repository.

## Installation
1. **Clone the Repository**:
   ```bash
   git clone https://github.com/alief9393/zond-indexer.git
   cd zond-indexer
   ```

2. **Install Dependencies**:
   Ensure all Go dependencies are installed:
   ```bash
   go mod tidy
   ```

3. **Set Up PostgreSQL**:
   - Create the database:
     ```bash
     psql -U postgres -c "CREATE DATABASE zond_indexer_db;"
     ```
   - Create the `zond_app` user and grant privileges by running the setup script:
     ```bash
     psql -U postgres -d zond_indexer_db -f scripts/setup_user.sql
     ```
     The script creates the `zond_app` user with the password `P@ssw0rd` and grants necessary privileges (`SELECT`, `INSERT`, `UPDATE`, `CREATE`) on the `zond_indexer_db` database.

## Configuration
The indexer is configured via a `.env` file in the project root. Create a `.env` file with the following content:

```env
POSTGRES_CONN=user=zond_app password=P@ssw0rd dbname=zond_indexer_db sslmode=disable host=localhost port=5432
RPC_ENDPOINT=ws://127.0.0.1:62127
RATE_LIMIT_MS=500
BEACON_HOST=localhost
BEACON_PORT=62139
```

- `POSTGRES_CONN`: Connection string for the PostgreSQL database.
- `RPC_ENDPOINT`: WebSocket URL of the Zond node’s RPC endpoint.
- `RATE_LIMIT_MS`: Rate limit for API requests (in milliseconds).
- `BEACON_HOST` and `BEACON_PORT`: Host and port of the Zond node’s Beacon API.

Ensure your Zond node is running and accessible at the specified `RPC_ENDPOINT` and `BEACON_HOST:BEACON_PORT`.

## Usage
1. **Run the Indexer (Development)**:
   Run the indexer directly using `go run`:
   ```bash
   go run ./cmd/indexer
   ```
   This will start indexing historical blocks and listen for new blocks in real-time. The indexer applies database migrations automatically on startup.

2. **Graceful Shutdown**:
   - Press `Ctrl+C` (SIGINT) or send a `SIGTERM` signal to stop the indexer gracefully.
   - The indexer will close database connections and shut down cleanly.

## Building
To build the indexer into a standalone binary:

1. **Build the Binary**:
   ```bash
   go build -o zond-indexer ./cmd/indexer
   ```
   This creates an executable named `zond-indexer` in the project root.

2. **Run the Binary**:
   ```bash
   ./zond-indexer
   ```

3. **Cross-Compile for Other Platforms** (Optional):
   - For Linux (AMD64):
     ```bash
     GOOS=linux GOARCH=amd64 go build -o zond-indexer-linux-amd64 ./cmd/indexer
     ```
   - For Windows (AMD64):
     ```bash
     GOOS=windows GOARCH=amd64 go build -o zond-indexer-windows-amd64.exe ./cmd/indexer
     ```

## Deployment
To deploy the indexer to a production server (e.g., Linux):

1. **Copy the Binary and `.env` File**:
   ```bash
   scp zond-indexer-linux-amd64 .env user@server:/path/to/deployment
   ```

2. **Set Up the Server**:
   - Ensure PostgreSQL is running and the `zond_indexer_db` database is set up (run `scripts/setup_user.sql` as described in the Installation section).
   - Ensure the Zond node is accessible at the `RPC_ENDPOINT` and `BEACON_HOST:BEACON_PORT` specified in the `.env` file.

3. **Run as a Service**:
   Create a systemd service to run the indexer continuously:
   ```bash
   sudo nano /etc/systemd/system/zond-indexer.service
   ```
   Add the following:
   ```
   [Unit]
   Description=Zond Indexer Service
   After=network.target

   [Service]
   ExecStart=/path/to/deployment/zond-indexer-linux-amd64
   WorkingDirectory=/path/to/deployment
   Restart=always
   User=youruser
   Environment="PATH=/usr/local/bin:/usr/bin:/bin"

   [Install]
   WantedBy=multi-user.target
   ```
   Enable and start the service:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable zond-indexer
   sudo systemctl start zond-indexer
   ```
   Check the status:
   ```bash
   sudo systemctl status zond-indexer
   ```

## Data Verification
After running the indexer, you can verify the indexed data using the `checks_data.sql` script. This script queries the top 5 rows from each table to inspect the data.

1. **Run the Script**:
   ```bash
   psql -U zond_app -d zond_indexer_db -f checks_data.sql
   ```
   You’ll be prompted for the `zond_app` password (`P@ssw0rd`).

2. **Example Output**:
   The script will display data from tables like `Blocks`, `Transactions`, `Accounts`, `Validators`, etc. For example:
   ```
    block_number | block_hash | timestamp           | miner_address    | ...
   --------------+------------+---------------------+------------------+-----
              70 | abcd1234   | 2025-04-12 13:43:27 | Z1234...         | ...
   ```

## Project Structure
- `cmd/indexer/`: Entry point of the application (`main.go`).
- `internal/config/`: Configuration loading and validation.
- `internal/db/`: Database migrations and connection setup.
- `internal/indexer/`: Core indexing logic for blocks, transactions, validators, etc.
- `internal/models/`: Data models and database operations for each table.
- `scripts/`: SQL scripts for setup (`setup_user.sql`) and data verification (`checks_data.sql`).

## Contributing
Contributions are welcome! To contribute:

1. Fork the repository.
2. Create a new branch (`git checkout -b feature/your-feature`).
3. Make your changes and commit them (`git commit -m "Add your feature"`).
4. Push to your branch (`git push origin feature/your-feature`).
5. Open a Pull Request on GitHub.

Please ensure your code follows the project’s style and includes tests where applicable.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact
For questions or support, please open an issue on the [GitHub repository](https://github.com/alief9393/zond-indexer) or contact the maintainer.
