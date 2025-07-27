# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Local Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m camoufox fetch
```

### Running the Crawler
```bash
python main.py
```

### Docker
```bash
docker build -t search-crawler .
docker run search-crawler
```

## Architecture

This is a web crawler built with Python that uses Camoufox (stealth Firefox) and Playwright for browser automation. The system is designed to crawl domains from a PostgreSQL database queue.

### Core Components

- **main.py**: Single-file application containing the entire crawler logic
- **Database Integration**: Uses asyncpg to connect to PostgreSQL with domain queue management
- **Browser Automation**: Camoufox + Playwright for stealth web scraping
- **Queue System**: Claims domains using `FOR UPDATE SKIP LOCKED` for concurrent processing

### Key Functions

- `claim_one_domain()`: Claims and locks next available domain from queue
- `analyze_page()`: Extracts basic page information (currently just title)
- `insert_domain_record()`: Stores crawled data back to database
- `crawl_one()`: Main crawling logic for a single domain

### Database Schema

The crawler expects two tables:
- `domain_ingestion`: Queue of domains to crawl (with locking mechanism)
- `domain`: Storage for crawled domain data and metadata

### Environment Variables

- `POSTGRES_URL`: PostgreSQL connection string (loaded from .env file)

### Dependencies

- `camoufox`: Stealth Firefox browser
- `playwright`: Browser automation
- `asyncpg`: Async PostgreSQL driver
- `python-dotenv`: Environment variable loading