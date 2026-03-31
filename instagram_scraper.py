import sys

from monolithic_code import get_logger, init_db, run_instagram_scraper


logger = get_logger(__name__)


def main() -> None:
    logger.info("Initializing Instagram scraper")

    try:
        init_db()
    except Exception as exc:
        logger.error(f"Could not initialize database: {exc}")
        logger.error("Please check database credentials or ensure PostgreSQL is running.")
        sys.exit(1)

    run_instagram_scraper()
    logger.info("Instagram scraping run completed successfully.")


if __name__ == "__main__":
    main()
