import argparse
from pathlib import Path

from src.config.config import load_source_configs
from src.layer.bronze.bronze import run_source_ingestion


def load_secrets(secrets_path: Path) -> dict[str, str]:
    """Read credentials from .dlt/secrets.toml keyed by source name."""
    import tomllib

    if not secrets_path.exists():
        raise FileNotFoundError(
            f"Secrets file not found: {secrets_path}. "
            f"Copy .dlt/secrets.toml.example to .dlt/secrets.toml and fill in credentials."
        )

    with open(secrets_path, "rb") as f:
        raw = tomllib.load(f)

    secrets = {}
    for key, value in raw.get("sources", {}).items():
        if isinstance(value, dict) and "credentials" in value:
            secrets[key] = value["credentials"]

    return secrets


def main():
    parser = argparse.ArgumentParser(description="Bronze layer ingestion")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/src2brz_config.csv"),
        help="Path to CSV config file",
    )
    parser.add_argument(
        "--secrets",
        type=Path,
        default=Path(".dlt/secrets.toml"),
        help="Path to secrets.toml with database credentials",
    )
    parser.add_argument(
        "--bucket-url",
        type=str,
        default="data/bronze",
        help="Output directory for parquet files",
    )
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        help="Run only a specific source by name (default: run all)",
    )
    args = parser.parse_args()

    sources = load_source_configs(args.config)
    secrets = load_secrets(args.secrets)

    for source_config in sources:
        if args.source and source_config.name != args.source:
            continue

        credentials = secrets.get(source_config.name)
        if not credentials:
            print(f"[{source_config.name}] Skipping — no credentials in secrets file")
            continue

        print(f"[{source_config.name}] Starting ingestion...")
        run_source_ingestion(source_config, args.bucket_url, credentials)


if __name__ == "__main__":
    main()
