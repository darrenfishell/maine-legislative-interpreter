from pathlib import Path

import duckdb


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    db_path = repo_root / "data" / "maine-legislative-testimony.duckdb"

    conn = duckdb.connect(db_path)
    try:
        for old, new in [
            ("main_staging", "staging"),
            ("main_intermediate", "intermediate"),
            ("main_mart", "mart"),
        ]:
            exists = conn.execute(
                "SELECT COUNT(*) > 0 FROM information_schema.schemata WHERE schema_name = ?",
                [old],
            ).fetchone()[0]
            if exists:
                conn.execute(f'ALTER SCHEMA "{old}" RENAME TO "{new}"')
    finally:
        conn.close()


if __name__ == "__main__":
    main()

