use crate::models::PoolConfig;
use anyhow::{Context, Result};
use rusqlite::{Connection, params};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct DynamicCandidatePool {
    pub pool: PoolConfig,
    pub token_mint: String,
    pub quote_mint: String,
    pub dex_count: u64,
    pub pool_count: u64,
    pub updated_unix: u64,
}

pub fn fetch_dynamic_candidate_pools(
    db_path: &str,
    max_rows: usize,
) -> Result<Vec<DynamicCandidatePool>> {
    if db_path.trim().is_empty() {
        return Ok(Vec::new());
    }

    let path = Path::new(db_path);
    if !path.exists() {
        return Ok(Vec::new());
    }

    let conn =
        Connection::open(path).with_context(|| format!("failed to open candidate db {db_path}"))?;

    let mut stmt = conn
        .prepare(
            "SELECT
                p.dex,
                p.kind,
                p.pool_id,
                p.token_mint,
                p.quote_mint,
                c.dex_count,
                c.pool_count,
                c.updated_unix
            FROM pools p
            JOIN pair_candidates c
              ON p.token_mint = c.token_mint
             AND p.quote_mint = c.quote_mint
            WHERE c.eligible = 1
            ORDER BY c.updated_unix DESC, p.last_seen_unix DESC
            LIMIT ?1",
        )
        .context("failed to prepare candidate query")?;

    let rows = stmt
        .query_map(params![max_rows as i64], |row| {
            let dex: String = row.get(0)?;
            let kind: String = row.get(1)?;
            let pool_id: String = row.get(2)?;
            let token_mint: String = row.get(3)?;
            let quote_mint: String = row.get(4)?;
            let dex_count: i64 = row.get(5)?;
            let pool_count: i64 = row.get(6)?;
            let updated_unix: i64 = row.get(7)?;

            Ok(DynamicCandidatePool {
                pool: PoolConfig {
                    dex: Some(dex),
                    kind,
                    symbol: format!("{}_{}", short_mint(&token_mint), short_mint(&quote_mint)),
                    pool_id,
                },
                token_mint,
                quote_mint,
                dex_count: dex_count.max(0) as u64,
                pool_count: pool_count.max(0) as u64,
                updated_unix: updated_unix.max(0) as u64,
            })
        })
        .context("failed to execute candidate query")?;

    let mut out = Vec::new();
    for row in rows {
        out.push(row.context("failed to decode candidate row")?);
    }
    Ok(out)
}

fn short_mint(mint: &str) -> String {
    if mint.len() <= 8 {
        return mint.to_string();
    }
    format!("{}{}", &mint[0..4], &mint[mint.len() - 4..])
}
