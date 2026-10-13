use crate::{
    BUCKET_NAME, Progress,
    list::{get_snapshots, list_all_keys},
    pack::{self, FileRecord, Manifest},
};
use aws_sdk_s3::Client;
use indicatif::ProgressBar;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

/// Parts are few and large, so the work is bandwidth-bound: a dozen in flight
/// already saturates a typical connection.
const PART_CONCURRENCY: usize = 12;
/// Legacy snapshots cost one round trip per file; there the win comes from
/// having many requests in flight, not from bytes per request.
const FILE_CONCURRENCY: usize = 64;
/// Downloaded parts are cached so an interrupted restore resumes instead of
/// re-fetching. The directory is already gitignored.
const CACHE_DIR: &str = "target/bsubp-cache";

pub async fn run(
    client: &Client,
    id: Option<String>,
    needs_confirm: bool,
    progress: &Progress,
) -> anyhow::Result<()> {
    let snapshot_id = match id {
        Some(id) => {
            if id == "latest" {
                let mut snapshots = get_snapshots(client).await?;
                if let Some(latest_id) = snapshots.pop() {
                    latest_id
                } else {
                    anyhow::bail!("no snapshots found");
                }
            } else {
                id
            }
        }
        None => {
            let snapshots = get_snapshots(client).await?;
            if snapshots.is_empty() {
                anyhow::bail!("no snapshots found");
            }

            let mut choices = snapshots;
            choices.reverse();
            inquire::Select::new("Which snapshot?", choices).prompt()?
        }
    };

    if needs_confirm
        && !inquire::Confirm::new(&format!("Restore snapshot `{snapshot_id}`"))
            .with_default(false)
            .with_help_message("This will use your network to download from the cloudflare storage")
            .prompt()?
    {
        return Ok(());
    }

    tracing::info!(snapshot = %snapshot_id, "starting restore");

    restore_snapshot(client, &snapshot_id, Path::new("."), progress).await?;

    tracing::info!(snapshot = %snapshot_id, "restore complete");

    Ok(())
}

async fn restore_snapshot(
    client: &Client,
    snapshot_id: &str,
    dst_root: &Path,
    progress: &Progress,
) -> anyhow::Result<()> {
    // Packed snapshots are the fast path: a handful of large GETs instead of
    // one request per asset.
    if let Some(manifest) = pack::fetch_manifest(client, BUCKET_NAME, snapshot_id).await? {
        return restore_packed(client, &manifest, dst_root, progress).await;
    }

    tracing::warn!(
        snapshot = %snapshot_id,
        "no manifest found; restoring a legacy one-object-per-file snapshot. \
         This layout is latency-bound and cannot be accelerated in place; \
         take a new backup to get the packed format"
    );
    restore_legacy(client, snapshot_id, dst_root, progress).await
}

/// Fetch every part (reusing any complete cached copy), then unpack the files.
async fn restore_packed(
    client: &Client,
    manifest: &Manifest,
    dst_root: &Path,
    progress: &Progress,
) -> anyhow::Result<()> {
    let cache = Path::new(CACHE_DIR).join(&manifest.snapshot);
    tokio::fs::create_dir_all(&cache).await?;

    let total_raw: u64 = manifest.parts.iter().sum();
    let bar = progress.bar_bytes(total_raw, "downloading parts");
    let semaphore = Arc::new(Semaphore::new(PART_CONCURRENCY));
    let mut joinset = JoinSet::new();

    for (part, raw_len) in manifest.parts.iter().enumerate() {
        let client = client.clone();
        let bar = bar.clone();
        let semaphore = semaphore.clone();
        let dest = cache.join(format!("{part:04}.tar.zst"));
        let key = format!("{}/{}", manifest.snapshot, manifest.key_for(part));
        let raw_len = *raw_len;

        joinset.spawn(async move {
            let _permit = semaphore.acquire_owned().await?;

            // A part is either fully written or absent: downloads land via a
            // temporary file, so presence plus non-zero length is enough.
            // Reusing the cache is what makes an interrupted restore resume.
            if let Ok(metadata) = tokio::fs::metadata(&dest).await
                && metadata.len() > 0
            {
                bar.inc(raw_len);
                return Ok::<_, anyhow::Error>((part, dest));
            }

            download_part(&client, &key, &dest, raw_len, &bar).await?;
            Ok((part, dest))
        });
    }

    let mut parts: Vec<(usize, PathBuf)> = Vec::with_capacity(manifest.parts.len());
    while let Some(res) = joinset.join_next().await {
        parts.push(res??);
    }
    parts.sort_by_key(|(part, _)| *part);
    bar.finish_with_message("parts downloaded");

    tracing::info!(
        parts = parts.len(),
        files = manifest.files.len(),
        "unpacking snapshot"
    );

    unpack_parts(manifest, &parts, dst_root, progress).await
}

/// Unpack parts concurrently: each part is independent, so files land on disk
/// while other parts are still being extracted.
async fn unpack_parts(
    manifest: &Manifest,
    parts: &[(usize, PathBuf)],
    dst_root: &Path,
    progress: &Progress,
) -> anyhow::Result<()> {
    let bar = progress.bar_items(manifest.files.len() as u64, "unpacking files");
    let mut joinset = JoinSet::new();

    for (part, path) in parts.iter().map(|(part, path)| (*part, path.clone())) {
        let dst_root = dst_root.to_path_buf();
        let bar = bar.clone();
        // Owned records keep the spawned task self-contained.
        let records: Vec<FileRecord> = manifest
            .files_of_part(part)
            .into_iter()
            .map(|record| FileRecord {
                path: record.path.clone(),
                part: record.part,
                size: record.size,
                offset: record.offset,
                unix_mode: record.unix_mode,
                mtime: record.mtime,
            })
            .collect();

        joinset.spawn_blocking(move || {
            let compressed = std::fs::read(&path)?;
            let raw = pack::decode_part(&compressed)?;
            unpack_part(&raw, &records, &dst_root)?;
            bar.inc(records.len() as u64);
            Ok::<_, anyhow::Error>(())
        });
    }

    while let Some(res) = joinset.join_next().await {
        res??;
    }

    bar.finish_with_message("restore complete");
    Ok(())
}

/// Write every member of an uncompressed part tar to its destination.
///
/// Members are matched by raw header position, which is exactly the offset the
/// packer recorded, so no path parsing or reads of unwanted files are needed.
fn unpack_part(raw: &[u8], records: &[FileRecord], dst_root: &Path) -> anyhow::Result<()> {
    let mut archive = tar::Archive::new(raw);
    let mut by_offset: HashMap<u64, &FileRecord> = records.iter().map(|r| (r.offset, r)).collect();

    for entry in archive.entries()? {
        let mut entry = entry?;
        let offset = entry.raw_header_position() + 512;
        let Some(record) = by_offset.remove(&offset) else {
            continue;
        };

        let dest = dst_root.join(safe_relative(&record.path)?);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut file = std::fs::File::create(&dest)?;
        std::io::copy(&mut entry, &mut file)?;
        apply_metadata(&dest, record);
    }

    Ok(())
}

#[cfg(unix)]
fn apply_metadata(path: &Path, record: &FileRecord) {
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(record.unix_mode));
}

#[cfg(not(unix))]
fn apply_metadata(_path: &Path, _record: &FileRecord) {}

/// Stream one part down to the restore cache.
async fn download_part(
    client: &Client,
    key: &str,
    dest: &Path,
    raw_len: u64,
    bar: &ProgressBar,
) -> anyhow::Result<()> {
    let mut resp = client
        .get_object()
        .bucket(BUCKET_NAME)
        .key(key)
        .send()
        .await?;

    // Write to a sibling file and rename, so a killed restore never leaves a
    // truncated part behind that the cache check would accept.
    let tmp = dest.with_extension("part");
    {
        let mut file = tokio::fs::File::create(&tmp).await?;
        while let Some(chunk) = resp.body.next().await {
            let bytes = chunk?;
            file.write_all(&bytes).await?;
        }
        file.flush().await?;
    }
    tokio::fs::rename(&tmp, dest).await?;

    tracing::debug!(key, raw_len, "part downloaded");
    bar.inc(raw_len);
    Ok(())
}

/// Legacy restore: one object per file, fetched with high concurrency.
async fn restore_legacy(
    client: &Client,
    snapshot_id: &str,
    dst_root: &Path,
    progress: &Progress,
) -> anyhow::Result<()> {
    let prefix = format!("{snapshot_id}/");
    let keys = list_all_keys(client, &prefix).await?;

    if keys.is_empty() {
        anyhow::bail!("snapshot {snapshot_id} has no objects");
    }

    let bar = progress.bar_items(keys.len() as u64, "downloading files");
    let semaphore = Arc::new(Semaphore::new(FILE_CONCURRENCY));
    let mut joinset = JoinSet::new();

    for key in keys {
        // Strip the snapshot prefix so files land at their original paths.
        let relative = key
            .strip_prefix(&prefix)
            .ok_or_else(|| anyhow::anyhow!("key {key} does not start with prefix {prefix}"))?;
        let rel_path = safe_relative(relative)?;

        let label = rel_path.display().to_string();
        let dest = dst_root.join(rel_path);

        let client = client.clone();
        let bar = bar.clone();
        let semaphore = semaphore.clone();

        joinset.spawn(async move {
            let _permit = semaphore.acquire_owned().await?;
            download_file(&client, &key, &dest).await?;
            bar.set_message(label);
            bar.inc(1);
            Ok::<_, anyhow::Error>(())
        });
    }

    while let Some(res) = joinset.join_next().await {
        res??;
    }

    bar.finish_with_message("restore complete");
    Ok(())
}

fn safe_relative(p: &str) -> anyhow::Result<PathBuf> {
    let mut out = PathBuf::new();
    for seg in p.replace('\\', "/").split('/') {
        match seg {
            "" | "." => continue,
            ".." => anyhow::bail!("refusing path traversal in key: {p}"),
            s => out.push(s),
        }
    }
    if out.as_os_str().is_empty() {
        anyhow::bail!("key resolves to empty path: {p}");
    }
    Ok(out)
}

/// Stream a single object from R2 to a local path (legacy snapshots).
async fn download_file(client: &Client, key: &str, dest: &Path) -> anyhow::Result<()> {
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let mut resp = client
        .get_object()
        .bucket(BUCKET_NAME)
        .key(key)
        .send()
        .await?;

    let mut file = tokio::fs::File::create(dest).await?;

    // Stream chunk-by-chunk so large files don't sit in memory.
    while let Some(chunk) = resp.body.next().await {
        let bytes = chunk?;
        file.write_all(&bytes).await?;
    }

    file.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pack::{self, SourceFile};
    use std::io::Write;

    fn write_source(root: &Path, rel: &str, contents: &[u8]) -> SourceFile {
        let abs = root.join(rel);
        std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
        std::fs::File::create(&abs)
            .unwrap()
            .write_all(contents)
            .unwrap();
        SourceFile {
            abs_path: abs,
            rel_path: rel.to_string(),
            size: contents.len() as u64,
            unix_mode: 0o644,
            mtime: 1_700_000_000,
        }
    }

    /// The offsets the packer records must be exactly the offsets the unpacker
    /// derives from the tar stream, in both nested and multi-member parts.
    #[test]
    fn pack_then_unpack_restores_files_byte_for_byte() {
        let work = std::env::temp_dir().join(format!("bsubp-unpack-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&work);
        let src = work.join("src");
        let out = work.join("out");

        let contents = [
            ("Assets/Unlicensed/a.png", vec![1u8; 5000]),
            ("Assets/Unlicensed/nested/b.bin", b"nested payload".to_vec()),
            ("Assets/Unlicensed/c.wav", vec![9u8; 70_000]),
            ("Assets/Unlicensed.meta", b"meta file".to_vec()),
        ];
        let files: Vec<SourceFile> = contents
            .iter()
            .map(|(rel, bytes)| write_source(&src, rel, bytes))
            .collect();

        let members: Vec<usize> = (0..files.len()).collect();
        let (compressed, _, offsets) = pack::build_part(&files, &members).unwrap();
        let records: Vec<FileRecord> = offsets
            .iter()
            .map(|(idx, offset)| FileRecord {
                path: files[*idx].rel_path.clone(),
                part: 0,
                size: files[*idx].size,
                offset: *offset,
                unix_mode: files[*idx].unix_mode,
                mtime: files[*idx].mtime,
            })
            .collect();

        let raw = pack::decode_part(&compressed).unwrap();
        unpack_part(&raw, &records, &out).unwrap();

        for (rel, expected) in &contents {
            let restored = out.join(rel.replace('/', std::path::MAIN_SEPARATOR_STR));
            let actual = std::fs::read(&restored)
                .unwrap_or_else(|e| panic!("missing {}: {e}", restored.display()));
            assert_eq!(&actual, expected, "content mismatch for {rel}");
        }

        std::fs::remove_dir_all(&work).unwrap();
    }

    /// Paths coming out of a manifest must never escape the restore root.
    #[test]
    fn safe_relative_rejects_traversal() {
        assert!(safe_relative("../evil").is_err());
        assert!(safe_relative("a/../../evil").is_err());
        assert!(safe_relative("").is_err());
        assert_eq!(
            safe_relative("Assets/Unlicensed/a.png").unwrap(),
            PathBuf::from("Assets").join("Unlicensed").join("a.png")
        );
    }
}
