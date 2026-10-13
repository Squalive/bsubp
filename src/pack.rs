//! Packed snapshot format (v2).
//!
//! The original format stored one S3 object per asset, so a restore had to pay a
//! full HTTP round trip per file. For a snapshot of a few thousand small assets
//! that put the restore firmly in latency-bound territory: ~70MB of data cost
//! thousands of sequential-ish round trips while barely moving any bytes.
//!
//! A v2 snapshot instead stores a handful of *parts*. Each part is a tar stream
//! (files concatenated in a deterministic order) compressed with a single zstd
//! frame, so one ranged GET fetches many files at once and decompression is a
//! sequential streaming operation.
//!
//! Layout under `<snapshot_id>/`:
//!
//! ```text
//!   manifest.json          index: every file -> (part, size, offset, mode, mtime)
//!   parts/0000.tar.zst     tar of the files assigned to part 0, zstd compressed
//!   parts/0001.tar.zst     ...
//! ```
//!
//! Files are grouped into a part until `PART_TARGET_BYTES` of raw data is
//! reached; a file larger than that gets a part to itself. Because
//! `manifest.parts[i]` records the raw size of part `i`, a single file can be
//! recovered with a ranged GET of the *compressed* span of the part followed by
//! a bounded streaming decompress that stops as soon as the file's bytes have
//! been seen. That keeps re-downloading one asset cheap on a v2 snapshot.

use aws_sdk_s3::{Client, primitives::ByteStream};
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::path::{Path, PathBuf};

pub const MANIFEST_KEY: &str = "manifest.json";
pub const PARTS_PREFIX: &str = "parts/";
/// Target amount of *uncompressed* tar data per part.
pub const PART_TARGET_BYTES: u64 = 8 * 1024 * 1024;
/// Bump when the manifest/part layout changes incompatibly.
pub const MANIFEST_VERSION: u32 = 2;
/// zstd level for part compression. Level 3 is the sweet spot: it decodes at
/// multiple GB/s, so it never shows up in restore profiles.
const ZSTD_LEVEL: i32 = 3;

#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub snapshot: String,
    /// Uncompressed byte length of each part, indexed by part number.
    pub parts: Vec<u64>,
    pub files: Vec<FileRecord>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileRecord {
    /// Path relative to the restore root, always with `/` separators.
    pub path: String,
    /// Index into `Manifest::parts`.
    pub part: usize,
    pub size: u64,
    /// Virtual offset of the file's content inside the uncompressed part tar.
    pub offset: u64,
    pub unix_mode: u32,
    pub mtime: u64,
}

impl Manifest {
    pub fn key_for(&self, part: usize) -> String {
        part_key(part)
    }

    /// Uncompressed (tar) length of a part.
    #[cfg(test)]
    pub fn raw_len(&self, part: usize) -> Option<u64> {
        self.parts.get(part).copied()
    }

    /// Files belonging to a part, in the order they appear in its tar stream.
    pub fn files_of_part(&self, part: usize) -> Vec<&FileRecord> {
        let mut files: Vec<&FileRecord> = self.files.iter().filter(|f| f.part == part).collect();
        files.sort_by_key(|f| f.offset);
        files
    }
}

pub fn part_key(part: usize) -> String {
    format!("{PARTS_PREFIX}{part:04}.tar.zst")
}

pub fn manifest_key(snapshot: &str) -> String {
    format!("{snapshot}/{MANIFEST_KEY}")
}

/// A local file that is about to go into a snapshot.
pub struct SourceFile {
    pub abs_path: PathBuf,
    /// Path relative to the snapshot root, `/` separated.
    pub rel_path: String,
    pub size: u64,
    pub unix_mode: u32,
    pub mtime: u64,
}

/// Collect the files of `Assets/Unlicensed/` plus `Assets/Unlicensed.meta`,
/// sorted so that a snapshot is byte-for-byte reproducible.
///
/// Keys are rooted at `Assets/`, matching the bucket's existing layout, so a
/// restore recreates `Assets/Unlicensed/...` rather than dropping the leading
/// directory.
pub fn collect_sources(root: &Path, extra: &[PathBuf]) -> anyhow::Result<Vec<SourceFile>> {
    let mut sources = Vec::new();

    // Normalize first: `backup` passes "Assets/Unlicensed/" with a trailing
    // separator, and Rust keeps that separator in `Path::parent`.
    let root: PathBuf = root.components().collect();
    // Keys start one level above the root's parent, so both the root's own
    // directory name ("Unlicensed") and its parent ("Assets") are kept.
    let base = root
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .unwrap_or_default();

    // Resolve both against the current directory. `backup` passes relative
    // paths, and a relative `base` would compare against absolute walk entries
    // and strip nothing, silently dropping the leading `Assets/`. This is the
    // same directory the files were read from, so it stays consistent.
    let abs_root = absolute(&root)?;
    let abs_base = absolute(&base)?;

    for entry in walkdir::WalkDir::new(&abs_root) {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        sources.push(source_file(entry.path(), &abs_base, entry.path())?);
    }

    // `extra` files sit beside the root ("Assets/Unlicensed.meta"), outside the
    // walked tree, so their key is the root's directory name plus the file name.
    let root_name = abs_root
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("snapshot root {} has no directory name", root.display()))?;
    for path in extra {
        let abs_path = absolute(path)?;
        if !abs_path.is_file() {
            continue;
        }
        let name = abs_path
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("metadata path {} has no file name", path.display()))?;
        let relative = Path::new(root_name).join(name);
        sources.push(source_file(&abs_path, &abs_base, &relative)?);
    }

    sources.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    Ok(sources)
}

/// Make a path absolute against the current directory without touching the
/// filesystem, so relative and absolute inputs yield keys the same way.
///
/// An empty path means "the current directory", which `std::path::absolute`
/// rejects; resolve it to `.` instead.
fn absolute(path: &Path) -> anyhow::Result<PathBuf> {
    let path = if path.as_os_str().is_empty() {
        Path::new(".")
    } else {
        path
    };
    std::path::absolute(path).map_err(|e| anyhow::anyhow!("cannot resolve {}: {e}", path.display()))
}

/// Build a source entry. `abs` is the file on disk, `base` the directory keys
/// are relative to, and `relative` the key to use when `abs` is outside `base`.
fn source_file(abs: &Path, base: &Path, relative: &Path) -> anyhow::Result<SourceFile> {
    let metadata = std::fs::metadata(abs)?;

    // A failed strip would silently fall back to the local absolute path and
    // leak machine-specific directories into object keys, so surface it.
    let key = if base.as_os_str().is_empty() {
        relative
    } else {
        abs.strip_prefix(base).map_err(|_| {
            anyhow::anyhow!(
                "path {} is not inside the snapshot root {}",
                abs.display(),
                base.display()
            )
        })?
    };

    Ok(SourceFile {
        abs_path: abs.to_path_buf(),
        rel_path: to_key_path(key),
        size: metadata.len(),
        unix_mode: unix_mode(&metadata),
        mtime: mtime_secs(&metadata),
    })
}

#[cfg(unix)]
fn unix_mode(metadata: &std::fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode()
}

#[cfg(not(unix))]
fn unix_mode(_metadata: &std::fs::Metadata) -> u32 {
    0o644
}

fn mtime_secs(metadata: &std::fs::Metadata) -> u64 {
    metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Normalize a relative path into a `/`-separated object-key component.
pub fn to_key_path(path: &Path) -> String {
    path.components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

/// Assign files to parts. Greedy in the deterministic (sorted) file order.
///
/// A file larger than the target gets a part of its own so that recovering any
/// other file never requires decompressing past it.
pub fn plan_parts(files: &[SourceFile]) -> Vec<Vec<usize>> {
    let mut parts: Vec<Vec<usize>> = Vec::new();
    let mut current: Vec<usize> = Vec::new();
    let mut current_bytes = 0u64;

    for (idx, file) in files.iter().enumerate() {
        let oversize = file.size > PART_TARGET_BYTES;

        // Flush first, so the size check decides which part this file *starts*
        // rather than leaving an oversized file attached to its predecessor.
        if oversize || (!current.is_empty() && current_bytes + file.size > PART_TARGET_BYTES) {
            if !current.is_empty() {
                parts.push(std::mem::take(&mut current));
            }
            current_bytes = 0;
        }

        current.push(idx);
        current_bytes += file.size;

        if oversize {
            parts.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
    }

    if !current.is_empty() {
        parts.push(current);
    }

    parts
}

/// Where a file's content landed inside its part: `(index into the source
/// list, byte offset of the content within the uncompressed part tar)`.
pub type PartOffsets = Vec<(usize, u64)>;

/// Build one part: a tar stream of `members` compressed as a single zstd frame.
///
/// Returns the compressed bytes, the uncompressed length, and each member's
/// virtual offset within the uncompressed tar.
pub fn build_part(
    files: &[SourceFile],
    members: &[usize],
) -> anyhow::Result<(Vec<u8>, u64, PartOffsets)> {
    let mut builder = tar::Builder::new(Vec::with_capacity(PART_TARGET_BYTES as usize));
    let mut offsets = Vec::with_capacity(members.len());

    for &idx in members {
        let file = &files[idx];
        let mut header = tar::Header::new_gnu();
        header.set_size(file.size);
        header.set_mode(file.unix_mode & 0o7777);
        header.set_mtime(file.mtime);
        header.set_entry_type(tar::EntryType::Regular);
        header.set_cksum();

        // Offset of this file's content: where the tar stream is *after* the
        // 512-byte header that `append_data` is about to write, plus the
        // block padding the builder applies for the previous entry.
        let start = builder.get_ref().len() as u64;
        let content_offset = start + 512;

        let mut handle = std::fs::File::open(&file.abs_path)?;
        builder.append_data(&mut header, &file.rel_path, &mut handle)?;
        offsets.push((idx, content_offset));
    }

    builder.finish()?;
    let raw = builder.into_inner()?;
    let raw_len = raw.len() as u64;

    let compressed = zstd::bulk::compress(&raw, ZSTD_LEVEL)?;
    Ok((compressed, raw_len, offsets))
}

/// Upload a packed snapshot and return its manifest.
pub async fn upload_part(
    client: &Client,
    bucket: &str,
    snapshot: &str,
    part: usize,
    compressed: Vec<u8>,
) -> anyhow::Result<()> {
    client
        .put_object()
        .bucket(bucket)
        .key(format!("{snapshot}/{}", part_key(part)))
        .body(ByteStream::from(compressed))
        .send()
        .await?;
    Ok(())
}

pub async fn upload_manifest(
    client: &Client,
    bucket: &str,
    manifest: &Manifest,
) -> anyhow::Result<()> {
    let body = serde_json::to_vec(manifest)?;
    client
        .put_object()
        .bucket(bucket)
        .key(manifest_key(&manifest.snapshot))
        .body(ByteStream::from(body))
        .send()
        .await?;
    Ok(())
}

/// Fetch a snapshot manifest, or `None` if the snapshot predates the v2 format.
pub async fn fetch_manifest(
    client: &Client,
    bucket: &str,
    snapshot: &str,
) -> anyhow::Result<Option<Manifest>> {
    let resp = match client
        .get_object()
        .bucket(bucket)
        .key(manifest_key(snapshot))
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(err) => {
            // A missing manifest is the legacy case, not an error.
            if err
                .as_service_error()
                .map(|e| e.is_no_such_key())
                .unwrap_or(false)
            {
                return Ok(None);
            }
            return Err(err.into());
        }
    };

    let bytes = resp.body.collect().await?.into_bytes();
    let manifest: Manifest = serde_json::from_slice(&bytes)?;
    if manifest.version != MANIFEST_VERSION {
        anyhow::bail!(
            "snapshot {snapshot} uses manifest version {}, this build understands {MANIFEST_VERSION}",
            manifest.version
        );
    }
    Ok(Some(manifest))
}

/// Decompress an uncompressed part tar. `raw` must be a complete part.
///
/// Streams rather than using the one-shot bulk API: a part's raw length is only
/// bounded by whichever oversized file it holds, so guessing a capacity here
/// would fail on exactly the largest assets.
pub fn decode_part(compressed: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut decoder = zstd::stream::read::Decoder::new(compressed)?;
    let mut raw = Vec::new();
    decoder.read_to_end(&mut raw)?;
    Ok(raw)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("bsubp-test-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn source(root: &Path, rel: &str, contents: &[u8]) -> SourceFile {
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

    /// The whole point of the format: a part can be decompressed and any single
    /// member recovered from it, byte-for-byte.
    #[test]
    fn part_round_trip_restores_members() {
        let dir = temp_dir("roundtrip");
        let files = vec![
            source(&dir, "a.bin", &[7u8; 4096]),
            source(&dir, "nested/b.bin", b"hello banana shooter"),
            source(&dir, "c.bin", &[19u8; 1]),
        ];

        let members: Vec<usize> = (0..files.len()).collect();
        let (compressed, raw_len, offsets) = build_part(&files, &members).unwrap();
        assert!(raw_len > 0);
        assert_eq!(offsets.len(), files.len());

        let raw = decode_part(&compressed).unwrap();
        assert_eq!(raw.len() as u64, raw_len);

        // Recover each file exactly the way a ranged restore does.
        for (file, (_, content_offset)) in files.iter().zip(&offsets) {
            let start = *content_offset as usize;
            let end = start + file.size as usize;
            let extracted = &raw[start..end];
            let expected = std::fs::read(&file.abs_path).unwrap();
            assert_eq!(
                extracted,
                expected.as_slice(),
                "mismatch for {}",
                file.rel_path
            );
        }

        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Files larger than the target still must land in a part of their own and
    /// come back intact, since that path bypasses the "fits in one part" case.
    #[test]
    fn oversized_file_gets_its_own_part() {
        let dir = temp_dir("oversized");
        let big = vec![3u8; (PART_TARGET_BYTES + 1024) as usize];
        let files = vec![
            source(&dir, "small.bin", b"small"),
            source(&dir, "big.bin", &big),
        ];

        let plan = plan_parts(&files);
        assert_eq!(plan.len(), 2, "big file must be isolated");
        // Sorted order puts "big.bin" first, so the small file's part is
        // flushed ahead of it and the oversized file gets the second part to
        // itself. Either way it shares a part with nothing.
        assert_eq!(plan[0], vec![0]);
        assert_eq!(plan[1].len(), 1);
        assert_eq!(files[plan[1][0]].rel_path, "big.bin");

        let (compressed, _, offsets) = build_part(&files, &plan[1]).unwrap();
        let raw = decode_part(&compressed).unwrap();
        let (idx, content_offset) = offsets[0];
        assert_eq!(idx, 1);
        let start = content_offset as usize;
        assert_eq!(&raw[start..start + big.len()], big.as_slice());

        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Grouping must respect the target size and never split a file.
    #[test]
    fn plan_groups_by_target_size() {
        let dir = temp_dir("plan");
        let chunk = vec![0u8; (PART_TARGET_BYTES / 2) as usize];
        let files = vec![
            source(&dir, "one.bin", &chunk),
            source(&dir, "two.bin", &chunk),
            source(&dir, "three.bin", b"tail"),
        ];

        let plan = plan_parts(&files);
        // 0.5 + 0.5 target-sized files fit together, the tail starts a new part.
        assert_eq!(plan.len(), 2);
        assert_eq!(plan[0], vec![0, 1]);
        assert_eq!(plan[1], vec![2]);

        // Every file appears exactly once.
        let mut seen: Vec<usize> = plan.iter().flatten().copied().collect();
        seen.sort_unstable();
        assert_eq!(seen, vec![0, 1, 2]);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The decoder must handle a part bigger than a fixed buffer would allow;
    /// a snapshot can contain a single file larger than the part target.
    #[test]
    fn decode_handles_parts_larger_than_the_target() {
        let dir = temp_dir("huge");
        // Highly compressible, and 4x the old hard-coded 32 MiB capacity.
        let payload = vec![0u8; (PART_TARGET_BYTES * 4) as usize];
        let files = vec![source(&dir, "huge.bin", &payload)];

        let plan = plan_parts(&files);
        assert_eq!(plan.len(), 1, "a lone oversized file still gets one part");

        let (compressed, raw_len, offsets) = build_part(&files, &plan[0]).unwrap();
        // Header plus content, ignoring tar's block padding.
        assert!(raw_len >= payload.len() as u64 + 512);

        let raw = decode_part(&compressed).unwrap();
        assert_eq!(raw.len() as u64, raw_len);
        let start = offsets[0].1 as usize;
        assert_eq!(&raw[start..start + payload.len()], payload.as_slice());

        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The snapshot must be rooted at `Assets/`: the bucket layout has always
    /// used `Assets/Unlicensed/...` keys, and a restore has to reproduce that
    /// tree rather than dropping the `Assets` component.
    #[test]
    fn collected_paths_keep_the_assets_prefix() {
        let dir = temp_dir("collect");
        let root = dir.join("Assets").join("Unlicensed");
        std::fs::create_dir_all(root.join("Nested")).unwrap();
        for rel in ["One.png", "Nested/Two.png"] {
            let abs = root.join(rel);
            std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
            std::fs::write(&abs, b"x").unwrap();
        }
        let meta = dir.join("Assets").join("Unlicensed.meta");
        std::fs::write(&meta, b"meta").unwrap();

        // Use the same trailing-slash form as `backup::run`, which is what
        // originally broke path rooting.
        let root = format!("{}/", root.display());
        let files = collect_sources(Path::new(&root), std::slice::from_ref(&meta)).unwrap();
        let mut paths: Vec<&str> = files.iter().map(|f| f.rel_path.as_str()).collect();
        paths.sort_unstable();

        assert_eq!(
            paths,
            vec![
                "Assets/Unlicensed.meta",
                "Assets/Unlicensed/Nested/Two.png",
                "Assets/Unlicensed/One.png",
            ],
            "keys must be rooted at Assets/ and use forward slashes"
        );

        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The backup calls `collect_sources` with *relative* paths exactly like
    /// these, which is what previously broke the metadata key: the extra file
    /// is not under the walk root, so its key is composed explicitly and must
    /// match the root's own directory.
    ///
    /// Re-executes this test binary in a scratch working directory, because
    /// `set_current_dir` is process-wide and the test harness runs tests
    /// concurrently.
    #[test]
    fn relative_inputs_keep_the_metadata_beside_the_asset_root() {
        const TAG: &str = "BS_UBP_RELATIVE_INPUTS";
        /// Marks the child's result lines so the harness's own output on the
        /// same stream cannot be mistaken for a key.
        const KEY_PREFIX: &str = "KEY ";

        // Child half: run from the project root layout with relative inputs.
        if std::env::var_os(TAG).is_some() {
            let files = collect_sources(
                Path::new("Assets/Unlicensed/"),
                &[PathBuf::from("Assets/Unlicensed.meta")],
            )
            .unwrap();
            let mut paths: Vec<String> = files.iter().map(|f| f.rel_path.clone()).collect();
            paths.sort();
            for path in paths {
                println!("{KEY_PREFIX}{path}");
            }
            return;
        }

        // Parent half: build the layout and run the child inside it.
        let dir = temp_dir("relative");
        std::fs::create_dir_all(dir.join("Assets").join("Unlicensed")).unwrap();
        std::fs::write(dir.join("Assets").join("Unlicensed.meta"), b"meta").unwrap();
        std::fs::write(
            dir.join("Assets").join("Unlicensed").join("One.png"),
            b"one",
        )
        .unwrap();

        let exe = std::env::current_exe().unwrap();
        let output = std::process::Command::new(exe)
            .args([
                "relative_inputs_keep_the_metadata_beside_the_asset_root",
                "--nocapture",
            ])
            .current_dir(&dir)
            .env(TAG, "1")
            .output()
            .unwrap();
        assert!(output.status.success(), "child run failed");

        // Both streams are read because the harness may route either.
        let stdout = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let mut paths: Vec<&str> = stdout
            .lines()
            .filter_map(|line| line.trim().strip_prefix(KEY_PREFIX))
            .collect();
        paths.sort_unstable();

        assert_eq!(
            paths,
            vec!["Assets/Unlicensed.meta", "Assets/Unlicensed/One.png"],
            "the metadata file must sit beside the asset root, not inside it"
        );

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn manifest_round_trips_through_json() {
        let manifest = Manifest {
            version: MANIFEST_VERSION,
            snapshot: "2024-01-01T00-00-00Z".to_string(),
            parts: vec![1234],
            files: vec![FileRecord {
                path: "Unlicensed/Foo.png".to_string(),
                part: 0,
                size: 10,
                offset: 512,
                unix_mode: 0o644,
                mtime: 1_700_000_000,
            }],
        };

        let encoded = serde_json::to_vec(&manifest).unwrap();
        let decoded: Manifest = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded.files.len(), 1);
        assert_eq!(decoded.files[0].path, "Unlicensed/Foo.png");
        assert_eq!(decoded.raw_len(0), Some(1234));
        assert!(decoded.raw_len(9).is_none());
    }
}
