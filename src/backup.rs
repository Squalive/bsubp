use crate::{
    BUCKET_NAME, Progress,
    pack::{
        self, FileRecord, Manifest, collect_sources, manifest_key, plan_parts, upload_manifest,
        upload_part,
    },
};
use aws_sdk_s3::Client;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Assets live under `Assets/Unlicensed/`; the snapshot is rooted at `Assets/`
/// so file paths and object keys match the original layout.
const UNLICENSED_ROOT: &str = "Assets/Unlicensed";
const UNLICENSED_META: &str = "Assets/Unlicensed.meta";

pub async fn run(client: &Client, progress: &Progress) -> anyhow::Result<()> {
    if !inquire::Confirm::new(
        "Upload a packed snapshot of Assets/Unlicensed/ and Assets/Unlicensed.meta?",
    )
    .with_default(false)
    .with_help_message("This will use your network to upload to cloudflare storage")
    .prompt()?
    {
        return Ok(());
    }

    let snapshot_id = new_snapshot_id();

    let root = Path::new(UNLICENSED_ROOT);
    if !root.is_dir() {
        anyhow::bail!("{UNLICENSED_ROOT} not found; run from the game project root");
    }

    let extra = [PathBuf::from(UNLICENSED_META)];
    let files = Arc::new(collect_sources(root, &extra)?);
    if files.is_empty() {
        anyhow::bail!("nothing to back up under {UNLICENSED_ROOT}");
    }

    let plan = plan_parts(&files);
    let total_raw: u64 = files.iter().map(|f| f.size).sum();

    tracing::info!(
        files = files.len(),
        parts = plan.len(),
        raw_bytes = total_raw,
        "packing snapshot"
    );

    // One part is built and uploaded at a time, so peak memory stays around a
    // single part rather than the whole snapshot.
    let bar = progress.bar_items(plan.len() as u64, "packing and uploading parts");
    let mut records: Vec<FileRecord> = Vec::with_capacity(files.len());
    let mut part_sizes: Vec<u64> = Vec::with_capacity(plan.len());
    let mut uploaded_bytes = 0u64;

    for (part, members) in plan.iter().enumerate() {
        // Packing is synchronous CPU + file I/O, so keep it off the runtime.
        // The file list is shared behind an Arc so the blocking closure can own
        // its handle without copying the list per part.
        let member_count = members.len();
        let files_for_part = Arc::clone(&files);
        let members_for_part = members.clone();
        let (compressed, raw_len, offsets) = tokio::task::spawn_blocking(move || {
            pack::build_part(&files_for_part, &members_for_part)
        })
        .await??;

        // `build_part` reports offsets in tar order, which is the member order.
        for (idx, offset) in offsets {
            let file = &files[idx];
            records.push(FileRecord {
                path: file.rel_path.clone(),
                part,
                size: file.size,
                offset,
                unix_mode: file.unix_mode,
                mtime: file.mtime,
            });
        }

        uploaded_bytes += compressed.len() as u64;
        part_sizes.push(raw_len);

        bar.set_message(format!("part {part:04} ({member_count} files)"));
        upload_part(client, BUCKET_NAME, &snapshot_id, part, compressed).await?;
        bar.inc(1);
    }

    bar.finish_with_message("parts uploaded");

    let manifest = Manifest {
        version: pack::MANIFEST_VERSION,
        snapshot: snapshot_id.clone(),
        parts: part_sizes,
        files: records,
    };
    upload_manifest(client, BUCKET_NAME, &manifest).await?;

    tracing::info!(
        snapshot = %snapshot_id,
        files = manifest.files.len(),
        parts = manifest.parts.len(),
        uploaded_bytes,
        ratio = format!("{:.2}x", ratio(total_raw, uploaded_bytes)),
        manifest = %manifest_key(&snapshot_id),
        "snapshot uploaded"
    );
    Ok(())
}

fn ratio(raw: u64, compressed: u64) -> f64 {
    if compressed == 0 {
        return 0.0;
    }
    raw as f64 / compressed as f64
}

pub fn new_snapshot_id() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H-%M-%SZ").to_string()
}
