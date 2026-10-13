CLI tool for backuping and restoring unlicensed assets for [Banana Shooter](https://github.com/Squalive/banana-shooter)

## Snapshot format

A snapshot is a set of objects under `<snapshot_id>/` in the `bs-private-backup`
bucket. Snapshot IDs look like `2024-05-01T12-30-00Z`.

New backups use the **packed (v2) format**:

```text
<snapshot_id>/manifest.json      index of every file: part, size, offset, mode, mtime
<snapshot_id>/parts/0000.tar.zst tar of a slice of the assets, zstd compressed
<snapshot_id>/parts/0001.tar.zst ...
```

Files are grouped into ~8 MiB tar slices, and each slice is compressed as a
single zstd frame. Restoring therefore costs a handful of large downloads
instead of one HTTP round trip per asset.

## Usage

Set `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, and `R2_SECRET_ACCESS_KEY`, then:

```sh
bsubp backup            # upload a new packed snapshot
bsubp list              # list snapshots
bsubp restore latest -c # restore the newest snapshot (no prompt)
```

Files are restored to their original paths under `Assets/`, so run from the
game project root:

```text
Assets/Unlicensed/...
Assets/Unlicensed.meta
```

`src/pack.rs` derives those keys from the parent *and grandparent* of the
backup root, which is what keeps the leading `Assets/` component, and resolves
the paths before stripping so relative and absolute inputs behave identically.
Both properties are covered by tests (`collected_paths_keep_the_assets_prefix`
and `relative_inputs_keep_the_metadata_beside_the_asset_root`).

## Restore cache

Downloaded parts are cached under `target/bsubp-cache/<snapshot_id>/`. An
interrupted restore resumes without re-fetching completed parts. Delete the
directory to free the space or to force a clean download.

## Progress output in CI

`indicatif` hides progress bars entirely when stderr is not a terminal, which
is exactly what happens under CI log capture (and under `| tee`), so nothing
would appear in the job log. `src/main.rs` therefore picks a mode once at
startup:

- **Terminal**: the usual in-place repaint.
- **Non-terminal**: a `TermLike` target that writes plain, newline-terminated
  lines, throttled to one update every 200 ms so the log stays readable.

Two details there are load-bearing and are covered by tests:

- `indicatif` ends each repaint with `flush`, not `write_line`, on this kind of
  target. Emitting from anywhere else silently produces no output at all.
- The reported width must be comfortably wider than the rendered template.
  `indicatif` pads each line to that width and subtracts, so a width that is
  too small panics inside its draw — and the panic is swallowed, again leaving
  no output.

To see the log lines locally, run with stderr redirected:

```sh
bsubp restore latest -c 2>&1 | tee restore.log
```
