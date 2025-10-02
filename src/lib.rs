//! fake iroh-blobs store for testing.
//!
//! generates data on-the-fly, stores nothing. serves deterministic data (zeros or
//! pseudo-random) at specific lengths for testing big blob transfers.
//!
//! # Examples
//!
//! ```
//! use iroh_fake_store::FakeStore;
//!
//! # tokio_test::block_on(async {
//! let store = FakeStore::builder()
//!     .with_blob(1024)           // 1KB blob
//!     .with_blob(1024 * 1024)    // 1MB blob
//!     .build();
//!
//! let hashes = store.blobs().list().hashes().await.unwrap();
//! assert_eq!(hashes.len(), 2);
//! # });
//! ```
//!
//! generates on-the-fly without storing, deterministic hashes, configurable patterns,
//! has safety limits to prevent accidentally making huge blobs

use std::{
    collections::{BTreeMap, HashMap},
    io::{self, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use bao_tree::{
    BaoTree, ChunkRanges,
    io::{
        Leaf,
        mixed::{EncodedItem, ReadBytesAt, traverse_ranges_validated},
        outboard::PreOrderMemOutboard,
        sync::ReadAt,
    },
};
use bytes::Bytes;
use iroh_blobs::{
    BlobFormat, Hash, HashAndFormat,
    api::{
        self, Store, TempTag,
        blobs::{Bitfield, ExportProgressItem},
        proto::{
            self, BlobStatus, Command, ExportBaoMsg, ExportBaoRequest, ExportPathMsg,
            ExportPathRequest, ExportRangesItem, ExportRangesMsg, ExportRangesRequest,
            ImportBaoMsg, ImportByteStreamMsg, ImportBytesMsg, ObserveMsg, ObserveRequest,
            WaitIdleMsg,
        },
    },
    protocol::ChunkRangesExt,
    store::IROH_BLOCK_SIZE,
};
use irpc::channel::mpsc;
use range_collections::range_set::RangeSetRange;
use ref_cast::RefCast;
use tokio::task::{JoinError, JoinSet};

/// data generation strategy for fake blobs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataStrategy {
    /// all zeros (default, most efficient)
    Zeros,
    /// all ones
    Ones,
    /// deterministic pseudo-random based on seed
    PseudoRandom { seed: u64 },
}

impl Default for DataStrategy {
    fn default() -> Self {
        Self::Zeros
    }
}

#[derive(Debug, Clone)]
pub struct FakeStoreConfig {
    pub strategy: DataStrategy,
    /// max blob size (prevents accidents)
    pub max_blob_size: Option<u64>,
}

impl Default for FakeStoreConfig {
    fn default() -> Self {
        Self {
            strategy: DataStrategy::Zeros,
            // 10GB limit to prevent accidents
            max_blob_size: Some(10 * 1024 * 1024 * 1024),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct FakeStoreBuilder {
    config: FakeStoreConfig,
    sizes: Vec<u64>,
}

impl FakeStoreBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn strategy(mut self, strategy: DataStrategy) -> Self {
        self.config.strategy = strategy;
        self
    }

    /// None for unlimited
    pub fn max_blob_size(mut self, max: Option<u64>) -> Self {
        self.config.max_blob_size = max;
        self
    }

    pub fn with_blob(mut self, size: u64) -> Self {
        self.sizes.push(size);
        self
    }

    pub fn with_blobs(mut self, sizes: impl IntoIterator<Item = u64>) -> Self {
        self.sizes.extend(sizes);
        self
    }

    /// # Panics
    /// panics if any blob size exceeds the configured max
    pub fn build(self) -> FakeStore {
        if let Some(max) = self.config.max_blob_size {
            for &size in &self.sizes {
                assert!(size <= max, "Blob size {} exceeds maximum {}", size, max);
            }
        }

        FakeStore::new_with_config(self.sizes, self.config)
    }
}

/// fake iroh-blobs store for testing
///
/// generates data on-the-fly, stores nothing. for testing big blobs when you don't
/// care about content.
///
/// # Examples
///
/// ```
/// use iroh_fake_store::{FakeStore, DataStrategy};
///
/// # tokio_test::block_on(async {
/// let store = FakeStore::new([1024, 2048]);
///
/// let store = FakeStore::builder()
///     .strategy(DataStrategy::Zeros)
///     .max_blob_size(Some(1024 * 1024 * 100)) // 100MB max
///     .with_blob(1024)
///     .build();
/// # });
/// ```
#[derive(Debug, Clone)]
pub struct FakeStore {
    store: Store,
}

impl std::ops::Deref for FakeStore {
    type Target = Store;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

/// wrapper around mpsc::Sender<EncodedItem> that impls bao_tree::io::mixed::Sender
#[derive(RefCast)]
#[repr(transparent)]
struct BaoTreeSender(mpsc::Sender<EncodedItem>);

impl bao_tree::io::mixed::Sender for BaoTreeSender {
    type Error = irpc::channel::SendError;
    async fn send(&mut self, item: EncodedItem) -> std::result::Result<(), Self::Error> {
        self.0.send(item).await
    }
}

#[derive(Debug, Clone)]
struct BlobMetadata {
    size: u64,
    outboard: Bytes,
    strategy: DataStrategy,
}

struct Actor {
    commands: tokio::sync::mpsc::Receiver<proto::Command>,
    tasks: JoinSet<()>,
    idle_waiters: Vec<irpc::channel::oneshot::Sender<()>>,
    blobs: Arc<Mutex<HashMap<Hash, BlobMetadata>>>,
    strategy: DataStrategy,
    tags: BTreeMap<api::Tag, HashAndFormat>,
}

impl Actor {
    fn new(
        commands: tokio::sync::mpsc::Receiver<proto::Command>,
        blobs: HashMap<Hash, BlobMetadata>,
        strategy: DataStrategy,
    ) -> Self {
        Self {
            blobs: Arc::new(Mutex::new(blobs)),
            commands,
            tasks: JoinSet::new(),
            idle_waiters: Vec::new(),
            strategy,
            tags: BTreeMap::new(),
        }
    }

    async fn handle_command(&mut self, cmd: Command) -> Option<irpc::channel::oneshot::Sender<()>> {
        match cmd {
            Command::ImportBao(msg) => {
                self.handle_import_bao(msg).await;
            }
            Command::WaitIdle(WaitIdleMsg { tx, .. }) => {
                if self.tasks.is_empty() {
                    tx.send(()).await.ok();
                } else {
                    self.idle_waiters.push(tx);
                }
            }
            Command::ImportBytes(msg) => {
                self.handle_import_bytes(msg).await;
            }
            Command::ImportByteStream(msg) => {
                self.handle_import_byte_stream(msg).await;
            }
            Command::ImportPath(msg) => {
                msg.tx
                    .send(io::Error::other("import path not supported").into())
                    .await
                    .ok();
            }
            Command::Observe(ObserveMsg {
                inner: ObserveRequest { hash },
                tx,
                ..
            }) => {
                let size = self.blobs.lock().unwrap().get(&hash).map(|x| x.size);
                self.tasks.spawn(async move {
                    if let Some(size) = size {
                        tx.send(Bitfield::complete(size)).await.ok();
                    } else {
                        tx.send(Bitfield::empty()).await.ok();
                    };
                });
            }
            Command::ExportBao(ExportBaoMsg {
                inner: ExportBaoRequest { hash, ranges, .. },
                tx,
                ..
            }) => {
                let metadata = self.blobs.lock().unwrap().get(&hash).cloned();
                self.tasks.spawn(export_bao(hash, metadata, ranges, tx));
            }
            Command::ExportPath(ExportPathMsg {
                inner: ExportPathRequest { hash, target, .. },
                tx,
                ..
            }) => {
                let metadata = self.blobs.lock().unwrap().get(&hash).cloned();
                self.tasks.spawn(export_path(metadata, target, tx));
            }
            Command::Batch(_cmd) => {}
            Command::ClearProtected(cmd) => {
                cmd.tx.send(Ok(())).await.ok();
            }
            Command::CreateTag(cmd) => {
                use api::proto::CreateTagRequest;
                use std::time::SystemTime;

                let CreateTagRequest { value } = cmd.inner;
                let tag = api::Tag::auto(SystemTime::now(), |t| self.tags.contains_key(t));
                self.tags.insert(tag.clone(), value);
                cmd.tx.send(Ok(tag)).await.ok();
            }
            Command::CreateTempTag(cmd) => {
                cmd.tx.send(TempTag::new(cmd.inner.value, None)).await.ok();
            }
            Command::RenameTag(cmd) => {
                cmd.tx
                    .send(Err(io::Error::other("rename tag not supported").into()))
                    .await
                    .ok();
            }
            Command::DeleteTags(cmd) => {
                cmd.tx
                    .send(Err(io::Error::other("delete tags not supported").into()))
                    .await
                    .ok();
            }
            Command::DeleteBlobs(cmd) => {
                cmd.tx
                    .send(Err(io::Error::other("delete blobs not supported").into()))
                    .await
                    .ok();
            }
            Command::ListBlobs(cmd) => {
                let hashes: Vec<Hash> = self.blobs.lock().unwrap().keys().cloned().collect();
                self.tasks.spawn(async move {
                    for hash in hashes {
                        cmd.tx.send(Ok(hash)).await.ok();
                    }
                });
            }
            Command::BlobStatus(cmd) => {
                let hash = cmd.inner.hash;
                let metadata = self.blobs.lock().unwrap().get(&hash).cloned();
                let status = if let Some(metadata) = metadata {
                    BlobStatus::Complete {
                        size: metadata.size,
                    }
                } else {
                    BlobStatus::NotFound
                };
                cmd.tx.send(status).await.ok();
            }
            Command::ListTags(cmd) => {
                cmd.tx.send(Vec::new()).await.ok();
            }
            Command::SetTag(cmd) => {
                cmd.tx
                    .send(Err(io::Error::other("set tag not supported").into()))
                    .await
                    .ok();
            }
            Command::ListTempTags(cmd) => {
                cmd.tx.send(Vec::new()).await.ok();
            }
            Command::SyncDb(cmd) => {
                cmd.tx.send(Ok(())).await.ok();
            }
            Command::Shutdown(cmd) => {
                return Some(cmd.tx);
            }
            Command::ExportRanges(cmd) => {
                let metadata = self.blobs.lock().unwrap().get(&cmd.inner.hash).cloned();
                self.tasks.spawn(export_ranges(cmd, metadata));
            }
        }
        None
    }

    fn log_unit_task(&self, res: Result<(), JoinError>) {
        if let Err(e) = res {
            eprintln!("task failed: {e}");
        }
    }

    async fn run(mut self) {
        loop {
            tokio::select! {
                Some(cmd) = self.commands.recv() => {
                    if let Some(shutdown) = self.handle_command(cmd).await {
                        shutdown.send(()).await.ok();
                        break;
                    }
                },
                Some(res) = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    self.log_unit_task(res);
                    if self.tasks.is_empty() {
                        for tx in self.idle_waiters.drain(..) {
                            tx.send(()).await.ok();
                        }
                    }
                },
                else => break,
            }
        }
    }

    async fn handle_import_bytes(&mut self, msg: ImportBytesMsg) {
        use bao_tree::io::outboard::PreOrderMemOutboard;
        use iroh_blobs::api::blobs::AddProgressItem;

        let ImportBytesMsg {
            inner: proto::ImportBytesRequest { data, .. },
            tx,
            ..
        } = msg;

        let size = data.len() as u64;

        if tx.send(AddProgressItem::Size(size)).await.is_err() {
            return;
        }
        if tx.send(AddProgressItem::CopyDone).await.is_err() {
            return;
        }

        // compute hash from the actual data
        let outboard = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
        let hash = Hash::from(outboard.root);

        // store metadata (we don't store the actual data, just remember the size)
        self.blobs.lock().unwrap().insert(
            hash,
            BlobMetadata {
                size,
                outboard: outboard.data.into(),
                strategy: self.strategy,
            },
        );

        // send completion with the hash
        let temp_tag = api::TempTag::new(
            HashAndFormat {
                hash,
                format: BlobFormat::Raw,
            },
            None,
        );
        if tx.send(AddProgressItem::Done(temp_tag)).await.is_err() {}
    }

    async fn handle_import_byte_stream(&mut self, msg: ImportByteStreamMsg) {
        use bao_tree::io::outboard::PreOrderMemOutboard;
        use iroh_blobs::api::blobs::AddProgressItem;
        use proto::ImportByteStreamUpdate;

        let ImportByteStreamMsg { tx, mut rx, .. } = msg;

        // collect all bytes to compute hash
        let mut data = Vec::new();
        loop {
            match rx.recv().await {
                Ok(Some(ImportByteStreamUpdate::Bytes(chunk))) => {
                    data.extend_from_slice(&chunk);
                    if tx
                        .send(AddProgressItem::CopyProgress(data.len() as u64))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Ok(Some(ImportByteStreamUpdate::Done)) => {
                    break;
                }
                Ok(None) | Err(_) => {
                    tx.send(AddProgressItem::Error(io::Error::other(
                        "stream ended unexpectedly",
                    )))
                    .await
                    .ok();
                    return;
                }
            }
        }

        let size = data.len() as u64;

        if tx.send(AddProgressItem::CopyDone).await.is_err() {
            return;
        }

        // compute hash
        let outboard = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
        let hash = Hash::from(outboard.root);

        // store metadata
        self.blobs.lock().unwrap().insert(
            hash,
            BlobMetadata {
                size,
                outboard: outboard.data.into(),
                strategy: self.strategy,
            },
        );

        // send completion
        let temp_tag = api::TempTag::new(
            HashAndFormat {
                hash,
                format: BlobFormat::Raw,
            },
            None,
        );
        if tx.send(AddProgressItem::Done(temp_tag)).await.is_err() {}
    }

    async fn handle_import_bao(&mut self, msg: ImportBaoMsg) {
        use bao_tree::io::outboard::PreOrderMemOutboard;
        use proto::ImportBaoRequest;

        let ImportBaoMsg {
            inner: ImportBaoRequest { hash, size },
            tx,
            mut rx,
            ..
        } = msg;

        let size_u64 = size.get();
        let strategy = self.strategy;
        let blobs = self.blobs.clone();

        self.tasks.spawn(async move {
            // consume all incoming chunks
            while let Ok(Some(_item)) = rx.recv().await {
                // just drain them, don't store
            }

            // once all chunks consumed, generate fake data to compute outboard
            let data = generate_data_for_strategy(size_u64, strategy);
            let outboard = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);

            // store metadata
            blobs.lock().unwrap().insert(
                hash,
                BlobMetadata {
                    size: size_u64,
                    outboard: outboard.data.into(),
                    strategy,
                },
            );

            tx.send(Ok(())).await.ok();
        });
    }
}

/// generates data on-the-fly based on strategy and offset
#[derive(Clone)]
struct DataReader {
    strategy: DataStrategy,
}

impl DataReader {
    fn new(strategy: DataStrategy) -> Self {
        Self { strategy }
    }

    fn byte_at(&self, offset: u64) -> u8 {
        match self.strategy {
            DataStrategy::Zeros => 0,
            DataStrategy::Ones => 0xFF,
            DataStrategy::PseudoRandom { seed } => {
                // offset-based hashing for independent byte generation
                // simpler than LCG jump-ahead, still deterministic
                // simple mixing function (SplitMix64-inspired)
                let mut x = seed.wrapping_add(offset);
                x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
                x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
                x = x ^ (x >> 31);
                (x >> 24) as u8
            }
        }
    }
}

impl ReadAt for DataReader {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        for (i, byte) in buf.iter_mut().enumerate() {
            *byte = self.byte_at(offset + i as u64);
        }
        Ok(buf.len())
    }
}

impl ReadBytesAt for DataReader {
    fn read_bytes_at(&self, offset: u64, size: usize) -> io::Result<Bytes> {
        let mut data = vec![0u8; size];
        self.read_at(offset, &mut data)?;
        Ok(Bytes::from(data))
    }
}

fn generate_data_for_strategy(size: u64, strategy: DataStrategy) -> Vec<u8> {
    let reader = DataReader::new(strategy);
    let mut data = vec![0u8; size as usize];
    reader.read_at(0, &mut data).expect("read should succeed");
    data
}

fn compute_hash_for_strategy(size: u64, strategy: DataStrategy) -> Hash {
    let data = generate_data_for_strategy(size, strategy);
    let outboard = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
    Hash::from(outboard.root)
}

async fn export_bao(
    hash: Hash,
    metadata: Option<BlobMetadata>,
    ranges: ChunkRanges,
    mut sender: mpsc::Sender<EncodedItem>,
) {
    let metadata = match metadata {
        Some(metadata) => metadata,
        None => {
            sender
                .send(EncodedItem::Error(bao_tree::io::EncodeError::Io(
                    io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "export task ended unexpectedly",
                    ),
                )))
                .await
                .ok();
            return;
        }
    };

    let size = metadata.size;
    let data = DataReader::new(metadata.strategy);

    let tree = BaoTree::new(size, IROH_BLOCK_SIZE);
    let outboard = PreOrderMemOutboard {
        root: hash.into(),
        tree,
        data: metadata.outboard,
    };

    let sender = BaoTreeSender::ref_cast_mut(&mut sender);
    traverse_ranges_validated(data, outboard, &ranges, sender)
        .await
        .ok();
}

async fn export_ranges(mut cmd: ExportRangesMsg, metadata: Option<BlobMetadata>) {
    let Some(metadata) = metadata else {
        cmd.tx
            .send(ExportRangesItem::Error(api::Error::io(
                io::ErrorKind::NotFound,
                "hash not found",
            )))
            .await
            .ok();
        return;
    };
    if let Err(cause) = export_ranges_impl(cmd.inner, &mut cmd.tx, metadata).await {
        cmd.tx
            .send(ExportRangesItem::Error(cause.into()))
            .await
            .ok();
    }
}

async fn export_ranges_impl(
    cmd: ExportRangesRequest,
    tx: &mut mpsc::Sender<ExportRangesItem>,
    metadata: BlobMetadata,
) -> io::Result<()> {
    let ExportRangesRequest { ranges, .. } = cmd;
    let size = metadata.size;
    let bitfield = Bitfield::complete(size);

    for range in ranges.iter() {
        let range = match range {
            RangeSetRange::Range(range) => size.min(*range.start)..size.min(*range.end),
            RangeSetRange::RangeFrom(range) => size.min(*range.start)..size,
        };
        let requested = ChunkRanges::bytes(range.start..range.end);
        if !bitfield.ranges.is_superset(&requested) {
            return Err(io::Error::other(format!(
                "missing range: {requested:?}, present: {bitfield:?}",
            )));
        }
        let reader = DataReader::new(metadata.strategy);
        let bs = 1024;
        let mut offset = range.start;
        loop {
            let end: u64 = (offset + bs).min(range.end);
            let chunk_size = (end - offset) as usize;
            let data = reader.read_bytes_at(offset, chunk_size)?;
            tx.send(Leaf { offset, data }.into()).await?;
            offset = end;
            if offset >= range.end {
                break;
            }
        }
    }
    Ok(())
}

impl FakeStore {
    /// uses zeros strategy. for more control use [`FakeStore::builder()`]
    pub fn new(sizes: impl IntoIterator<Item = u64>) -> Self {
        Self::builder().with_blobs(sizes).build()
    }

    pub fn builder() -> FakeStoreBuilder {
        FakeStoreBuilder::new()
    }

    fn new_with_config(sizes: impl IntoIterator<Item = u64>, config: FakeStoreConfig) -> Self {
        let mut blobs = HashMap::new();
        for size in sizes {
            let hash = compute_hash_for_strategy(size, config.strategy);
            let data = generate_data_for_strategy(size, config.strategy);
            let outboard_data = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
            blobs.insert(
                hash,
                BlobMetadata {
                    size,
                    outboard: outboard_data.data.into(),
                    strategy: config.strategy,
                },
            );
        }

        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let actor = Actor::new(receiver, blobs, config.strategy);
        tokio::spawn(actor.run());

        // Store is #[repr(transparent)] so we can use RefCast
        let local = irpc::LocalSender::from(sender);
        let client = local.into();

        Self {
            store: Store::ref_cast(&client).clone(),
        }
    }

    pub fn store(&self) -> &Store {
        &self.store
    }
}

async fn export_path(
    metadata: Option<BlobMetadata>,
    target: PathBuf,
    mut tx: mpsc::Sender<ExportProgressItem>,
) {
    let Some(metadata) = metadata else {
        tx.send(api::Error::io(io::ErrorKind::NotFound, "hash not found").into())
            .await
            .ok();
        return;
    };
    match export_path_impl(metadata, target, &mut tx).await {
        Ok(()) => tx.send(ExportProgressItem::Done).await.ok(),
        Err(cause) => tx.send(api::Error::from(cause).into()).await.ok(),
    };
}

async fn export_path_impl(
    metadata: BlobMetadata,
    target: PathBuf,
    tx: &mut mpsc::Sender<ExportProgressItem>,
) -> io::Result<()> {
    let size = metadata.size;
    let mut file = std::fs::File::create(&target)?;
    tx.send(ExportProgressItem::Size(size)).await?;

    let buf = [0u8; 1024 * 64];
    for offset in (0..size).step_by(1024 * 64) {
        let len = std::cmp::min(size - offset, 1024 * 64) as usize;
        let buf = &buf[..len];
        file.write_all(buf)?;
        tx.try_send(ExportProgressItem::CopyProgress(offset))
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
