use crate::metainfo::MetaInfo;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub(crate) async fn write_block(
    metainfo: &MetaInfo,
    file: &mut File,
    piece_index: u32,
    block_begin: u32,
    block: &[u8],
) -> Result<(), std::io::Error> {
    let piece_offset = metainfo.piece_offset(piece_index);

    file.seek(std::io::SeekFrom::Start(piece_offset + block_begin as u64))
        .await?;

    file.write_all(block).await?;
    file.flush().await?;

    Ok(())
}

pub(crate) async fn read_block(
    metainfo: &MetaInfo,
    file: &mut File,
    piece_index: u32,
    block_begin: u32,
    block_length: u32,
) -> Result<Vec<u8>, std::io::Error> {
    if block_length > metainfo.actual_piece_length(piece_index) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::FileTooLarge,
            "block length must not exceed piece length",
        ));
    }

    let piece_offset = metainfo.piece_offset(piece_index);

    file.seek(std::io::SeekFrom::Start(piece_offset + block_begin as u64))
        .await?;

    let mut block = vec![0u8; block_length.try_into().unwrap()];

    file.read_exact(&mut block).await?;

    Ok(block)
}

pub(crate) async fn verify_piece(
    metainfo: &MetaInfo,
    file: &mut File,
    piece_index: u32,
) -> Result<bool, std::io::Error> {
    let piece_length = metainfo.actual_piece_length(piece_index);
    let piece_offset = metainfo.piece_offset(piece_index);

    file.seek(std::io::SeekFrom::Start(piece_offset)).await?;

    let mut piece = vec![0u8; piece_length as usize];

    file.read_exact(&mut piece).await?;

    if let Some(piece_hash) = metainfo.piece_hash(piece_index) {
        Ok(&crate::hash(&piece) == piece_hash)
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "couldn't find piece hash for index",
        ))
    }
}

async fn verify_local_data() {
    // TODO move this over from the torrent module
}
