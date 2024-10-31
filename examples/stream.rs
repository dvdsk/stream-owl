// #![recursion_limit = "256"]
use std::error::Error;
use std::io::Read;
use std::num::NonZeroUsize;

use tokio::task;

// 274- The Age of the Algorithm;
const URL1: &str = "https://dts.podtrac.com/redirect.mp3/chrt.fm/track/288D49/stitcher.simplecastaudio.com/3bb687b0-04af-4257-90f1-39eef4e631b6/episodes/c660ce6b-ced1-459f-9535-113c670e83c9/audio/128/default.mp3?aid=rss_feed&awCollectionId=3bb687b0-04af-4257-90f1-39eef4e631b6&awEpisodeId=c660ce6b-ced1-459f-9535-113c670e83c9&feed=BqbsxVfO";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let (mut handle, stream) = stream_owl::StreamBuilder::try_new(URL1)
        .unwrap()
        .with_prefetch(0)
        .to_unlimited_mem()
        .with_fixed_chunk_size(NonZeroUsize::new(10_000).unwrap())
        .start()
        .await
        .unwrap();

    task::spawn(stream);

    let mut reader = handle
        .try_get_reader()
        .expect("reader has not yet been taken");

    let _ = task::spawn_blocking(move || {
        let mut buf = vec![0u8; 100_000];
        reader.read(&mut buf).unwrap();
        assert!(buf.iter().filter(|i| **i == 0).count() < 100);
    });

    Ok(())
}
