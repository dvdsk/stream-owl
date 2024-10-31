use crate::manager::stream::StreamConfig;
use crate::manager::task::bandwidth;
use crate::network::BandwidthAllowed;
use crate::StreamId;

use super::{Controller, LimitBandwidthById};

struct HttpClientMock;
impl LimitBandwidthById for HttpClientMock {
    async fn limit_bandwidth(&self, id: StreamId, limit: crate::BandwidthLimit) {
        dbg!(id, limit);
    }
}

#[tokio::test]
async fn unlimited_upstream_and_bw() {
    let (mut controller, _cb) = Controller::new(|_, _| (), BandwidthAllowed::UnLimited);

    let id1 = StreamId::new();
    let config = StreamConfig::default();
    let limiter = HttpClientMock;
    dbg!(&config);
    let (_, _guard) = controller.register(id1, config.clone(), &limiter).await;
    let id2 = StreamId::new();
    let (_, _guard) = controller.register(id2, config, &limiter).await;

    controller
        .handle_update(&limiter, bandwidth::Update::Scheduled)
        .await;

    controller
        .handle_update(
            &limiter,
            bandwidth::Update::StreamUpdate {
                id: id1,
                bandwidth: 100,
            },
        )
        .await;

    controller
        .handle_update(&limiter, bandwidth::Update::Scheduled)
        .await;
    controller
        .handle_update(
            &limiter,
            bandwidth::Update::StreamUpdate {
                id: id2,
                bandwidth: 500,
            },
        )
        .await;
    controller
        .handle_update(&limiter, bandwidth::Update::Scheduled)
        .await;
    controller
        .handle_update(
            &limiter,
            bandwidth::Update::StreamUpdate {
                id: id2,
                bandwidth: 50,
            },
        )
        .await;
    controller
        .handle_update(&limiter, bandwidth::Update::Scheduled)
        .await;
}
