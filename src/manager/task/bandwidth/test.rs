use std::collections::HashMap;
use std::iter;
use std::sync::{Arc, Mutex};

use crate::manager::stream::StreamConfig;
use crate::manager::task::bandwidth;
use crate::network::BandwidthAllowed;
use crate::StreamId;

use super::allocation::Bandwidth;
use super::{Controller, LimitBandwidthById};

#[derive(Debug)]
struct HttpClientMock {
    curr_limits: Arc<Mutex<HashMap<StreamId, Bandwidth>>>,
}

impl LimitBandwidthById for HttpClientMock {
    async fn limit_bandwidth(&self, id: StreamId, limit: crate::BandwidthLimit) {
        let limit = limit.0.get();
        let mut curr_limits = self.curr_limits.lock().unwrap();
        curr_limits.insert(id, limit);
    }
}

impl HttpClientMock {
    fn new() -> Self {
        Self {
            curr_limits: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn current_limit_for(&self, stream: StreamId) -> Bandwidth {
        let curr_limits = self.curr_limits.lock().unwrap();
        curr_limits.get(&stream).copied().unwrap_or(Bandwidth::MAX)
    }
}

async fn simulate(
    mut gen_bw_update: impl FnMut(StreamId, Bandwidth) -> Bandwidth,
    streams: usize,
    steps_to_simulate: usize,
) {
    let (mut controller, _cb) = Controller::new(|_, _| (), BandwidthAllowed::UnLimited);

    let config = StreamConfig::default();
    let limiter = HttpClientMock::new();
    let stream_ids: Vec<_> = (0..streams).into_iter().map(|_| StreamId::new()).collect();

    let mut _stream_guards: Vec<_> = Vec::new();
    for id in &stream_ids {
        _stream_guards.push(controller.register(*id, config.clone(), &limiter).await)
    }

    for _ in 0..steps_to_simulate {
        for (id, update) in stream_ids.iter().map(|id| {
            let limit = limiter.current_limit_for(*id);
            let update = gen_bw_update(*id, limit);
            let update = update.min(limit);
            (id, update)
        }) {
            controller
                .handle_update(
                    &limiter,
                    bandwidth::Update::StreamBandwidth {
                        id: *id,
                        bandwidth: update as usize,
                    },
                )
                .await;
        }

        controller
            .handle_update(&limiter, bandwidth::Update::Scheduled)
            .await;
    }
}

#[tokio::test]
async fn test2() {
    let mut recorded_slow_limits = Vec::new();
    let mut recorded_fast_limits = Vec::new();

    let mut slow_stream_id = None;
    let mut fast_stream_id = None;
    let upstream_responses = |id: StreamId, limit: Bandwidth| {
        if slow_stream_id.is_none() {
            slow_stream_id = Some(id);
        } else if fast_stream_id.is_none() {
            fast_stream_id = Some(id);
        }

        let bandwidth_to_report = if slow_stream_id.is_some_and(|slow| slow == id) {
            recorded_slow_limits.push(limit);
            10
        } else {
            recorded_fast_limits.push(limit);
            100_000
        };

        bandwidth_to_report
    };

    let steps = 5;
    simulate(upstream_responses, 2, steps).await;

    slow_stream_id.unwrap();
    fast_stream_id.unwrap();

    let expected_slow_limits: Vec<_> = iter::repeat_n(10, steps).collect();
    let expected_fast_limits: Vec<_> = iter::repeat_n(100_000, steps).collect();

    assert_eq!(
        expected_slow_limits, recorded_slow_limits,
        "expected and recorded limits for slow stream do not match"
    );
    assert_eq!(
        expected_fast_limits, recorded_fast_limits,
        "expected and recorded limits for fast stream do not match"
    );
}
