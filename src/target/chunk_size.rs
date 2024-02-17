use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) enum ChunkSizeBuilder {
    Dynamic { max: Option<usize> },
    Fixed(usize),
}

impl Default for ChunkSizeBuilder {
    fn default() -> Self {
        Self::new_dynamic()
    }
}

impl ChunkSizeBuilder {
    pub(crate) fn new_fixed(size: NonZeroUsize) -> Self {
        Self::Fixed(size.get())
    }

    pub(crate) fn new_dynamic() -> Self {
        Self::Dynamic { max: None }
    }

    pub(crate) fn new_dynamic_with_max(max: NonZeroUsize) -> Self {
        Self::Dynamic {
            max: Some(max.get()),
        }
    }

    pub(crate) fn build(self, bandwidth: Arc<AtomicUsize>) -> ChunkSize {
        match self {
            ChunkSizeBuilder::Dynamic { max } => ChunkSize::Dynamic {
                max_size: max,
                bandwidth,
            },
            ChunkSizeBuilder::Fixed(size) => ChunkSize::Fixed(size),
        }
    }
}

#[derive(Debug)]
pub(crate) enum ChunkSize {
    Dynamic {
        max_size: Option<usize>,
        bandwidth: Arc<AtomicUsize>,
    },
    Fixed(usize),
}

impl ChunkSize {
    pub(crate) fn calc(&mut self) -> usize {
        match self {
            ChunkSize::Dynamic {
                max_size,
                ref bandwidth,
            } => update_dynamic(*max_size, bandwidth.as_ref()),
            ChunkSize::Fixed(size) => *size,
        }
    }
}

fn update_dynamic(max_size: Option<usize>, bandwidth: &AtomicUsize) -> usize {
    let bytes_per_second = bandwidth.load(Ordering::Relaxed) * 1;

    // aim for a new http range request every 2 seconds
    let new_optimal = bytes_per_second * 2;
    let res = new_optimal.max(100);

    if let Some(max) = max_size {
        res.min(max)
    } else {
        res
    }
}
