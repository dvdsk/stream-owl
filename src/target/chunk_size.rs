use std::num::NonZeroUsize;

#[derive(Debug)]
pub(crate) enum ChunkSizeBuilder {
    Dynamic { max: Option<usize> },
    Fixed(usize),
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

    pub(crate) fn build(self) -> ChunkSize {
        match self {
            ChunkSizeBuilder::Dynamic { max } => ChunkSize::Dynamic { max, last: 10_000 },
            ChunkSizeBuilder::Fixed(size) => ChunkSize::Fixed(size),
        }
    }
}

#[derive(Debug)]
pub(crate) enum ChunkSize {
    Dynamic { last: usize, max: Option<usize> },
    Fixed(usize),
}

impl ChunkSize {
    pub(crate) fn calc(&mut self) -> usize {
        match self {
            ChunkSize::Dynamic { max, ref mut last } => {
                *last = update_dynamic(*max, last);
                *last
            }
            ChunkSize::Fixed(size) => *size,
        }
    }

    pub(crate) fn get(&self) -> usize {
        match self {
            ChunkSize::Dynamic { last, .. } => *last,
            ChunkSize::Fixed(size) => *size,
        }
    }
}

fn update_dynamic(max: Option<usize>, last: &mut usize) -> usize {
    let res = last;

    if let Some(max) = max {
        (*res).min(max)
    }else {
        *res
    }
}
