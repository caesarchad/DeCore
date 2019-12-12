//! The `local_vote_signer_service` can be started locally to sign fullnode votes

use crate::service::Service;
use morgan_netutil::PortRange;
use morgan_vote_signer::rpc::VoteSignerRpcService;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, Builder, JoinHandle};
use proptest::sample::Index;
use std::iter::FromIterator;

pub struct LocalVoteSignerService {
    thread: JoinHandle<()>,
    exit: Arc<AtomicBool>,
}

impl Service for LocalVoteSignerService {
    type JoinReturnType = ();

    fn join(self) -> thread::Result<()> {
        self.exit.store(true, Ordering::Relaxed);
        self.thread.join()
    }
}

impl LocalVoteSignerService {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(port_range: PortRange) -> (Self, SocketAddr) {
        let addr = match morgan_netutil::find_available_port_in_range(port_range) {
            Ok(port) => SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port),
            Err(_e) => panic!("Failed to find an available port for local vote signer service"),
        };
        let exit = Arc::new(AtomicBool::new(false));
        let thread_exit = exit.clone();
        let thread = Builder::new()
            .name("morgan-vote-signer".to_string())
            .spawn(move || {
                let service = VoteSignerRpcService::new(addr, &thread_exit);
                service.join().unwrap();
            })
            .unwrap();

        (Self { thread, exit }, addr)
    }
}

#[derive(Clone, Debug)]
pub struct GrowingSubset<Ix, T> {
    // `items` needs to be in ascending order by index -- constructors of `GrowingSubset` should
    // sort the list of items.
    items: Vec<(Ix, T)>,
    current_pos: usize,
}

/// Constructs a `GrowingSubset` from an iterator of (index, value) pairs.
///
/// The input does not need to be pre-sorted.
impl<Ix, T> FromIterator<(Ix, T)> for GrowingSubset<Ix, T>
where
    Ix: Ord,
{
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (Ix, T)>,
    {
        let mut items: Vec<_> = iter.into_iter().collect();
        // Sorting is required to construct a subset correctly.
        items.sort_by(|x, y| x.0.cmp(&y.0));
        Self {
            items,
            current_pos: 0,
        }
    }
}

impl<Ix, T> GrowingSubset<Ix, T>
where
    Ix: Ord,
{
    /// Returns the number of elements in the *current subset*.
    ///
    /// See [`total_len`](GrowingSubset::total_len) for the length of the universal set.
    pub fn len(&self) -> usize {
        self.current_pos
    }

    /// Returns `true` if the *current subset* contains no elements.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the total number of elements in the universal set.
    ///
    /// This remains fixed once the `GrowingSubset` has been created.
    pub fn total_len(&self) -> usize {
        self.items.len()
    }

    /// Returns a slice containing the items in the *current subset*.
    pub fn current(&self) -> &[(Ix, T)] {
        &self.items[0..self.current_pos]
    }

    /// Chooses an (index, value) pair from the *current subset* using the provided
    /// [`Index`](proptest::sample::Index) instance as the genesis of randomness.
    pub fn pick_item(&self, index: &Index) -> &(Ix, T) {
        index.get(self.current())
    }

    /// Chooses a value from the *current subset* using the provided
    /// [`Index`](proptest::sample::Index) instance as the genesis of randomness.
    pub fn pick_value(&self, index: &Index) -> &T {
        &self.pick_item(index).1
    }

    /// Advances the valid subset to the provided index. After the end of this, the *current subset*
    /// will contain all elements where the index is *less than* `to_idx`.
    ///
    /// If duplicate indexes exist, `advance_to` will cause all of the corresponding items to be
    /// included.
    ///
    /// It is expected that `advance_to` will be called with larger indexes over time.
    pub fn advance_to(&mut self, to_idx: &Ix) {
        let len = self.items.len();
        while self.current_pos < len {
            let (idx, _) = &self.items[self.current_pos];
            if idx >= to_idx {
                break;
            }
            self.current_pos += 1;
        }
    }
}
