use hashbrown::{HashMap, HashSet};
use log::*;
use morgan_interface::bvm_address::BvmAddr;

pub type Fork = u64;

#[derive(Default)]
pub struct AccountsIndex<T> {
    account_maps: HashMap<BvmAddr, Vec<(Fork, T)>>,
    roots: HashSet<Fork>,
    pub last_root: Fork,
}

impl<T: Clone> AccountsIndex<T> {
    pub fn get(&self, address: &BvmAddr, ancestors: &HashMap<Fork, usize>) -> Option<(&T, Fork)> {
        let list = self.account_maps.get(address)?;
        let mut max = 0;
        let mut rv = None;
        for e in list.iter().rev() {
            if e.0 >= max && (ancestors.get(&e.0).is_some() || self.is_genesis(e.0)) {
                trace!("GET {} {:?}", e.0, ancestors);
                rv = Some((&e.1, e.0));
                max = e.0;
            }
        }
        rv
    }

    pub fn insert(&mut self, fork: Fork, address: &BvmAddr, account_info: T) -> Vec<(Fork, T)> {
        let mut rv = vec![];
        let mut fork_vec: Vec<(Fork, T)> = vec![];
        {
            let entry = self.account_maps.entry(*address).or_insert(vec![]);
            std::mem::swap(entry, &mut fork_vec);
        };

        rv.extend(fork_vec.iter().filter(|(f, _)| *f == fork).cloned());
        fork_vec.retain(|(f, _)| *f != fork);


        fork_vec.push((fork, account_info));

        rv.extend(
            fork_vec
                .iter()
                .filter(|(fork, _)| self.is_purged(*fork))
                .cloned(),
        );
        fork_vec.retain(|(fork, _)| !self.is_purged(*fork));
        {
            let entry = self.account_maps.entry(*address).or_insert(vec![]);
            std::mem::swap(entry, &mut fork_vec);
        };
        rv
    }
    pub fn is_purged(&self, fork: Fork) -> bool {
        fork < self.last_root
    }
    pub fn is_genesis(&self, fork: Fork) -> bool {
        self.roots.contains(&fork)
    }
    pub fn add_root(&mut self, fork: Fork) {
        assert!(
            (self.last_root == 0 && fork == 0) || (fork > self.last_root),
            "new roots must be increasing"
        );
        self.last_root = fork;
        self.roots.insert(fork);
    }

    pub fn cleanup_dead_fork(&mut self, fork: Fork) {
        self.roots.remove(&fork);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use morgan_interface::signature::{Keypair, KeypairUtil};

    #[test]
    fn test_get_empty() {
        let key = Keypair::new();
        let index = AccountsIndex::<bool>::default();
        let ancestors = HashMap::new();
        assert_eq!(index.get(&key.address(), &ancestors), None);
    }

    #[test]
    fn test_insert_no_ancestors() {
        let key = Keypair::new();
        let mut index = AccountsIndex::<bool>::default();
        let gc = index.insert(0, &key.address(), true);
        assert!(gc.is_empty());

        let ancestors = HashMap::new();
        assert_eq!(index.get(&key.address(), &ancestors), None);
    }

    #[test]
    fn test_insert_wrong_ancestors() {
        let key = Keypair::new();
        let mut index = AccountsIndex::<bool>::default();
        let gc = index.insert(0, &key.address(), true);
        assert!(gc.is_empty());

        let ancestors = vec![(1, 1)].into_iter().collect();
        assert_eq!(index.get(&key.address(), &ancestors), None);
    }

    #[test]
    fn test_insert_with_ancestors() {
        let key = Keypair::new();
        let mut index = AccountsIndex::<bool>::default();
        let gc = index.insert(0, &key.address(), true);
        assert!(gc.is_empty());

        let ancestors = vec![(0, 0)].into_iter().collect();
        assert_eq!(index.get(&key.address(), &ancestors), Some((&true, 0)));
    }

    #[test]
    fn test_is_root() {
        let mut index = AccountsIndex::<bool>::default();
        assert!(!index.is_genesis(0));
        index.add_root(0);
        assert!(index.is_genesis(0));
    }

    #[test]
    fn test_insert_with_root() {
        let key = Keypair::new();
        let mut index = AccountsIndex::<bool>::default();
        let gc = index.insert(0, &key.address(), true);
        assert!(gc.is_empty());

        let ancestors = vec![].into_iter().collect();
        index.add_root(0);
        assert_eq!(index.get(&key.address(), &ancestors), Some((&true, 0)));
    }

    #[test]
    fn test_is_purged() {
        let mut index = AccountsIndex::<bool>::default();
        assert!(!index.is_purged(0));
        index.add_root(1);
        assert!(index.is_purged(0));
        index.add_root(2);
        assert!(index.is_purged(1));
    }

    #[test]
    fn test_max_last_root() {
        let mut index = AccountsIndex::<bool>::default();
        index.add_root(1);
        assert_eq!(index.last_root, 1);
    }

    #[test]
    #[should_panic]
    fn test_max_last_root_old() {
        let mut index = AccountsIndex::<bool>::default();
        index.add_root(1);
        index.add_root(0);
    }

    #[test]
    fn test_cleanup_first() {
        let mut index = AccountsIndex::<bool>::default();
        index.add_root(0);
        index.add_root(1);
        index.cleanup_dead_fork(0);
        assert!(index.is_genesis(1));
        assert!(!index.is_genesis(0));
    }

    #[test]
    fn test_cleanup_last() {
        let mut index = AccountsIndex::<bool>::default();
        index.add_root(0);
        index.add_root(1);
        index.cleanup_dead_fork(1);
        assert!(!index.is_genesis(1));
        assert!(index.is_genesis(0));
    }

    #[test]
    fn test_update_last_wins() {
        let key = Keypair::new();
        let mut index = AccountsIndex::<bool>::default();
        let ancestors = vec![(0, 0)].into_iter().collect();
        let gc = index.insert(0, &key.address(), true);
        assert!(gc.is_empty());
        assert_eq!(index.get(&key.address(), &ancestors), Some((&true, 0)));

        let gc = index.insert(0, &key.address(), false);
        assert_eq!(gc, vec![(0, true)]);
        assert_eq!(index.get(&key.address(), &ancestors), Some((&false, 0)));
    }

    #[test]
    fn test_update_new_fork() {
        let key = Keypair::new();
        let mut index = AccountsIndex::<bool>::default();
        let ancestors = vec![(0, 0)].into_iter().collect();
        let gc = index.insert(0, &key.address(), true);
        assert!(gc.is_empty());
        let gc = index.insert(1, &key.address(), false);
        assert!(gc.is_empty());
        assert_eq!(index.get(&key.address(), &ancestors), Some((&true, 0)));
        let ancestors = vec![(1, 0)].into_iter().collect();
        assert_eq!(index.get(&key.address(), &ancestors), Some((&false, 1)));
    }

    #[test]
    fn test_update_gc_purged_fork() {
        let key = Keypair::new();
        let mut index = AccountsIndex::<bool>::default();
        let gc = index.insert(0, &key.address(), true);
        assert!(gc.is_empty());
        index.add_root(1);
        let gc = index.insert(1, &key.address(), false);
        assert_eq!(gc, vec![(0, true)]);
        let ancestors = vec![].into_iter().collect();
        assert_eq!(index.get(&key.address(), &ancestors), Some((&false, 1)));
    }
}
