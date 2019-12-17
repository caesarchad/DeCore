use crate::block_buffer_pool::BlockBufferPool;
use morgan_storage_api::SLOTS_PER_SEGMENT;
use std::fs::File;
use std::io;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

pub const CHACHA_BLOCK_SIZE: usize = 64;
pub const CHACHA_KEY_SIZE: usize = 32;

#[link(name = "cpu-crypt")]
extern "C" {
    fn chacha20_cbc_encrypt(
        input: *const u8,
        output: *mut u8,
        in_len: usize,
        key: *const u8,
        ivec: *mut u8,
    );
}

pub fn chacha_cbc_encrypt(input: &[u8], output: &mut [u8], key: &[u8], ivec: &mut [u8]) {
    unsafe {
        chacha20_cbc_encrypt(
            input.as_ptr(),
            output.as_mut_ptr(),
            input.len(),
            key.as_ptr(),
            ivec.as_mut_ptr(),
        );
    }
}

pub fn chacha_cbc_encrypt_ledger(
    block_buffer_pool: &Arc<BlockBufferPool>,
    slice: u64,
    out_path: &Path,
    ivec: &mut [u8; CHACHA_BLOCK_SIZE],
) -> io::Result<usize> {
    let mut out_file =
        BufWriter::new(File::create(out_path).expect("Can't open ledger encrypted data file"));
    const BUFFER_SIZE: usize = 8 * 1024;
    let mut buffer = [0; BUFFER_SIZE];
    let mut encrypted_buffer = [0; BUFFER_SIZE];
    let key = [0; CHACHA_KEY_SIZE];
    let mut total_entries = 0;
    let mut total_size = 0;
    let mut entry = slice;

    loop {
        match block_buffer_pool.read_db_by_bytes(0, SLOTS_PER_SEGMENT - total_entries, &mut buffer, entry) {
            Ok((num_entries, entry_len)) => {
                debug!(
                    "chacha: encrypting slice: {} num_entries: {} entry_len: {}",
                    slice, num_entries, entry_len
                );
                debug!("read {} bytes", entry_len);
                let mut size = entry_len as usize;
                if size == 0 {
                    break;
                }

                if size < BUFFER_SIZE {
                    // We are on the last block, round to the nearest key_size
                    // boundary
                    size = (size + CHACHA_KEY_SIZE - 1) & !(CHACHA_KEY_SIZE - 1);
                }
                total_size += size;

                chacha_cbc_encrypt(&buffer[..size], &mut encrypted_buffer[..size], &key, ivec);
                if let Err(res) = out_file.write(&encrypted_buffer[..size]) {
                    // warn!("Error writing file! {:?}", res);
                    println!(
                        "{}",
                        Warn(
                            format!("Error writing file! {:?}", res).to_string(),
                            module_path!().to_string()
                        )
                    );
                    return Err(res);
                }

                total_entries += num_entries;
                entry += num_entries;
            }
            Err(e) => {
                // info!("{}", Info(format!("Error encrypting file: {:?}", e).to_string()));
                let loginfo: String = format!("Error encrypting file: {:?}", e).to_string();
                println!("{}",
                    printLn(
                        loginfo,
                        module_path!().to_string()
                    )
                );
                break;
            }
        }
    }
    Ok(total_size)
}

#[derive(Clone)]
pub(crate) struct WrappedShard {
	inner: Vec<u8>,
}

impl WrappedShard {
	/// Wrap `data`.
	pub(crate) fn new(mut data: Vec<u8>) -> Self {
		if data.len() % 2 != 0 {
			data.push(0);
		}

		WrappedShard { inner: data }
	}

	/// Unwrap and yield inner data.
	pub(crate) fn into_inner(self) -> Vec<u8> {
		self.inner
	}
}

impl AsRef<[u8]> for WrappedShard {
	fn as_ref(&self) -> &[u8] {
		self.inner.as_ref()
	}
}

impl AsMut<[u8]> for WrappedShard {
	fn as_mut(&mut self) -> &mut [u8] {
		self.inner.as_mut()
	}
}

impl AsRef<[[u8; 2]]> for WrappedShard {
	fn as_ref(&self) -> &[[u8; 2]] {
		assert_eq!(self.inner.len() % 2, 0);
		if self.inner.is_empty() { return &[] }
		unsafe {
			::std::slice::from_raw_parts(&self.inner[0] as *const _ as _, self.inner.len() / 2)
		}
	}
}

impl AsMut<[[u8; 2]]> for WrappedShard {
	fn as_mut(&mut self) -> &mut [[u8; 2]] {
		let len = self.inner.len();
		assert_eq!(len % 2, 0);

		if self.inner.is_empty() { return &mut [] }
		unsafe {
			::std::slice::from_raw_parts_mut(&mut self.inner[0] as *mut _ as _, len / 2)
		}
	}
}

impl std::iter::FromIterator<[u8; 2]> for WrappedShard {
	fn from_iter<I: IntoIterator<Item=[u8; 2]>>(iterable: I) -> Self {
		let iter = iterable.into_iter();

		let (l, _) = iter.size_hint();
		let mut inner = Vec::with_capacity(l * 2);

		for [a, b] in iter {
			inner.push(a);
			inner.push(b);
		}

		debug_assert_eq!(inner.len() % 2, 0);
		WrappedShard { inner }
	}
}

#[cfg(test)]
mod tests {
    use crate::block_buffer_pool::fetch_interim_ledger_location;
    use crate::block_buffer_pool::BlockBufferPool;
    use crate::chacha::chacha_cbc_encrypt_ledger;
    use crate::entry_info::Entry;
    use crate::create_keys::GenKeys;
    use morgan_interface::hash::{hash, Hash, Hasher};
    use morgan_interface::signature::KeypairUtil;
    use morgan_interface::system_transaction;
    use std::fs::remove_file;
    use std::fs::File;
    use std::io::Read;
    use std::path::Path;
    use std::sync::Arc;

    fn make_tiny_deterministic_test_entries(num: usize) -> Vec<Entry> {
        let zero = Hash::default();
        let one = hash(&zero.as_ref());

        let seed = [2u8; 32];
        let mut rnd = GenKeys::new(seed);
        let keypair = rnd.gen_keypair();

        let mut id = one;
        let mut num_hashes = 0;
        (0..num)
            .map(|_| {
                Entry::new_mut(
                    &mut id,
                    &mut num_hashes,
                    vec![system_transaction::create_user_account(
                        &keypair,
                        &keypair.pubkey(),
                        1,
                        one,
                    )],
                )
            })
            .collect()
    }

    #[test]
    fn test_encrypt_ledger() {
        morgan_logger::setup();
        let ledger_dir = "chacha_test_encrypt_file";
        let ledger_path = fetch_interim_ledger_location(ledger_dir);
        let ticks_per_slot = 16;
        let block_buffer_pool = Arc::new(BlockBufferPool::open_ledger_file(&ledger_path).unwrap());
        let out_path = Path::new("test_chacha_encrypt_file_output.txt.enc");

        let entries = make_tiny_deterministic_test_entries(32);
        block_buffer_pool
            .update_entries(0, 0, 0, ticks_per_slot, &entries)
            .unwrap();

        let mut key = hex!(
            "abcd1234abcd1234abcd1234abcd1234 abcd1234abcd1234abcd1234abcd1234
                            abcd1234abcd1234abcd1234abcd1234 abcd1234abcd1234abcd1234abcd1234"
        );
        chacha_cbc_encrypt_ledger(&block_buffer_pool, 0, out_path, &mut key).unwrap();
        let mut out_file = File::open(out_path).unwrap();
        let mut buf = vec![];
        let size = out_file.read_to_end(&mut buf).unwrap();
        let mut hasher = Hasher::default();
        hasher.hash(&buf[..size]);

        //  golden needs to be updated if blob stuff changes....
        let golden: Hash = "9xb2Asf7UK5G8WqPwsvzo5xwLi4dixBSDiYKCtYRikA"
            .parse()
            .unwrap();

        assert_eq!(hasher.result(), golden);
        remove_file(out_path).unwrap();
    }
}
