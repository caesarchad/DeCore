use generic_array::typenum::U32;
use generic_array::GenericArray;
use std::error;
use std::fmt;
use std::fs::{self, File};
use std::io::Write;
use std::mem;
use std::path::Path;
use std::str::FromStr;

#[repr(C)]
#[derive(Serialize, Deserialize, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct BvmAddr(GenericArray<u8, U32>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrseAddrErr {
    WrongSize,
    Invalid,
}

impl fmt::Display for PrseAddrErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PrseAddrErr: {:?}", self)
    }
}

impl error::Error for PrseAddrErr {}

impl FromStr for BvmAddr {
    type Err = PrseAddrErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let address_vec = bs58::decode(s)
            .into_vec()
            .map_err(|_| PrseAddrErr::Invalid)?;
        if address_vec.len() != mem::size_of::<BvmAddr>() {
            Err(PrseAddrErr::WrongSize)
        } else {
            Ok(BvmAddr::new(&address_vec))
        }
    }
}

impl BvmAddr {
    pub fn new(address_vec: &[u8]) -> Self {
        BvmAddr(GenericArray::clone_from_slice(&address_vec))
    }

    pub fn new_rand() -> Self {
        Self::new(&rand::random::<[u8; 32]>())
    }
}

impl AsRef<[u8]> for BvmAddr {
    fn as_ref(&self) -> &[u8] {
        &self.0[..]
    }
}

impl fmt::Debug for BvmAddr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", bs58::encode(self.0).into_string())
    }
}

impl fmt::Display for BvmAddr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", bs58::encode(self.0).into_string())
    }
}

pub fn write_address(outfile: &str, address: BvmAddr) -> Result<(), Box<error::Error>> {
    let printable = format!("{}", address);
    let serialized = serde_json::to_string(&printable)?;

    if let Some(outdir) = Path::new(&outfile).parent() {
        fs::create_dir_all(outdir)?;
    }
    let mut f = File::create(outfile)?;
    f.write_all(&serialized.clone().into_bytes())?;

    Ok(())
}

pub fn read_address(infile: &str) -> Result<BvmAddr, Box<error::Error>> {
    let f = File::open(infile.to_string())?;
    let printable: String = serde_json::from_reader(f)?;
    Ok(BvmAddr::from_str(&printable)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::remove_file;

    #[test]
    fn address_fromstr() {
        let address = BvmAddr::new_rand();
        let mut address_base58_str = bs58::encode(address.0).into_string();

        assert_eq!(address_base58_str.parse::<BvmAddr>(), Ok(address));

        address_base58_str.push_str(&bs58::encode(address.0).into_string());
        assert_eq!(
            address_base58_str.parse::<BvmAddr>(),
            Err(PrseAddrErr::WrongSize)
        );

        address_base58_str.truncate(address_base58_str.len() / 2);
        assert_eq!(address_base58_str.parse::<BvmAddr>(), Ok(address));

        address_base58_str.truncate(address_base58_str.len() / 2);
        assert_eq!(
            address_base58_str.parse::<BvmAddr>(),
            Err(PrseAddrErr::WrongSize)
        );

        let mut address_base58_str = bs58::encode(address.0).into_string();
        assert_eq!(address_base58_str.parse::<BvmAddr>(), Ok(address));

        // throw some non-base58 stuff in there
        address_base58_str.replace_range(..1, "I");
        assert_eq!(
            address_base58_str.parse::<BvmAddr>(),
            Err(PrseAddrErr::Invalid)
        );
    }

    #[test]
    fn test_read_write_address() -> Result<(), Box<error::Error>> {
        let filename = "test_address.json";
        let address = BvmAddr::new_rand();
        write_address(filename, address)?;
        let read = read_address(filename)?;
        assert_eq!(read, address);
        remove_file(filename)?;
        Ok(())
    }
}
