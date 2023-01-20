wit_bindgen_rust::import!("../../wit/ephemeral/key-value.wit");

use key_value::Store as RawStore;

/// Errors which may be raised by the methods of `Store`
pub type Error = key_value::Error;

/// Represents a store in which key value tuples may be placed
#[derive(Clone, Debug)]
pub struct Store(RawStore);

impl Store {
    /// Open the specified store.
    pub fn open(name: impl AsRef<str>) -> Result<Self, Error> {
        key_value::open(name.as_ref()).map(Self)
    }

    /// Get the value, if any, associated with the specified key in this store.
    ///
    /// If no value is found, this will return `Err(Error::NoSuchKey)`.
    pub fn get(&self, key: impl AsRef<str>) -> Result<Vec<u8>, Error> {
        key_value::get(self.0, key.as_ref())
    }

    /// Set the value for the specified key.
    ///
    /// This will overwrite any previous value, if present.
    pub fn set(&self, key: impl AsRef<str>, value: impl AsRef<[u8]>) -> Result<(), Error> {
        key_value::set(self.0, key.as_ref(), value.as_ref())
    }

    /// Delete the tuple for the specified key, if any.
    ///
    /// This will have no effect and return `Ok(())` if the tuple was not present.
    pub fn delete(&self, key: impl AsRef<str>) -> Result<(), Error> {
        key_value::delete(self.0, key.as_ref())
    }

    /// Check whether a tuple exists for the specified key.
    pub fn exists(&self, key: impl AsRef<str>) -> Result<bool, Error> {
        key_value::exists(self.0, key.as_ref())
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        key_value::close(self.0)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {}
