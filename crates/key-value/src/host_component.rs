use spin_core::HostComponent;

use crate::{Config, KeyValueDispatch};

pub struct KeyValueComponent {
    config: Config,
}

impl KeyValueComponent {
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}

impl HostComponent for KeyValueComponent {
    type Data = KeyValueDispatch;

    fn add_to_linker<T: Send>(
        linker: &mut spin_core::Linker<T>,
        get: impl Fn(&mut spin_core::Data<T>) -> &mut Self::Data + Send + Sync + Copy + 'static,
    ) -> anyhow::Result<()> {
        super::key_value::add_to_linker(linker, get)
    }

    fn build_data(&self) -> Self::Data {
        KeyValueDispatch::new(self.config.clone())
    }
}
