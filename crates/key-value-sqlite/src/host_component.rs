use spin_core::HostComponent;

use crate::KeyValueSqlite;

pub struct KeyValueSqliteComponent;

impl HostComponent for KeyValueSqliteComponent {
    type Data = KeyValueSqlite;

    fn add_to_linker<T: Send>(
        linker: &mut spin_core::Linker<T>,
        get: impl Fn(&mut spin_core::Data<T>) -> &mut Self::Data + Send + Sync + Copy + 'static,
    ) -> anyhow::Result<()> {
        super::key_value::add_to_linker(linker, get)
    }

    fn build_data(&self) -> Self::Data {
        Default::default()
    }
}
