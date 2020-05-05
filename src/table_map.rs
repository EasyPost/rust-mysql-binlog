use std::collections::BTreeMap;

use crate::column_types::ColumnType;

#[derive(Debug)]
/// Opaque reference to a table map, intended to be consumed by [`Event`]
pub struct SingleTableMap {
    pub(crate) schema_name: String,
    pub(crate) table_name: String,
    pub(crate) columns: Vec<ColumnType>,
}

/// A MySQL binary log includes Table Map events; the first time a table is referenced in a given
/// binlog, a TME will be emitted describing the fields of that table and assigning them to a
/// binlog-unique identifier. The TableMap object is used to keep track of that mapping.
pub struct TableMap {
    inner: BTreeMap<u64, SingleTableMap>,
}

impl TableMap {
    pub fn new() -> Self {
        TableMap {
            inner: BTreeMap::new(),
        }
    }

    pub fn handle(
        &mut self,
        table_id: u64,
        schema_name: String,
        table_name: String,
        columns: Vec<ColumnType>,
    ) {
        let map = SingleTableMap {
            schema_name,
            table_name,
            columns,
        };
        self.inner.insert(table_id, map);
    }

    pub fn get(&self, table_id: u64) -> Option<&SingleTableMap> {
        self.inner.get(&table_id)
    }
}
