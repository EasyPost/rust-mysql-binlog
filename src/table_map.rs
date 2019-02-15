use std::collections::BTreeMap;

use crate::column_types::ColumnType;


#[derive(Debug)]
pub struct SingleTableMap {
    pub schema_name: String,
    pub table_name: String,
    pub columns: Vec<ColumnType>,
}


pub struct TableMap {
    inner: BTreeMap<u64, SingleTableMap>
}


impl TableMap {
    pub fn new() -> Self {
        TableMap {
            inner: BTreeMap::new()
        }
    }

    pub fn handle(&mut self, table_id: u64, schema_name: String, table_name: String, columns: Vec<ColumnType>) {
        let map = SingleTableMap { schema_name, table_name, columns };
        self.inner.insert(table_id, map);
    }

    pub fn get(&self, table_id: u64) -> Option<&SingleTableMap> {
        self.inner.get(&table_id)
    }
}
