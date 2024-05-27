use crate::sql::catalog::ColumnId;
use crate::sql::types::{DataType, Value};
use crate::storage::page::column::ColumnDesc;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnCatalog {
    id: ColumnId,
    desc: ColumnDesc,
}

impl ColumnCatalog {
    pub fn new(id: ColumnId, desc: ColumnDesc) -> Self {
        Self { id, desc }
    }
    pub fn name(&self) -> &str {
        &self.desc.name
    }

    pub fn id(&self) -> ColumnId {
        self.id
    }
    pub fn set_id(&mut self, column_id: ColumnId) {
        self.id = column_id
    }

    pub fn primary(&self) -> bool {
        self.desc.primary_key
    }
    pub fn set_primary(&mut self, primary: bool) {
        self.desc.primary_key = primary
    }

    pub fn set_nullable(&mut self, nullable: bool) {
        self.desc.nullable = Some(nullable);
    }

    pub fn nullable(&self) -> Option<bool> {
        self.desc.nullable
    }

    pub fn set_default(&mut self, default: Option<Value>) {
        self.desc.default = default
    }

    pub fn default(&self) -> Option<&Value> {
        self.desc.default.as_ref()
    }

    pub fn set_index(&mut self, index: bool) {
        self.desc.index = index
    }

    pub fn index(&self) -> bool {
        self.desc.index
    }

    pub fn set_unique(&mut self, unique: bool) {
        self.desc.unique = unique
    }

    pub fn unique(&self) -> bool {
        self.desc.unique
    }
}
