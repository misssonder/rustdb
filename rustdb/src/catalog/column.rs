use crate::catalog::{ColumnId, TableId};
use crate::sql::types::{DataType, Value};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ColumnCatalog {
    pub id: ColumnId,
    pub name: String,
    pub datatype: DataType,
    pub primary_key: bool,
    pub nullable: Option<bool>,
    pub default: Option<Value>,
    pub unique: bool,
    pub index: bool,
    pub references: Option<String>,
}

impl ColumnCatalog {
    pub fn new<T: Into<String>>(id: TableId, name: T, datatype: DataType) -> Self {
        Self {
            id,
            name: name.into(),
            datatype,
            primary_key: false,
            nullable: None,
            default: None,
            unique: false,
            index: false,
            references: None,
        }
    }

    pub fn id(&self) -> ColumnId {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn primary(&self) -> bool {
        self.primary_key
    }

    pub fn set_id(&mut self, id: ColumnId) {
        self.id = id
    }

    pub fn with_datatye(mut self, datatype: DataType) -> Self {
        self.datatype = datatype;
        self
    }

    pub fn with_primary(mut self, primary: bool) -> Self {
        self.primary_key = primary;
        self
    }

    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = Some(nullable);
        self
    }

    pub fn with_default(mut self, default: Value) -> Self {
        self.default = Some(default);
        self
    }

    pub fn with_unique(mut self, unique: bool) -> Self {
        self.unique = unique;
        self
    }

    pub fn with_index(mut self, index: bool) -> Self {
        self.index = index;
        self
    }

    pub fn with_references(mut self, references: impl Into<String>) -> Self {
        self.references = Some(references.into());
        self
    }
}
