use crate::sql::types::{DataType, Value};
use crate::storage::{Error, StorageResult};

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub primary_key: bool,
    pub nullable: Option<bool>,
    pub default: Option<Value>,
    pub unique: bool,
    pub index: bool,
    pub references: Option<String>,
}

impl Column {
    pub fn new(name: impl Into<String>, datatype: DataType) -> Self {
        Self {
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

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn primary(&self) -> bool {
        self.primary_key
    }

    pub fn validate(&self) -> StorageResult<()> {
        // Validate primary key
        if self.primary_key && self.nullable.unwrap_or_default() {
            return Err(Error::Value(format!(
                "Primary key {} cannot be nullable",
                self.name
            )));
        }
        if self.primary_key && !self.unique {
            return Err(Error::Value(format!(
                "Primary key {} must be unique",
                self.name
            )));
        }

        // Validate default value
        if let Some(default) = &self.default {
            if let Some(datatype) = default.datatype() {
                if datatype != self.datatype {
                    return Err(Error::Value(format!(
                        "Default value for column {} has datatype {}, must be {}",
                        self.name, datatype, self.datatype
                    )));
                }
            } else if !self.nullable.unwrap_or_default() {
                return Err(Error::Value(format!(
                    "Can't use NULL as default value for non-nullable column {}",
                    self.name
                )));
            }
        } else if self.nullable.unwrap_or_default() {
            return Err(Error::Value(format!(
                "Nullable column {} must have a default value",
                self.name
            )));
        }
        // todo validate reference

        Ok(())
    }
}
