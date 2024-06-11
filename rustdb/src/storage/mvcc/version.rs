use crate::sql::types::Value;
use crate::storage::TimeStamp;
use std::collections::BTreeMap;

/// if VersionValues == None, it's deleted.
pub type VersionValues = Option<BTreeMap<u64, Value>>;
/// Version storage modified values of tuple in memory;
#[derive(Clone, Debug, Default)]
pub struct Versions(BTreeMap<TimeStamp, VersionValues>);
