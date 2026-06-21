//! `SqliteResultStore` runs the shared result-store conformance suite from
//! `worklane-test`, proving the SQLite backend satisfies the durable-result-store
//! contract. A private `:memory:` database per harness gives scenario isolation,
//! so these tests need no external service and always run.

use std::sync::Arc;

use worklane_sqlite::SqliteResultStore;
use worklane_test::{ResultStoreContractHarness, result_store_contract};

struct SqliteResultStoreHarness {
    store: Arc<SqliteResultStore>,
}

impl SqliteResultStoreHarness {
    fn new() -> Self {
        SqliteResultStoreHarness {
            store: Arc::new(
                SqliteResultStore::open(":memory:").expect("open in-memory sqlite result store"),
            ),
        }
    }
}

impl ResultStoreContractHarness for SqliteResultStoreHarness {
    type Store = SqliteResultStore;

    fn store(&self) -> Arc<SqliteResultStore> {
        self.store.clone()
    }
}

result_store_contract!(SqliteResultStoreHarness::new());
