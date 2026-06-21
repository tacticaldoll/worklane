//! `SqliteBroker::result_store()` must hand out a result store that shares the
//! broker's database — including for an in-memory broker, where the old remedy
//! (`SqliteResultStore::open(":memory:")`) silently opened a *private* database
//! the broker could not see.

use worklane_core::{JobId, ResultStore};
use worklane_sqlite::SqliteBroker;

#[tokio::test]
async fn in_memory_result_store_shares_the_brokers_database() {
    let broker = SqliteBroker::open_in_memory().expect("open in-memory broker");

    // Two stores derived from the same broker.
    let writer = broker.result_store();
    let reader = broker.result_store();

    let id = JobId::new();
    writer.store(&id, b"result-bytes").await.expect("store");

    // The second store sees the first store's write, proving they share one
    // in-memory database. A private `open(":memory:")` store would see `None`.
    let got = reader.get(&id).await.expect("get");
    assert_eq!(
        got.as_deref(),
        Some(&b"result-bytes"[..]),
        "a result store from the same broker must share its database"
    );
}
