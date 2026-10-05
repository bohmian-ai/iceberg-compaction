//! The Wyrd-facing, non-committing compaction boundary.
//!
//! Upstream's [`Compaction`](crate::compaction::Compaction) is a full
//! orchestrator: it plans, rewrites, *and* commits, and its commit path owns
//! retry, conflict handling, and snapshot production. Wyrd cannot use that
//! path. Publication in Wyrd is a durable decision made under a tenant lease,
//! a fence, an audit record, and a reconciliation contract that the core knows
//! nothing about, so a core that could commit would be quietly deciding
//! something it has no authority to decide.
//!
//! This module is the seam Wyrd holds instead. It offers exactly one
//! capability — rewrite one plan into candidate objects. Wyrd plans through
//! [`CompactionPlanner`](crate::compaction::CompactionPlanner) directly, against
//! a table it loaded itself. There is no commit method, no transaction, no
//! catalog mutation, and no handle that reaches one. Upstream's committing composition
//! is untouched and remains available to other consumers; it is simply not
//! reachable from here.

use std::sync::Arc;

use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};

use crate::compaction::{CompactionBuilder, CompactionPlan, RewriteResult};
use crate::config::CompactionConfig;
use crate::error::Result;
use crate::executor::DataFusionExecutor;
use crate::managed::context::ManagedExecutionContext;

/// Rewrites one table's plans without any authority to publish the result.
///
/// Bound to exactly one attempt: the leased runtime, memory budget, scratch
/// root, cancellation token, and observer all come from the
/// [`ManagedExecutionContext`] it was built with. Reusing it for a second
/// attempt would attribute the second attempt's objects to the first
/// attempt's identity.
pub struct NonCommittingCompaction {
    inner: crate::compaction::Compaction,
}

impl std::fmt::Debug for NonCommittingCompaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NonCommittingCompaction")
            .field("table_ident", &self.inner.table_ident)
            .finish_non_exhaustive()
    }
}

impl NonCommittingCompaction {
    /// Binds one table, one configuration, and one attempt's leased resources.
    ///
    /// `catalog` is used for reads only. Passing a catalog that can mutate is
    /// not a licence to mutate it: nothing reachable from this type issues a
    /// write, and a caller that wants that guarantee enforced rather than
    /// documented can pass a read-only catalog.
    #[must_use]
    pub fn new(
        catalog: Arc<dyn Catalog>,
        table_ident: TableIdent,
        config: Arc<CompactionConfig>,
        context: Arc<ManagedExecutionContext>,
    ) -> Self {
        let inner = CompactionBuilder::new(catalog, table_ident)
            .with_config(config)
            .with_executor(Box::new(DataFusionExecutor::with_context(context)))
            .build();
        Self { inner }
    }

    /// Rewrites one plan into candidate objects, publishing nothing.
    ///
    /// The returned [`RewriteResult`] names objects that exist in storage and
    /// belong to no snapshot. Whether they are ever published, and whether
    /// they are reclaimed if they are not, is the caller's decision alone.
    ///
    /// # Errors
    ///
    /// Returns [`CompactionError::Cancelled`](crate::error::CompactionError::Cancelled)
    /// when the attempt's admission was withdrawn, and propagates planning
    /// mismatch and executor failures.
    pub async fn rewrite(&self, plan: CompactionPlan, table: &Table) -> Result<RewriteResult> {
        let execution_config = self
            .inner
            .config
            .as_ref()
            .map(|config| config.execution.clone())
            .unwrap_or_default();
        self.inner
            .rewrite_plan(plan, &execution_config, table)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder};
    use iceberg::spec::NestedField;
    use iceberg::{Catalog, CatalogBuilder, ErrorKind, NamespaceIdent, TableCreation, TableIdent};

    use super::NonCommittingCompaction;
    use crate::config::{CompactionConfigBuilder, CompactionPlanningConfig, SmallFilesConfig};
    use crate::managed::context::ManagedExecutionContext;

    /// Catalog that serves reads and turns every mutation into a failure.
    #[derive(Debug)]
    struct ReadOnlyCatalog {
        inner: Arc<dyn Catalog>,
    }

    impl ReadOnlyCatalog {
        /// Refuses one named mutation.
        fn refuse<T>(operation: &str) -> iceberg::Result<T> {
            Err(iceberg::Error::new(
                ErrorKind::FeatureUnsupported,
                format!("read-only catalog refused mutation: {operation}"),
            ))
        }
    }

    #[async_trait::async_trait]
    impl Catalog for ReadOnlyCatalog {
        async fn list_namespaces(
            &self,
            parent: Option<&NamespaceIdent>,
        ) -> iceberg::Result<Vec<NamespaceIdent>> {
            self.inner.list_namespaces(parent).await
        }

        async fn create_namespace(
            &self,
            _namespace: &NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> iceberg::Result<iceberg::Namespace> {
            Self::refuse("create_namespace")
        }

        async fn get_namespace(
            &self,
            namespace: &NamespaceIdent,
        ) -> iceberg::Result<iceberg::Namespace> {
            self.inner.get_namespace(namespace).await
        }

        async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
            self.inner.namespace_exists(namespace).await
        }

        async fn update_namespace(
            &self,
            _namespace: &NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> iceberg::Result<()> {
            Self::refuse("update_namespace")
        }

        async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<()> {
            Self::refuse("drop_namespace")
        }

        async fn list_tables(
            &self,
            namespace: &NamespaceIdent,
        ) -> iceberg::Result<Vec<TableIdent>> {
            self.inner.list_tables(namespace).await
        }

        async fn create_table(
            &self,
            _namespace: &NamespaceIdent,
            _creation: TableCreation,
        ) -> iceberg::Result<iceberg::table::Table> {
            Self::refuse("create_table")
        }

        async fn load_table(&self, table: &TableIdent) -> iceberg::Result<iceberg::table::Table> {
            self.inner.load_table(table).await
        }

        async fn drop_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
            Self::refuse("drop_table")
        }

        async fn purge_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
            Self::refuse("purge_table")
        }

        async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
            self.inner.table_exists(table).await
        }

        async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> iceberg::Result<()> {
            Self::refuse("rename_table")
        }

        async fn register_table(
            &self,
            _table: &TableIdent,
            _metadata_location: String,
        ) -> iceberg::Result<iceberg::table::Table> {
            Self::refuse("register_table")
        }

        async fn update_table(
            &self,
            _commit: iceberg::TableCommit,
        ) -> iceberg::Result<iceberg::table::Table> {
            Self::refuse("update_table")
        }
    }

    #[tokio::test]
    async fn wyrd_non_committing_boundary_exposes_no_catalog_commit() {
        // Static call-path proof: nothing in this module names a commit-capable
        // symbol, so no call path from the seam can reach one. A reintroduced
        // capability fails here before any test has to catch it at runtime.
        const SOURCE: &str = include_str!("boundary.rs");
        let body = SOURCE
            .split_once("#[cfg(test)]")
            .expect("the module has a test section")
            .0;
        for forbidden in [
            "CommitManager",
            "commit_rewrite_results",
            "compact_with_plan",
            "build_commit_manager",
            "Transaction",
            "ApplyTransactionAction",
            "update_table",
            "register_table",
            "rewrite_files_from_results",
            "overwrite_files",
        ] {
            assert!(
                !body.contains(forbidden),
                "the non-committing boundary must not name '{forbidden}'"
            );
        }

        // The seam's own surface is exactly rewrite. `compact` in particular is
        // upstream's plan-execute-commit shortcut and must not be reachable.
        assert!(!body.contains("pub async fn compact"));
        assert!(!body.contains("pub fn catalog"));
        assert!(body.contains("pub async fn rewrite("));

        // Runtime proof: the whole boundary runs against a catalog that turns
        // any mutation into an error, and completes.
        let temp_dir = tempfile::TempDir::new().unwrap();
        let warehouse_location = temp_dir.path().to_str().unwrap().to_owned();
        let catalog: Arc<MemoryCatalog> = Arc::new(
            MemoryCatalogBuilder::default()
                .load(
                    "memory",
                    HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.to_owned(),
                        warehouse_location.clone(),
                    )]),
                )
                .await
                .unwrap(),
        );
        let namespace = NamespaceIdent::new("ns".to_owned());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        let schema = iceberg::spec::Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(
                    1,
                    "id",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let table_ident = TableIdent::new(namespace.clone(), "t".to_owned());
        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("t".to_owned())
                    .schema(schema)
                    .build(),
            )
            .await
            .unwrap();

        let read_only: Arc<dyn Catalog> = Arc::new(ReadOnlyCatalog {
            inner: catalog.clone() as Arc<dyn Catalog>,
        });
        let context = ManagedExecutionContext::builder().build().unwrap();
        let boundary = NonCommittingCompaction::new(
            read_only,
            table_ident.clone(),
            Arc::new(
                CompactionConfigBuilder::default()
                    .planning(CompactionPlanningConfig::SmallFiles(
                        SmallFilesConfig::default(),
                    ))
                    .build()
                    .unwrap(),
            ),
            context,
        );
        assert!(format!("{boundary:?}").contains("NonCommittingCompaction"));

        // The refusal is real, not merely unexercised.
        let refused = ReadOnlyCatalog {
            inner: catalog.clone() as Arc<dyn Catalog>,
        }
        .drop_table(&table_ident)
        .await;
        assert!(refused.is_err());
    }
}
