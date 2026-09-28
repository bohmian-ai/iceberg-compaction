//! Proof that the managed core resolves exactly one native analytical universe.
//!
//! Arrow, Parquet, `DataFusion`, Iceberg, and `object_store` all carry data
//! across an FFI-shaped boundary via types that are only compatible when both
//! sides were compiled from the same crate version. Two resolutions of `arrow`
//! in one binary are not a duplicate-code problem; they are two incompatible
//! `RecordBatch` types with the same name, and the mismatch surfaces as a
//! confusing trait error at best and as wrong data at worst.
//!
//! Wyrd embeds this core alongside its own Arrow/`DataFusion` stack, so a second
//! universe here becomes a second universe there. This module reads the
//! resolved lockfile and refuses that outcome as a checked property rather
//! than a review habit.

use std::collections::BTreeMap;

use crate::error::{CompactionError, Result};

/// One resolution of one package, as the lockfile recorded it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolution {
    /// Exact resolved version.
    pub version: String,
    /// Registry or git source line, absent for path/workspace members.
    pub source: Option<String>,
}

/// The pin one native package is required to resolve to.
#[derive(Debug, Clone, Copy)]
pub struct NativePin {
    /// Package name as it appears in the lockfile.
    pub name: &'static str,
    /// Exact required version.
    pub version: &'static str,
    /// Substring the source line must contain, identifying the origin
    /// (a registry, or the managed fork's exact revision).
    pub source_contains: &'static str,
}

/// The resolved packages of one lockfile, indexed for pin checking.
///
/// Built by parsing rather than by running Cargo so the proof is available in
/// the same fast lane as the rest of the library's tests, and so a negative
/// fixture can be checked without mutating the working tree.
#[derive(Debug)]
pub struct NativeUniverseAudit {
    resolutions: BTreeMap<String, Vec<Resolution>>,
}

impl NativeUniverseAudit {
    /// Parses a `Cargo.lock`.
    ///
    /// Reads only the `[[package]]` stanzas' `name`, `version`, and `source`
    /// keys; everything else in the file is irrelevant to which universe a
    /// symbol comes from. Unparseable or absent stanzas simply contribute no
    /// resolutions, and a missing expected package is then reported by
    /// [`Self::verify`] rather than being silently tolerated here.
    #[must_use]
    pub fn from_lockfile(lockfile: &str) -> Self {
        let mut resolutions: BTreeMap<String, Vec<Resolution>> = BTreeMap::new();
        let mut name: Option<String> = None;
        let mut version: Option<String> = None;
        let mut source: Option<String> = None;

        let mut flush = |name: &mut Option<String>,
                         version: &mut Option<String>,
                         source: &mut Option<String>| {
            if let (Some(name), Some(version)) = (name.take(), version.take()) {
                resolutions.entry(name).or_default().push(Resolution {
                    version,
                    source: source.take(),
                });
            } else {
                let _ = source.take();
            }
        };

        for line in lockfile.lines() {
            let line = line.trim();
            if line == "[[package]]" {
                flush(&mut name, &mut version, &mut source);
            } else if let Some(value) = quoted_value(line, "name") {
                name = Some(value);
            } else if let Some(value) = quoted_value(line, "version") {
                version = Some(value);
            } else if let Some(value) = quoted_value(line, "source") {
                source = Some(value);
            }
        }
        flush(&mut name, &mut version, &mut source);

        Self { resolutions }
    }

    /// Returns every resolution recorded for `name`.
    #[must_use]
    pub fn resolutions(&self, name: &str) -> &[Resolution] {
        self.resolutions.get(name).map_or(&[], Vec::as_slice)
    }

    /// Requires each pin to have exactly one resolution, at the exact version,
    /// from the expected source.
    ///
    /// All violations are collected rather than short-circuited: a dependency
    /// drift usually moves several members of one universe together, and
    /// reporting only the first would hide the shape of the change.
    ///
    /// # Errors
    ///
    /// Returns [`CompactionError::Config`] naming every pin that is missing,
    /// resolved more than once, resolved at the wrong version, or resolved
    /// from an unexpected source.
    pub fn verify(&self, pins: &[NativePin]) -> Result<()> {
        let mut violations = Vec::new();
        for pin in pins {
            let found = self.resolutions(pin.name);
            match found {
                [] => violations.push(format!("'{}' is not resolved at all", pin.name)),
                [single] => {
                    if single.version != pin.version {
                        violations.push(format!(
                            "'{}' resolved {} but the pin is {}",
                            pin.name, single.version, pin.version
                        ));
                    }
                    let source = single.source.as_deref().unwrap_or("");
                    if !source.contains(pin.source_contains) {
                        violations.push(format!(
                            "'{}' resolved from '{source}' which is not '{}'",
                            pin.name, pin.source_contains
                        ));
                    }
                }
                many => violations.push(format!(
                    "'{}' resolved {} times ({}), so the native universe is not single",
                    pin.name,
                    many.len(),
                    many.iter()
                        .map(|resolution| resolution.version.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )),
            }
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(CompactionError::Config(format!(
                "native dependency universe is not single: {}",
                violations.join("; ")
            )))
        }
    }
}

/// Extracts `value` from a lockfile line of the form `key = "value"`.
fn quoted_value(line: &str, key: &str) -> Option<String> {
    let rest = line
        .strip_prefix(key)?
        .trim_start()
        .strip_prefix('=')?
        .trim();
    Some(rest.strip_prefix('"')?.strip_suffix('"')?.to_owned())
}

#[cfg(test)]
mod tests {
    use super::{NativePin, NativeUniverseAudit};

    /// The exact analytical universe the managed core and Wyrd must share.
    const PINS: &[NativePin] = &[
        NativePin {
            name: "arrow",
            version: "59.3.0",
            source_contains: "registry+https://github.com/rust-lang/crates.io-index",
        },
        NativePin {
            name: "arrow-array",
            version: "59.3.0",
            source_contains: "registry+https://github.com/rust-lang/crates.io-index",
        },
        NativePin {
            name: "arrow-schema",
            version: "59.3.0",
            source_contains: "registry+https://github.com/rust-lang/crates.io-index",
        },
        NativePin {
            name: "parquet",
            version: "59.3.0",
            source_contains: "registry+https://github.com/rust-lang/crates.io-index",
        },
        NativePin {
            name: "datafusion",
            version: "55.0.0",
            source_contains: "registry+https://github.com/rust-lang/crates.io-index",
        },
        NativePin {
            name: "object_store",
            version: "0.13.2",
            source_contains: "registry+https://github.com/rust-lang/crates.io-index",
        },
        NativePin {
            name: "iceberg",
            version: "0.10.0",
            source_contains: "git+https://github.com/bohmian-ai/iceberg-rust.git?rev=3712abbfb348bca9fc714d226685a477afe9bd65",
        },
    ];

    /// The workspace's own resolved lockfile.
    const LOCKFILE: &str = include_str!("../../../Cargo.lock");

    #[test]
    fn wyrd_native_dependency_universe_is_single() {
        let audit = NativeUniverseAudit::from_lockfile(LOCKFILE);
        audit
            .verify(PINS)
            .expect("the managed core must resolve exactly one native analytical universe");

        // The managed Iceberg fork is the sole Iceberg, at the exact revision
        // Wyrd pins; a crates.io Iceberg alongside it would be a second
        // universe wearing the same name.
        let iceberg = audit.resolutions("iceberg");
        assert_eq!(iceberg.len(), 1);
        assert!(
            iceberg[0]
                .source
                .as_deref()
                .is_some_and(|source| source.contains("bohmian-ai/iceberg-rust")),
            "iceberg must come from the managed fork"
        );

        // Negative fixture: one native version drifts. The check must fail,
        // otherwise the green above proves nothing.
        let drifted = LOCKFILE.replacen(
            "name = \"arrow\"\nversion = \"59.3.0\"",
            "name = \"arrow\"\nversion = \"58.0.0\"",
            1,
        );
        assert_ne!(
            drifted, LOCKFILE,
            "the negative fixture must alter the lock"
        );
        let error = NativeUniverseAudit::from_lockfile(&drifted)
            .verify(PINS)
            .expect_err("a drifted native version must fail the dependency check");
        assert!(error.to_string().contains("'arrow' resolved 58.0.0"));

        // Negative fixture: a second resolution of the same package.
        let doubled = format!(
            "{LOCKFILE}\n[[package]]\nname = \"arrow\"\nversion = \"58.0.0\"\n\
             source = \"registry+https://github.com/rust-lang/crates.io-index\"\n"
        );
        let error = NativeUniverseAudit::from_lockfile(&doubled)
            .verify(PINS)
            .expect_err("two resolutions of one native crate must fail the check");
        assert!(error.to_string().contains("'arrow' resolved 2 times"));

        // Negative fixture: right version, foreign source.
        let reforked = LOCKFILE.replace(
            "git+https://github.com/bohmian-ai/iceberg-rust.git?rev=3712abbfb348bca9fc714d226685a477afe9bd65",
            "registry+https://github.com/rust-lang/crates.io-index",
        );
        let error = NativeUniverseAudit::from_lockfile(&reforked)
            .verify(PINS)
            .expect_err("an unmanaged Iceberg source must fail the check");
        assert!(error.to_string().contains("'iceberg' resolved from"));
    }
}
