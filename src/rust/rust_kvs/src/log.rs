// Copyright (c) 2025 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache License Version 2.0 which is available at
// <https://www.apache.org/licenses/LICENSE-2.0>
//
// SPDX-License-Identifier: Apache-2.0

//! Logging module.
//! Utilizes `"PERS"` context by default.

#![allow(unused_macros)]

pub(crate) const CONTEXT: &str = "PERS";

/// Proxy for `mw_log::fatal!`.
#[clippy::format_args]
macro_rules! fatal {
    // fatal!(logger: my_logger, "a {} event", "log")
    (logger: $logger:expr, $($arg:tt)+) => (mw_log::fatal!(logger: $logger, context: $crate::log::CONTEXT, $($arg)+));

    // fatal!("a {} event", "log")
    ($($arg:tt)+) => (mw_log::fatal!(context: $crate::log::CONTEXT, $($arg)+));
}

/// Proxy for `mw_log::error!`.
#[clippy::format_args]
macro_rules! error {
    // error!(logger: my_logger, "a {} event", "log")
    (logger: $logger:expr, $($arg:tt)+) => (mw_log::error!(logger: $logger, context: $crate::log::CONTEXT, $($arg)+));

    // error!("a {} event", "log")
    ($($arg:tt)+) => (mw_log::error!(context: $crate::log::CONTEXT, $($arg)+));
}

/// Proxy for `mw_log::warn!`.
#[clippy::format_args]
macro_rules! warning {
    // warn!(logger: my_logger, "a {} event", "log")
    (logger: $logger:expr, $($arg:tt)+) => (mw_log::warn!(logger: $logger, context: $crate::log::CONTEXT, $($arg)+));

    // warn!("a {} event", "log")
    ($($arg:tt)+) => (mw_log::warn!(context: $crate::log::CONTEXT, $($arg)+));
}

/// Proxy for `mw_log::info!`.
#[clippy::format_args]
macro_rules! info {
    // info!(logger: my_logger, "a {} event", "log")
    (logger: $logger:expr, $($arg:tt)+) => (mw_log::info!(logger: $logger, context: $crate::log::CONTEXT, $($arg)+));

    // info!("a {} event", "log")
    ($($arg:tt)+) => (mw_log::info!(context: $crate::log::CONTEXT, $($arg)+));
}

/// Proxy for `mw_log::debug!`.
#[clippy::format_args]
macro_rules! debug {
    // debug!(logger: my_logger, "a {} event", "log")
    (logger: $logger:expr, $($arg:tt)+) => (mw_log::debug!(logger: $logger, context: $crate::log::CONTEXT, $($arg)+));

    // debug!("a {} event", "log")
    ($($arg:tt)+) => (mw_log::debug!(context: $crate::log::CONTEXT, $($arg)+));
}

/// Proxy for `mw_log::trace!`.
#[clippy::format_args]
macro_rules! trace {
    // trace!(logger: my_logger, "a {} event", "log")
    (logger: $logger:expr, $($arg:tt)+) => (mw_log::trace!(logger: $logger, context: $crate::log::CONTEXT, $($arg)+));

    // trace!("a {} event", "log")
    ($($arg:tt)+) => (mw_log::trace!(context: $crate::log::CONTEXT, $($arg)+));
}

// Export macros from this module (e.g., `crate::log::error`).
// `#[macro_export]` would export them from crate (e.g., `crate::error`).
//
// `warning as warn` is due to `warn` macro name conflicting with `warn` attribute.
#[allow(unused_imports)]
pub(crate) use {debug, error, fatal, info, trace, warning as warn};

// Re-export symbols from `mw_log`.
pub(crate) use mw_log::fmt::ScoreDebug;
pub(crate) use mw_log::ScoreDebug;
