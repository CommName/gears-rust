//! Application Manager gear.
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

pub mod config;
pub mod domain;
pub mod gear;
pub mod infra;
#[cfg(test)]
mod test_support;
