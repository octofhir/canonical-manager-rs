//! Configuration validation for optimization settings
//!
//! This module validates configuration settings according to ADR-002 specifications
//! and provides recommendations for optimal performance.

use crate::config::{FcmConfig, OptimizationConfig};
use crate::error::{ConfigError, FcmError, Result};
use serde::{Deserialize, Serialize};

/// Configuration validator for optimization settings
pub struct ConfigValidator;

/// Validation result with recommendations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    pub is_valid: bool,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
    pub recommendations: Vec<String>,
    pub performance_impact: PerformanceImpact,
}

/// Expected performance impact of current configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceImpact {
    pub incremental_speedup_estimate: f64,
    pub parallel_efficiency_estimate: f64,
    pub memory_usage_estimate: String,
    pub disk_usage_estimate: String,
}

impl ConfigValidator {
    /// Validate a complete FCM configuration
    pub fn validate_config(config: &FcmConfig) -> ValidationResult {
        let mut result = ValidationResult {
            is_valid: true,
            warnings: Vec::new(),
            errors: Vec::new(),
            recommendations: Vec::new(),
            performance_impact: PerformanceImpact::default(),
        };

        // Validate optimization settings
        Self::validate_optimization_config(&config.optimization, &mut result);

        // Validate storage settings
        Self::validate_storage_config(config, &mut result);

        // Generate performance estimates
        Self::estimate_performance(&config.optimization, &mut result);

        // Generate recommendations
        Self::generate_recommendations(config, &mut result);

        result
    }

    /// Validate optimization configuration
    fn validate_optimization_config(config: &OptimizationConfig, result: &mut ValidationResult) {
        // Validate parallel workers
        if config.parallel_workers == 0 {
            result
                .errors
                .push("parallel_workers must be greater than 0".to_string());
            result.is_valid = false;
        } else if config.parallel_workers > 32 {
            result.warnings.push(format!(
                "parallel_workers ({}) is very high - may cause resource contention",
                config.parallel_workers
            ));
        } else if config.parallel_workers < num_cpus::get() {
            result.recommendations.push(format!(
                "Consider increasing parallel_workers to {} (number of CPU cores)",
                num_cpus::get()
            ));
        }

        // Validate batch size
        if config.batch_size == 0 {
            result
                .errors
                .push("batch_size must be greater than 0".to_string());
            result.is_valid = false;
        } else if config.batch_size < 10 {
            result
                .warnings
                .push("batch_size is very small - may reduce parallel efficiency".to_string());
        } else if config.batch_size > 1000 {
            result
                .warnings
                .push("batch_size is very large - may increase memory usage".to_string());
        }

        // Validate checksum cache size
        if config.checksum_cache_size == 0 {
            result
                .errors
                .push("checksum_cache_size must be greater than 0".to_string());
            result.is_valid = false;
        } else if config.checksum_cache_size < 100 {
            result.warnings.push(
                "checksum_cache_size is small - may reduce change detection efficiency".to_string(),
            );
        }

        // Validate full rebuild threshold
        if config.full_rebuild_threshold < 0.0 || config.full_rebuild_threshold > 1.0 {
            result
                .errors
                .push("full_rebuild_threshold must be between 0.0 and 1.0".to_string());
            result.is_valid = false;
        } else if config.full_rebuild_threshold < 0.1 {
            result.warnings.push(
                "full_rebuild_threshold is very low - may trigger too many full rebuilds"
                    .to_string(),
            );
        } else if config.full_rebuild_threshold > 0.8 {
            result.warnings.push(
                "full_rebuild_threshold is very high - may delay needed full rebuilds".to_string(),
            );
        }

        // Validate compression level
        if config.compression_level < 1 || config.compression_level > 22 {
            result
                .errors
                .push("compression_level must be between 1 and 22 for zstd".to_string());
            result.is_valid = false;
        } else if config.compression_level > 15 {
            result.warnings.push(
                "compression_level is very high - will increase CPU usage significantly"
                    .to_string(),
            );
        }

        // Validate incremental batch size
        if config.incremental_batch_size == 0 {
            result
                .errors
                .push("incremental_batch_size must be greater than 0".to_string());
            result.is_valid = false;
        }
    }

    /// Validate storage configuration
    fn validate_storage_config(config: &FcmConfig, result: &mut ValidationResult) {
        // Parse max cache size
        match Self::parse_size_string(&config.storage.max_cache_size) {
            Ok(size_bytes) => {
                if size_bytes < 100 * 1024 * 1024 {
                    // 100 MB
                    result
                        .warnings
                        .push("max_cache_size is very small - may limit performance".to_string());
                } else if size_bytes > 10 * 1024 * 1024 * 1024 {
                    // 10 GB
                    result.warnings.push(
                        "max_cache_size is very large - ensure sufficient system memory"
                            .to_string(),
                    );
                }
            }
            Err(_) => {
                result.errors.push(
                    "max_cache_size format is invalid - use format like '1GB', '500MB'".to_string(),
                );
                result.is_valid = false;
            }
        }

        // Validate max index size for memory mapping
        if config.optimization.use_mmap {
            match Self::parse_size_string(&config.optimization.max_index_size) {
                Ok(size_bytes) => {
                    if size_bytes < 1024 * 1024 {
                        // 1 MB
                        result.recommendations.push(
                            "Consider disabling memory mapping for small indexes".to_string(),
                        );
                    }
                }
                Err(_) => {
                    result.warnings.push(
                        "max_index_size format is invalid when using memory mapping".to_string(),
                    );
                }
            }
        }
    }

    /// Estimate performance impact of current configuration
    fn estimate_performance(config: &OptimizationConfig, result: &mut ValidationResult) {
        // Estimate incremental speedup based on parallel workers and batch size
        let base_speedup = if config.incremental_indexing {
            3.0
        } else {
            1.0
        };
        let parallel_boost = (config.parallel_workers as f64).min(8.0) * 0.5;
        let batch_efficiency = if config.batch_size >= 50 && config.batch_size <= 200 {
            1.2
        } else {
            1.0
        };

        result.performance_impact.incremental_speedup_estimate =
            base_speedup * parallel_boost * batch_efficiency;

        // Estimate parallel efficiency
        let cpu_cores = num_cpus::get() as f64;
        result.performance_impact.parallel_efficiency_estimate =
            (config.parallel_workers as f64 / cpu_cores).min(1.0) * 0.85; // 85% theoretical max

        // Estimate memory usage
        let cache_memory = config.checksum_cache_size * 256; // ~256 bytes per entry estimate
        let worker_memory = config.parallel_workers * 50 * 1024 * 1024; // ~50MB per worker
        let total_memory_mb = (cache_memory + worker_memory) / (1024 * 1024);

        result.performance_impact.memory_usage_estimate = format!("~{total_memory_mb}MB");

        // Estimate disk usage impact from compression
        let compression_ratio = match config.compression_level {
            1..=3 => 0.7,
            4..=6 => 0.6,
            7..=12 => 0.5,
            _ => 0.4,
        };

        result.performance_impact.disk_usage_estimate = format!(
            "{}% of uncompressed size",
            (compression_ratio * 100.0) as u32
        );
    }

    /// Generate configuration recommendations
    fn generate_recommendations(config: &FcmConfig, result: &mut ValidationResult) {
        let cpu_cores = num_cpus::get();

        // Parallel workers recommendation
        if config.optimization.parallel_workers != cpu_cores {
            result.recommendations.push(format!(
                "Consider setting parallel_workers to {cpu_cores} (matches CPU cores)"
            ));
        }

        // Batch size optimization
        if !(50..=200).contains(&config.optimization.batch_size) {
            result
                .recommendations
                .push("Consider batch_size between 50-200 for optimal performance".to_string());
        }

        // Memory mapping recommendation
        if !config.optimization.use_mmap {
            result.recommendations.push(
                "Enable memory mapping (use_mmap=true) for better performance with large indexes"
                    .to_string(),
            );
        }

        // Compression level optimization
        if config.optimization.compression_level > 6 {
            result.recommendations.push(
                "Consider compression_level 3-6 for balance of speed and compression".to_string(),
            );
        }

        // Checksum cache optimization
        if config.optimization.checksum_cache_size < 1000 {
            result.recommendations.push("Consider increasing checksum_cache_size to at least 1000 for better change detection".to_string());
        }

        // Performance monitoring recommendation
        if !config.optimization.enable_metrics {
            result.recommendations.push("Enable performance metrics (enable_metrics=true) to monitor optimization effectiveness".to_string());
        }
    }

    /// Parse size strings like "1GB", "500MB", etc.
    fn parse_size_string(size_str: &str) -> Result<u64> {
        let size_str = size_str.trim().to_uppercase();

        if let Some(pos) = size_str.find(|c: char| c.is_alphabetic()) {
            let (number_part, unit_part) = size_str.split_at(pos);
            let number: f64 = number_part.parse().map_err(|_| {
                FcmError::Config(ConfigError::InvalidValue {
                    key: "size_string".to_string(),
                    value: number_part.to_string(),
                })
            })?;

            let multiplier = match unit_part {
                "B" => 1,
                "KB" => 1_024,
                "MB" => 1_024 * 1_024,
                "GB" => 1_024 * 1_024 * 1_024,
                "TB" => 1_024_u64.pow(4),
                _ => {
                    return Err(FcmError::Config(ConfigError::InvalidValue {
                        key: "size_unit".to_string(),
                        value: unit_part.to_string(),
                    }));
                }
            };

            Ok((number * multiplier as f64) as u64)
        } else {
            // Assume bytes if no unit
            size_str.parse::<u64>().map_err(|_| {
                FcmError::Config(ConfigError::InvalidValue {
                    key: "size_number".to_string(),
                    value: size_str.clone(),
                })
            })
        }
    }

    /// Validate and optimize configuration automatically
    pub fn optimize_config(config: &mut FcmConfig) -> ValidationResult {
        let validation = Self::validate_config(config);

        // Apply automatic optimizations if configuration is valid
        if validation.is_valid {
            Self::apply_optimizations(config);
        }

        validation
    }

    /// Apply automatic optimizations to configuration
    fn apply_optimizations(config: &mut FcmConfig) {
        let cpu_cores = num_cpus::get();

        // Optimize parallel workers to match CPU cores
        if config.optimization.parallel_workers == 0 {
            config.optimization.parallel_workers = cpu_cores;
        }

        // Optimize batch size based on parallel workers
        if config.optimization.batch_size < 10 {
            config.optimization.batch_size = (cpu_cores * 10).max(50);
        }

        // Enable memory mapping for better performance
        if !config.optimization.use_mmap {
            config.optimization.use_mmap = true;
        }

        // Set reasonable compression level
        if config.optimization.compression_level > 12 {
            config.optimization.compression_level = 6; // Good balance of speed/compression
        }

        // Ensure metrics are enabled for performance monitoring
        if !config.optimization.enable_metrics {
            config.optimization.enable_metrics = true;
        }
    }
}

impl Default for PerformanceImpact {
    fn default() -> Self {
        Self {
            incremental_speedup_estimate: 1.0,
            parallel_efficiency_estimate: 0.0,
            memory_usage_estimate: "Unknown".to_string(),
            disk_usage_estimate: "Unknown".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::OptimizationConfig;

    #[test]
    fn test_size_string_parsing() {
        assert_eq!(ConfigValidator::parse_size_string("1024").unwrap(), 1024);
        assert_eq!(ConfigValidator::parse_size_string("1KB").unwrap(), 1024);
        assert_eq!(
            ConfigValidator::parse_size_string("1MB").unwrap(),
            1024 * 1024
        );
        assert_eq!(
            ConfigValidator::parse_size_string("1GB").unwrap(),
            1024 * 1024 * 1024
        );
        assert_eq!(
            ConfigValidator::parse_size_string("2.5GB").unwrap(),
            (2.5 * 1024.0 * 1024.0 * 1024.0) as u64
        );
    }

    #[test]
    fn test_optimization_config_validation() {
        let mut config = OptimizationConfig::default();
        config.parallel_workers = 0; // Invalid
        config.batch_size = 0; // Invalid
        config.full_rebuild_threshold = 1.5; // Invalid

        let mut result = ValidationResult {
            is_valid: true,
            warnings: Vec::new(),
            errors: Vec::new(),
            recommendations: Vec::new(),
            performance_impact: PerformanceImpact::default(),
        };

        ConfigValidator::validate_optimization_config(&config, &mut result);

        assert!(!result.is_valid);
        assert!(result.errors.len() >= 3);
    }

    #[test]
    fn test_config_optimization() {
        let mut config = FcmConfig::default();
        config.optimization.parallel_workers = 0;
        config.optimization.use_mmap = false;

        ConfigValidator::apply_optimizations(&mut config);

        assert!(config.optimization.parallel_workers > 0);
        assert!(config.optimization.use_mmap);
        assert!(config.optimization.enable_metrics);
    }
}
