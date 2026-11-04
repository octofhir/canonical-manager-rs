//! Configuration validation for optimization settings
//!
//! Provides validation and recommendations for FCM configuration with SQLite backend.

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
    pub parallel_efficiency_estimate: f64,
    pub memory_usage_estimate: String,
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
    }

    /// Estimate performance impact of current configuration
    fn estimate_performance(config: &OptimizationConfig, result: &mut ValidationResult) {
        // Estimate parallel efficiency
        let cpu_cores = num_cpus::get() as f64;
        result.performance_impact.parallel_efficiency_estimate =
            (config.parallel_workers as f64 / cpu_cores).min(1.0) * 0.85; // 85% theoretical max

        // Estimate memory usage
        let cache_memory = config.checksum_cache_size * 256; // ~256 bytes per entry estimate
        let worker_memory = config.parallel_workers * 50 * 1024 * 1024; // ~50MB per worker
        let total_memory_mb = (cache_memory + worker_memory) / (1024 * 1024);

        result.performance_impact.memory_usage_estimate = format!("~{}MB", total_memory_mb);
    }

    /// Generate configuration recommendations
    fn generate_recommendations(config: &FcmConfig, result: &mut ValidationResult) {
        let cpu_cores = num_cpus::get();

        // Parallel workers recommendation
        if config.optimization.parallel_workers != cpu_cores {
            result.recommendations.push(format!(
                "Consider setting parallel_workers to {} (matches CPU cores)",
                cpu_cores
            ));
        }

        // Batch size optimization
        if !(50..=200).contains(&config.optimization.batch_size) {
            result
                .recommendations
                .push("Consider batch_size between 50-200 for optimal performance".to_string());
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
            Err(FcmError::Config(ConfigError::InvalidValue {
                key: "size_string".to_string(),
                value: size_str,
            }))
        }
    }

    /// Apply automatic optimizations to configuration
    pub fn apply_auto_optimizations(config: &mut FcmConfig) {
        let cpu_cores = num_cpus::get();

        // Optimize parallel workers
        if config.optimization.parallel_workers == 1 {
            config.optimization.parallel_workers = cpu_cores;
        }
    }
}

impl Default for PerformanceImpact {
    fn default() -> Self {
        Self {
            parallel_efficiency_estimate: 0.0,
            memory_usage_estimate: "Unknown".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FcmConfig;

    #[test]
    fn test_config_optimization() {
        let config = FcmConfig::default();
        let result = ConfigValidator::validate_config(&config);
        assert!(result.is_valid);
    }

    #[test]
    fn test_config_validation_failures() {
        let mut config = FcmConfig::default();
        config.optimization.parallel_workers = 0;

        let result = ConfigValidator::validate_config(&config);
        assert!(!result.is_valid);
        assert!(!result.errors.is_empty());
    }

    #[test]
    fn test_size_string_parsing() {
        assert_eq!(
            ConfigValidator::parse_size_string("1GB").unwrap(),
            1_073_741_824
        );
        assert_eq!(
            ConfigValidator::parse_size_string("500MB").unwrap(),
            524_288_000
        );
        assert!(ConfigValidator::parse_size_string("invalid").is_err());
    }

    #[test]
    fn test_auto_optimizations() {
        let mut config = FcmConfig::default();
        config.optimization.parallel_workers = 1;

        ConfigValidator::apply_auto_optimizations(&mut config);
        assert_eq!(config.optimization.parallel_workers, num_cpus::get());
    }
}
