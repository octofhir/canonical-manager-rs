//! Progress tracking for package installation operations.
//!
//! This module provides a generic progress callback system for tracking
//! multi-package installations with dependencies. It's designed to be
//! used by both CLI applications and web servers (via SSE/WebSocket).

use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Event types for package installation progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InstallEvent {
    /// Installation started
    Started {
        /// Total number of packages to install (including dependencies)
        total_packages: usize,
    },
    /// Resolving package dependencies
    ResolvingDependencies { package: String, version: String },
    /// Dependencies resolved, listing what will be installed
    DependenciesResolved {
        /// List of all packages that will be installed (name@version)
        packages: Vec<String>,
    },
    /// Starting to download a package
    DownloadStarted {
        package: String,
        version: String,
        /// Current package index (1-based)
        current: usize,
        /// Total packages to download
        total: usize,
        /// Total size in bytes if known
        total_bytes: Option<u64>,
    },
    /// Download progress for a package
    DownloadProgress {
        package: String,
        version: String,
        /// Bytes downloaded so far
        downloaded_bytes: u64,
        /// Total bytes if known
        total_bytes: Option<u64>,
        /// Download percentage (0-100)
        percent: u8,
    },
    /// Package download completed
    DownloadCompleted { package: String, version: String },
    /// Extracting a package
    Extracting {
        package: String,
        version: String,
        current: usize,
        total: usize,
    },
    /// Package extracted
    Extracted {
        package: String,
        version: String,
        /// Number of resources in the package
        resource_count: usize,
    },
    /// Storing resources in database
    Storing {
        package: String,
        version: String,
        current: usize,
        total: usize,
    },
    /// Package fully installed
    PackageInstalled {
        package: String,
        version: String,
        /// Number of resources indexed
        resource_count: usize,
    },
    /// All packages installed successfully
    Completed {
        /// Total packages installed
        total_installed: usize,
        /// Total resources indexed
        total_resources: usize,
        /// Duration in milliseconds
        duration_ms: u64,
    },
    /// An error occurred
    Error {
        package: Option<String>,
        version: Option<String>,
        message: String,
    },
    /// Package was skipped (already installed)
    Skipped {
        package: String,
        version: String,
        reason: String,
    },
}

/// Trait for receiving installation progress events.
///
/// Implementors can handle progress events for UI updates, logging,
/// or streaming to clients via SSE/WebSocket.
pub trait InstallProgressCallback: Send + Sync {
    /// Called when a progress event occurs.
    fn on_event(&self, event: InstallEvent);
}

/// A simple callback that stores events in a vector.
/// Useful for testing or collecting all events.
#[derive(Default)]
pub struct CollectingCallback {
    events: std::sync::Mutex<Vec<InstallEvent>>,
}

impl CollectingCallback {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn events(&self) -> Vec<InstallEvent> {
        self.events.lock().unwrap().clone()
    }
}

impl InstallProgressCallback for CollectingCallback {
    fn on_event(&self, event: InstallEvent) {
        self.events.lock().unwrap().push(event);
    }
}

/// A callback that wraps a function.
pub struct FnCallback<F: Fn(InstallEvent) + Send + Sync> {
    callback: F,
}

impl<F: Fn(InstallEvent) + Send + Sync> FnCallback<F> {
    pub fn new(callback: F) -> Self {
        Self { callback }
    }
}

impl<F: Fn(InstallEvent) + Send + Sync> InstallProgressCallback for FnCallback<F> {
    fn on_event(&self, event: InstallEvent) {
        (self.callback)(event);
    }
}

/// A callback that sends events through a tokio broadcast channel.
/// Useful for SSE streaming to multiple clients.
pub struct BroadcastCallback {
    sender: tokio::sync::broadcast::Sender<InstallEvent>,
}

impl BroadcastCallback {
    pub fn new(capacity: usize) -> (Self, tokio::sync::broadcast::Receiver<InstallEvent>) {
        let (sender, receiver) = tokio::sync::broadcast::channel(capacity);
        (Self { sender }, receiver)
    }

    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<InstallEvent> {
        self.sender.subscribe()
    }
}

impl InstallProgressCallback for BroadcastCallback {
    fn on_event(&self, event: InstallEvent) {
        // Ignore send errors (no subscribers)
        let _ = self.sender.send(event);
    }
}

/// A callback that sends events through a tokio mpsc channel.
/// Useful for single-consumer scenarios.
pub struct ChannelCallback {
    sender: tokio::sync::mpsc::UnboundedSender<InstallEvent>,
}

impl ChannelCallback {
    pub fn new() -> (Self, tokio::sync::mpsc::UnboundedReceiver<InstallEvent>) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        (Self { sender }, receiver)
    }
}

impl InstallProgressCallback for ChannelCallback {
    fn on_event(&self, event: InstallEvent) {
        let _ = self.sender.send(event);
    }
}

/// Context for tracking installation progress.
/// Holds state and callback reference.
pub struct InstallProgressContext {
    /// The callback for receiving events
    pub callback: Arc<dyn InstallProgressCallback>,
    total_packages: usize,
    current_package: usize,
    start_time: std::time::Instant,
    total_resources: usize,
}

impl InstallProgressContext {
    pub fn new(callback: Arc<dyn InstallProgressCallback>) -> Self {
        Self {
            callback,
            total_packages: 0,
            current_package: 0,
            start_time: std::time::Instant::now(),
            total_resources: 0,
        }
    }

    pub fn emit(&self, event: InstallEvent) {
        self.callback.on_event(event);
    }

    pub fn started(&mut self, total_packages: usize) {
        self.total_packages = total_packages;
        self.current_package = 0;
        self.start_time = std::time::Instant::now();
        self.emit(InstallEvent::Started { total_packages });
    }

    pub fn resolving_dependencies(&self, package: &str, version: &str) {
        self.emit(InstallEvent::ResolvingDependencies {
            package: package.to_string(),
            version: version.to_string(),
        });
    }

    pub fn dependencies_resolved(&self, packages: Vec<String>) {
        self.emit(InstallEvent::DependenciesResolved { packages });
    }

    pub fn download_started(&mut self, package: &str, version: &str, total_bytes: Option<u64>) {
        self.current_package += 1;
        self.emit(InstallEvent::DownloadStarted {
            package: package.to_string(),
            version: version.to_string(),
            current: self.current_package,
            total: self.total_packages,
            total_bytes,
        });
    }

    pub fn download_progress(
        &self,
        package: &str,
        version: &str,
        downloaded: u64,
        total: Option<u64>,
    ) {
        let percent = if let Some(total) = total {
            if total > 0 {
                ((downloaded as f64 / total as f64) * 100.0).min(100.0) as u8
            } else {
                0
            }
        } else {
            0
        };

        self.emit(InstallEvent::DownloadProgress {
            package: package.to_string(),
            version: version.to_string(),
            downloaded_bytes: downloaded,
            total_bytes: total,
            percent,
        });
    }

    pub fn download_completed(&self, package: &str, version: &str) {
        self.emit(InstallEvent::DownloadCompleted {
            package: package.to_string(),
            version: version.to_string(),
        });
    }

    pub fn extracting(&self, package: &str, version: &str) {
        self.emit(InstallEvent::Extracting {
            package: package.to_string(),
            version: version.to_string(),
            current: self.current_package,
            total: self.total_packages,
        });
    }

    pub fn extracted(&self, package: &str, version: &str, resource_count: usize) {
        self.emit(InstallEvent::Extracted {
            package: package.to_string(),
            version: version.to_string(),
            resource_count,
        });
    }

    pub fn storing(&self, package: &str, version: &str) {
        self.emit(InstallEvent::Storing {
            package: package.to_string(),
            version: version.to_string(),
            current: self.current_package,
            total: self.total_packages,
        });
    }

    pub fn package_installed(&mut self, package: &str, version: &str, resource_count: usize) {
        self.total_resources += resource_count;
        self.emit(InstallEvent::PackageInstalled {
            package: package.to_string(),
            version: version.to_string(),
            resource_count,
        });
    }

    pub fn skipped(&self, package: &str, version: &str, reason: &str) {
        self.emit(InstallEvent::Skipped {
            package: package.to_string(),
            version: version.to_string(),
            reason: reason.to_string(),
        });
    }

    pub fn error(&self, package: Option<&str>, version: Option<&str>, message: &str) {
        self.emit(InstallEvent::Error {
            package: package.map(String::from),
            version: version.map(String::from),
            message: message.to_string(),
        });
    }

    pub fn completed(&self) {
        let duration_ms = self.start_time.elapsed().as_millis() as u64;
        self.emit(InstallEvent::Completed {
            total_installed: self.current_package,
            total_resources: self.total_resources,
            duration_ms,
        });
    }
}
