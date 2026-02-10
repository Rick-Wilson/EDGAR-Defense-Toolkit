//! EDGAR Defense Toolkit - Graphical User Interface
//!
//! Provides a tabbed interface for all toolkit operations:
//! fetch cardplay, anonymize, analyze DD, statistics, and display hand.

use edgar_defense_toolkit::pipeline;
use iced::widget::{
    button, checkbox, column, container, progress_bar, row, rule, scrollable, text, text_input,
};
use iced::{Center, Element, Fill, Task, Theme};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

fn main() -> iced::Result {
    iced::application(App::new, App::update, App::view)
        .theme(App::theme)
        .centered()
        .run()
}

// ============================================================================
// App State
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TabId {
    Welcome,
    Fetch,
    Anonymize,
    Analyze,
    Stats,
    Display,
    Package,
}

use edgar_defense_toolkit::pipeline::CaseFiles;

struct App {
    active_tab: TabId,

    // Welcome / global settings
    case_folder: String,
    case_files: CaseFiles,
    case_usernames: Vec<String>,
    deal_limit_enabled: bool,
    deal_limit_count: String,

    // Fetch Cardplay tab
    fetch_input: String,
    fetch_output: String,
    fetch_delay: String,
    fetch_batch_size: String,
    fetch_batch_delay: String,
    fetch_resume: bool,
    fetch_advanced_open: bool,
    fetch_row_count: Option<usize>,

    // Anonymize tab
    anon_input: String,
    anon_output: String,
    anon_map: String,

    // Analyze DD tab
    analyze_input: String,
    analyze_output: String,
    analyze_threads: String,
    analyze_checkpoint: String,
    analyze_resume: bool,
    analyze_advanced_open: bool,

    // Stats tab
    stats_input: String,
    stats_top_n: String,
    stats_result: String,

    // Display Hand tab
    display_input: String,
    display_row: String,
    display_result: String,

    // Package (Welcome tab)
    package_output: String,
    package_status: String,

    // Task state
    fetch_cancel: Arc<AtomicBool>,
    is_running: bool,
    running_tab: Option<TabId>,
    progress: f32,
    progress_total: usize,
    progress_completed: usize,
    progress_errors: usize,
    progress_skipped: usize,
    fetch_start_time: Option<Instant>,
    status_text: String,
    log_lines: Vec<String>,
}

impl App {
    fn theme(&self) -> Theme {
        Theme::Dark
    }

    fn new() -> (Self, Task<Message>) {
        let cfg = load_config();
        let saved_folder = cfg.case_folder.clone();
        let mut app = App {
            active_tab: TabId::Welcome,
            case_folder: String::new(),
            case_files: CaseFiles::default(),
            case_usernames: Vec::new(),
            deal_limit_enabled: cfg.deal_limit_enabled,
            deal_limit_count: cfg.deal_limit_count,
            fetch_input: String::new(),
            fetch_output: String::new(),
            fetch_delay: "20".to_string(),
            fetch_batch_size: "100".to_string(),
            fetch_batch_delay: "500".to_string(),
            fetch_resume: false,
            fetch_advanced_open: false,
            fetch_row_count: None,
            anon_input: String::new(),
            anon_output: String::new(),
            anon_map: String::new(),
            analyze_input: String::new(),
            analyze_output: String::new(),
            analyze_threads: String::new(),
            analyze_checkpoint: "100".to_string(),
            analyze_resume: false,
            analyze_advanced_open: false,
            stats_input: String::new(),
            stats_top_n: "10".to_string(),
            stats_result: String::new(),
            display_input: String::new(),
            display_row: "1".to_string(),
            display_result: String::new(),
            package_output: String::new(),
            package_status: String::new(),
            fetch_cancel: Arc::new(AtomicBool::new(false)),
            is_running: false,
            running_tab: None,
            progress: 0.0,
            progress_total: 0,
            progress_completed: 0,
            progress_errors: 0,
            progress_skipped: 0,
            fetch_start_time: None,
            status_text: String::new(),
            log_lines: Vec::new(),
        };

        // Restore last case folder from config
        if !saved_folder.is_empty() {
            let p = PathBuf::from(&saved_folder);
            if p.is_dir() {
                app.case_folder = saved_folder;
                app.case_files = pipeline::scan_case_folder(&p);

                app.case_usernames = if let Some(concise) = &app.case_files.concise_file {
                    pipeline::parse_concise_usernames(concise)
                } else {
                    Vec::new()
                };

                if let Some(csv) = &app.case_files.csv_file {
                    app.fetch_input = csv.display().to_string();
                    app.update_fetch_row_count();
                    app.update_fetch_output();
                    app.anon_input = app.fetch_output.clone();
                    app.update_anon_output();
                    app.analyze_input = app.anon_output.clone();
                    app.update_analyze_output();
                }

                let default_names = ["Bob", "Sally"];
                let map_parts: Vec<String> = app
                    .case_usernames
                    .iter()
                    .zip(default_names.iter())
                    .map(|(user, alias)| format!("{}={}", user, alias))
                    .collect();
                app.anon_map = map_parts.join(",");

                let subject = app
                    .case_files
                    .concise_file
                    .as_deref()
                    .and_then(pipeline::extract_concise_subject)
                    .unwrap_or_else(|| "Report".to_string());
                app.package_output = format!(
                    "{}/EDGAR Defense/EDGAR Defense {}.xlsx",
                    p.display(),
                    subject
                );
            }
        }

        (app, Task::none())
    }

    /// Shorten an absolute path to show from the case folder's parent on down.
    fn shorten_path(&self, full: &str) -> String {
        let parent = Path::new(&self.case_folder)
            .parent()
            .and_then(|p| p.to_str())
            .unwrap_or("");
        if !parent.is_empty() && full.starts_with(parent) {
            full[parent.len()..].trim_start_matches('/').to_string()
        } else {
            full.to_string()
        }
    }

    /// Get the current deal limit if enabled and valid.
    fn deal_limit(&self) -> Option<usize> {
        if self.deal_limit_enabled {
            self.deal_limit_count.parse().ok()
        } else {
            None
        }
    }

    /// Count rows in the fetch input CSV and store the result.
    fn update_fetch_row_count(&mut self) {
        if self.fetch_input.is_empty() {
            self.fetch_row_count = None;
            return;
        }
        self.fetch_row_count = pipeline::count_csv_rows(Path::new(&self.fetch_input)).ok();
    }

    /// Get the effective number of boards for fetch (with deal limit applied).
    fn fetch_board_count(&self) -> Option<usize> {
        let total = self.fetch_row_count?;
        if let Some(limit) = self.deal_limit() {
            Some(total.min(limit))
        } else {
            Some(total)
        }
    }

    /// Recompute fetch output path in the EDGAR Defense folder.
    fn update_fetch_output(&mut self) {
        if self.fetch_input.is_empty() || self.case_folder.is_empty() {
            return;
        }
        let edgar_dir = Path::new(&self.case_folder).join("EDGAR Defense");
        let csv_stem = Path::new(&self.fetch_input)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("output");
        // Strip "Hand Records " prefix if present
        let base = csv_stem.strip_prefix("Hand Records ").unwrap_or(csv_stem);
        // Strip existing " cardplay" or " cardplay NNN" suffix to avoid duplication
        let base = regex::Regex::new(r" cardplay( \d+)?$")
            .ok()
            .and_then(|re| re.find(base).map(|m| &base[..m.start()]))
            .unwrap_or(base);
        let filename = match self.deal_limit() {
            Some(n) => format!("{} cardplay {}.csv", base, n),
            None => format!("{} cardplay.csv", base),
        };
        self.fetch_output = edgar_dir.join(filename).display().to_string();
    }

    /// Recompute anonymize output from anonymize input.
    fn update_anon_output(&mut self) {
        if !self.anon_input.is_empty() {
            self.anon_output = add_suffix_to_filename(&self.anon_input, "anon");
        }
    }

    /// Recompute analyze output from analyze input.
    fn update_analyze_output(&mut self) {
        if !self.analyze_input.is_empty() {
            self.analyze_output = derive_analyze_output(&self.analyze_input);
        }
    }
}

// ============================================================================
// Messages
// ============================================================================

#[derive(Debug, Clone)]
enum Message {
    // Tab navigation
    TabSelected(TabId),

    // Welcome / case folder
    BrowseFolder,
    FolderSelected(Option<PathBuf>),
    DealLimitToggled(bool),
    DealLimitChanged(String),

    // Package (Welcome tab)
    PackageStart,
    PackageCompleted(Result<String, String>),
    OpenPackage,

    // Fetch tab
    FetchDelayChanged(String),
    FetchBatchSizeChanged(String),
    FetchBatchDelayChanged(String),
    FetchResumeToggled(bool),
    ToggleFetchAdvanced,
    FetchStart,
    FetchCancel,

    // Anonymize tab
    AnonMapChanged(String),
    AnonStart,
    AnonCancel,

    // Analyze tab
    AnalyzeThreadsChanged(String),
    AnalyzeCheckpointChanged(String),
    AnalyzeResumeToggled(bool),
    ToggleAnalyzeAdvanced,
    AnalyzeStart,
    AnalyzeCancel,

    // Stats tab
    StatsTopNChanged(String),
    StatsRun,
    StatsCompleted(Result<String, String>),

    // Display Hand tab
    DisplayRowChanged(String),
    DisplayShow,
    DisplayCompleted(Result<String, String>),

    // Background task progress
    ProgressUpdate {
        completed: usize,
        total: usize,
        errors: usize,
        skipped: usize,
        phase: String,
    },
    TaskFinished(Result<String, String>),
}

// ============================================================================
// Update
// ============================================================================

impl App {
    fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            // -- Tab navigation --
            Message::TabSelected(tab) => {
                self.active_tab = tab;
                Task::none()
            }

            // -- Case folder --
            Message::BrowseFolder => Task::perform(
                async {
                    let folder = rfd::AsyncFileDialog::new()
                        .set_title("Select EDGAR case folder")
                        .pick_folder()
                        .await;
                    folder.map(|f| f.path().to_path_buf())
                },
                Message::FolderSelected,
            ),

            Message::FolderSelected(path) => {
                if let Some(p) = path {
                    self.case_folder = p.display().to_string();
                    save_config(
                        self.deal_limit_enabled,
                        &self.deal_limit_count,
                        &self.case_folder,
                    );
                    self.case_files = pipeline::scan_case_folder(&p);

                    // Create EDGAR Defense subfolder
                    let edgar_dir = p.join("EDGAR Defense");
                    let _ = std::fs::create_dir_all(&edgar_dir);

                    // Parse subject usernames from concise report
                    self.case_usernames = if let Some(concise) = &self.case_files.concise_file {
                        pipeline::parse_concise_usernames(concise)
                    } else {
                        Vec::new()
                    };

                    // Wire found CSV to Fetch input, output to EDGAR Defense folder
                    if let Some(csv) = &self.case_files.csv_file {
                        self.fetch_input = csv.display().to_string();
                        self.update_fetch_row_count();
                        self.update_fetch_output();
                        // Pre-populate anonymize input from fetch output
                        self.anon_input = self.fetch_output.clone();
                        self.update_anon_output();
                        // Pre-populate analyze input from anonymize output
                        self.analyze_input = self.anon_output.clone();
                        self.update_analyze_output();
                    }

                    // Default mappings: first subject = Bob, second = Sally
                    let default_names = ["Bob", "Sally"];
                    let map_parts: Vec<String> = self
                        .case_usernames
                        .iter()
                        .zip(default_names.iter())
                        .map(|(user, alias)| format!("{}={}", user, alias))
                        .collect();
                    self.anon_map = map_parts.join(",");

                    // Auto-derive package output path
                    let subject = self
                        .case_files
                        .concise_file
                        .as_deref()
                        .and_then(pipeline::extract_concise_subject)
                        .unwrap_or_else(|| "Report".to_string());
                    self.package_output = format!(
                        "{}/EDGAR Defense/EDGAR Defense {}.xlsx",
                        p.display(),
                        subject
                    );
                    self.package_status.clear();
                }
                Task::none()
            }

            // -- Deal limit --
            Message::DealLimitToggled(v) => {
                self.deal_limit_enabled = v;
                save_config(
                    self.deal_limit_enabled,
                    &self.deal_limit_count,
                    &self.case_folder,
                );
                self.update_fetch_output();
                Task::none()
            }
            Message::DealLimitChanged(v) => {
                self.deal_limit_count = v;
                save_config(
                    self.deal_limit_enabled,
                    &self.deal_limit_count,
                    &self.case_folder,
                );
                self.update_fetch_output();
                Task::none()
            }

            // -- Package --
            Message::PackageStart => {
                let csv = match &self.case_files.csv_file {
                    Some(p) => p.clone(),
                    None => return Task::none(),
                };
                let hotspot = match &self.case_files.hotspot_file {
                    Some(p) => p.clone(),
                    None => return Task::none(),
                };
                let concise = match &self.case_files.concise_file {
                    Some(p) => p.clone(),
                    None => return Task::none(),
                };
                // Use cardplay CSV if it exists on disk
                let cardplay_path = PathBuf::from(&self.fetch_output);
                let cardplay_file = if cardplay_path.exists() {
                    Some(cardplay_path)
                } else {
                    None
                };
                let config = pipeline::PackageConfig {
                    csv_file: csv,
                    hotspot_file: hotspot,
                    concise_file: concise,
                    output: PathBuf::from(&self.package_output),
                    case_folder: self.case_folder.clone(),
                    subject_players: self.case_usernames.clone(),
                    deal_limit: self.deal_limit(),
                    cardplay_file,
                };

                // Check for anon files
                let edgar_dir = Path::new(&self.case_folder).join("EDGAR Defense");
                let anon_files = pipeline::find_anon_files(&edgar_dir, &self.case_files);
                let anon_config = anon_files.map(|af| {
                    let subject = self
                        .case_files
                        .concise_file
                        .as_deref()
                        .and_then(pipeline::extract_concise_subject)
                        .unwrap_or_else(|| "Report".to_string());
                    let anon_output =
                        edgar_dir.join(format!("EDGAR Defense {} anon.xlsx", subject));
                    pipeline::PackageConfig {
                        csv_file: af.csv_file,
                        hotspot_file: af.hotspot_file,
                        concise_file: af.concise_file,
                        output: anon_output,
                        case_folder: self.case_folder.clone(),
                        subject_players: self.case_usernames.clone(),
                        deal_limit: self.deal_limit(),
                        cardplay_file: None,
                    }
                });

                self.is_running = true;
                self.running_tab = Some(TabId::Package);
                self.package_status = "Creating workbook...".to_string();
                Task::perform(
                    async move {
                        let mut summary =
                            pipeline::package_workbook(&config).map_err(|e| e.to_string())?;
                        if let Some(anon_cfg) = &anon_config {
                            match pipeline::package_workbook(anon_cfg) {
                                Ok(s) => {
                                    summary.push('\n');
                                    summary.push_str(&s);
                                }
                                Err(e) => {
                                    summary.push_str(&format!("\nAnon workbook error: {}", e));
                                }
                            }
                        }
                        Ok(summary)
                    },
                    Message::PackageCompleted,
                )
            }
            Message::PackageCompleted(result) => {
                self.is_running = false;
                self.running_tab = None;
                match result {
                    Ok(s) => self.package_status = s,
                    Err(e) => self.package_status = format!("Error: {}", e),
                }
                Task::none()
            }
            Message::OpenPackage => {
                let path = self.package_output.clone();
                if !path.is_empty() {
                    let _ = std::process::Command::new("open").arg(&path).spawn();
                }
                Task::none()
            }

            // -- Fetch tab --
            Message::FetchDelayChanged(v) => {
                self.fetch_delay = v;
                Task::none()
            }
            Message::FetchBatchSizeChanged(v) => {
                self.fetch_batch_size = v;
                Task::none()
            }
            Message::FetchBatchDelayChanged(v) => {
                self.fetch_batch_delay = v;
                Task::none()
            }
            Message::FetchResumeToggled(v) => {
                self.fetch_resume = v;
                Task::none()
            }
            Message::ToggleFetchAdvanced => {
                self.fetch_advanced_open = !self.fetch_advanced_open;
                Task::none()
            }
            Message::FetchStart => {
                self.is_running = true;
                self.running_tab = Some(TabId::Fetch);
                self.status_text = "Starting fetch...".to_string();
                self.log_lines.clear();
                self.progress = 0.0;
                self.progress_completed = 0;
                self.progress_total = 0;
                self.progress_errors = 0;
                self.progress_skipped = 0;
                self.fetch_start_time = Some(Instant::now());
                self.fetch_cancel.store(false, Ordering::Relaxed);

                let input_path = self.fetch_input.clone();
                let deal_limit = self.deal_limit();
                let cancel = self.fetch_cancel.clone();

                let output_path = PathBuf::from(&self.fetch_output);
                let lookup_output = pipeline::derive_lookup_path(&output_path);
                let config = pipeline::FetchCardplayConfig {
                    input: PathBuf::from(&input_path),
                    output: output_path,
                    lookup_output,
                    url_column: "BBO".to_string(),
                    delay_ms: self.fetch_delay.parse().unwrap_or(20),
                    batch_size: self.fetch_batch_size.parse().unwrap_or(100),
                    batch_delay_ms: self.fetch_batch_delay.parse().unwrap_or(500),
                    resume: self.fetch_resume,
                };

                Task::run(fetch_cardplay_stream(config, deal_limit, cancel), |msg| msg)
            }
            Message::FetchCancel => {
                self.fetch_cancel.store(true, Ordering::Relaxed);
                // Don't immediately reset state — let TaskFinished handle cleanup
                self.status_text = "Cancelling...".to_string();
                Task::none()
            }

            // -- Anonymize tab --
            Message::AnonMapChanged(v) => {
                self.anon_map = v;
                Task::none()
            }
            Message::AnonStart => {
                // Derive output paths
                let csv_output = add_suffix_to_filename(&self.anon_input, "anon");
                self.anon_output = csv_output.clone();

                // Key from subject name
                let key = self
                    .case_files
                    .concise_file
                    .as_deref()
                    .and_then(pipeline::extract_concise_subject)
                    .unwrap_or_else(|| "default".to_string());

                let edgar_dir = Path::new(&self.case_folder).join("EDGAR Defense");

                let concise_input = self.case_files.concise_file.clone();
                let concise_output = concise_input.as_ref().map(|p| {
                    let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or("concise");
                    edgar_dir.join(format!("{} anon.txt", stem))
                });

                let hotspot_input = self.case_files.hotspot_file.clone();
                let hotspot_output = hotspot_input.as_ref().map(|p| {
                    let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or("hotspot");
                    edgar_dir.join(format!("{} anon.txt", stem))
                });

                let config = pipeline::AnonymizeAllConfig {
                    csv_input: PathBuf::from(&self.anon_input),
                    csv_output: PathBuf::from(&csv_output),
                    key,
                    map: self.anon_map.clone(),
                    columns: "N,S,E,W,Ob name,Dec name,Leader"
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .collect(),
                    concise_input,
                    concise_output,
                    hotspot_input,
                    hotspot_output,
                };
                self.is_running = true;
                self.running_tab = Some(TabId::Anonymize);
                self.status_text = "Anonymizing...".to_string();
                self.progress = 0.0;
                self.progress_completed = 0;
                self.progress_total = 0;
                self.progress_errors = 0;
                self.progress_skipped = 0;
                self.fetch_cancel.store(false, Ordering::Relaxed);

                let cancel = self.fetch_cancel.clone();
                Task::run(anonymize_stream(config, cancel), |msg| msg)
            }
            Message::AnonCancel => {
                self.fetch_cancel.store(true, Ordering::Relaxed);
                self.status_text = "Cancelling...".to_string();
                Task::none()
            }

            // -- Analyze tab --
            Message::AnalyzeThreadsChanged(v) => {
                self.analyze_threads = v;
                Task::none()
            }
            Message::AnalyzeCheckpointChanged(v) => {
                self.analyze_checkpoint = v;
                Task::none()
            }
            Message::AnalyzeResumeToggled(v) => {
                self.analyze_resume = v;
                Task::none()
            }
            Message::ToggleAnalyzeAdvanced => {
                self.analyze_advanced_open = !self.analyze_advanced_open;
                Task::none()
            }
            Message::AnalyzeStart => {
                self.is_running = true;
                self.running_tab = Some(TabId::Analyze);
                self.status_text = "Running DD analysis...".to_string();
                self.log_lines.clear();
                self.progress = 0.0;
                self.progress_completed = 0;
                self.progress_total = 0;
                self.progress_errors = 0;
                self.progress_skipped = 0;
                self.fetch_start_time = Some(Instant::now());
                self.fetch_cancel.store(false, Ordering::Relaxed);

                let cancel = self.fetch_cancel.clone();
                let config = pipeline::AnalyzeDdConfig {
                    input: PathBuf::from(&self.analyze_input),
                    output: PathBuf::from(&self.analyze_output),
                    threads: self.analyze_threads.parse().ok(),
                    resume: self.analyze_resume,
                    checkpoint_interval: self.analyze_checkpoint.parse().unwrap_or(100),
                };

                Task::run(analyze_dd_stream(config, cancel), |msg| msg)
            }
            Message::AnalyzeCancel => {
                self.fetch_cancel.store(true, Ordering::Relaxed);
                self.status_text = "Cancelling...".to_string();
                Task::none()
            }

            // -- Stats tab --
            Message::StatsTopNChanged(v) => {
                self.stats_top_n = v;
                Task::none()
            }
            Message::StatsRun => {
                let input = self.stats_input.clone();
                let top_n = self.stats_top_n.clone();
                self.stats_result = "Computing statistics...".to_string();
                Task::perform(
                    async move {
                        let top_n: usize = top_n.parse().unwrap_or(10);
                        pipeline::compute_stats(std::path::Path::new(&input), top_n)
                            .map_err(|e| e.to_string())
                    },
                    Message::StatsCompleted,
                )
            }
            Message::StatsCompleted(result) => {
                self.stats_result = match result {
                    Ok(s) => s,
                    Err(e) => format!("Error: {}", e),
                };
                Task::none()
            }

            // -- Display Hand tab --
            Message::DisplayRowChanged(v) => {
                self.display_row = v;
                Task::none()
            }
            Message::DisplayShow => {
                let input = self.display_input.clone();
                let row = self.display_row.clone();
                self.display_result = "Loading...".to_string();
                Task::perform(
                    async move {
                        let row_num: usize = row
                            .parse()
                            .map_err(|_| format!("Invalid row number: {}", row))?;
                        pipeline::display_hand(std::path::Path::new(&input), row_num)
                            .map_err(|e| e.to_string())
                    },
                    Message::DisplayCompleted,
                )
            }
            Message::DisplayCompleted(result) => {
                self.display_result = match result {
                    Ok(s) => s,
                    Err(e) => format!("Error: {}", e),
                };
                Task::none()
            }

            // -- Background task progress --
            Message::ProgressUpdate {
                completed,
                total,
                errors,
                skipped,
                phase,
            } => {
                self.progress_completed = completed;
                self.progress_total = total;
                self.progress_errors = errors;
                self.progress_skipped = skipped;
                self.progress = if total > 0 {
                    completed as f32 / total as f32
                } else {
                    0.0
                };
                if !phase.is_empty() {
                    self.status_text = phase;
                }
                Task::none()
            }
            Message::TaskFinished(result) => {
                self.is_running = false;
                self.fetch_start_time = None;
                let finished_tab = self.running_tab.take();
                match &result {
                    Ok(s) => {
                        self.status_text = s.clone();
                        for line in s.lines() {
                            self.log_lines.push(line.to_string());
                        }
                    }
                    Err(e) => {
                        self.status_text = format!("Error: {}", e);
                        self.log_lines.push(format!("ERROR: {}", e));
                    }
                }
                // Chain outputs to next stage inputs
                if result.is_ok() {
                    match finished_tab {
                        Some(TabId::Fetch) => {
                            self.anon_input = self.fetch_output.clone();
                            self.update_anon_output();
                        }
                        Some(TabId::Anonymize) => {
                            self.analyze_input = self.anon_output.clone();
                            self.update_analyze_output();
                        }
                        Some(TabId::Analyze) => {
                            self.stats_input = self.analyze_output.clone();
                            self.display_input = self.analyze_output.clone();
                        }
                        _ => {}
                    }
                }
                // For stats tab, put the output in stats_result
                if finished_tab == Some(TabId::Stats) {
                    if let Ok(s) = &result {
                        self.stats_result = s.clone();
                    }
                }
                self.progress = 0.0;
                Task::none()
            }
        }
    }
}

// ============================================================================
// View
// ============================================================================

impl App {
    fn view(&self) -> Element<'_, Message> {
        let tab_bar = row![
            tab_button("Welcome", TabId::Welcome, self.active_tab),
            tab_button("Fetch Cardplay", TabId::Fetch, self.active_tab),
            tab_button("Anonymize", TabId::Anonymize, self.active_tab),
            tab_button("Analyze DD", TabId::Analyze, self.active_tab),
            tab_button("Statistics", TabId::Stats, self.active_tab),
            tab_button("Display Hand", TabId::Display, self.active_tab),
            tab_button("Package", TabId::Package, self.active_tab),
        ]
        .spacing(4);

        let content: Element<'_, Message> = match self.active_tab {
            TabId::Welcome => self.view_welcome_tab(),
            TabId::Fetch => self.view_fetch_tab(),
            TabId::Anonymize => self.view_anonymize_tab(),
            TabId::Analyze => self.view_analyze_tab(),
            TabId::Stats => self.view_stats_tab(),
            TabId::Display => self.view_display_tab(),
            TabId::Package => self.view_package_tab(),
        };

        let body = container(content).padding(20).width(Fill).height(Fill);

        column![
            container(tab_bar).padding([10, 20]),
            rule::horizontal(1),
            body,
        ]
        .into()
    }

    // -- Welcome tab --
    fn view_welcome_tab(&self) -> Element<'_, Message> {
        let title = text("EDGAR Defense Toolkit").size(28);
        let subtitle = text("Error Detection for Game Analysis and Review").size(14);

        // Case folder picker
        let folder_section = column![
            text("Case Folder").size(16),
            row![
                text_input(
                    "Select a folder containing EDGAR report files...",
                    &self.case_folder
                )
                .width(Fill),
                button(text("Browse").size(13)).on_press(Message::BrowseFolder),
            ]
            .spacing(10)
            .align_y(Center),
        ]
        .spacing(8);

        // File table and subject usernames side by side
        let case_info: Element<'_, Message> = if !self.case_folder.is_empty() {
            let csv_row = file_status_row("Hand Records (CSV)", &self.case_files.csv_file);
            let concise_row = file_status_row("Concise Report", &self.case_files.concise_file);
            let hotspot_row = file_status_row("Hotspot Report", &self.case_files.hotspot_file);

            let file_table = column![
                row![
                    text("File Type").size(13).width(180),
                    text("Filename").size(13).width(Fill),
                ]
                .spacing(10),
                rule::horizontal(1),
                csv_row,
                concise_row,
                hotspot_row,
            ]
            .spacing(4)
            .width(Fill);

            let mut username_rows: Vec<Element<'_, Message>> = Vec::new();
            username_rows.push(text("Subject Players").size(13).into());
            username_rows.push(rule::horizontal(1).into());
            if self.case_usernames.is_empty() {
                username_rows.push(
                    text("-- none detected --")
                        .size(13)
                        .color(iced::Color::from_rgb(0.6, 0.6, 0.6))
                        .into(),
                );
            } else {
                for name in &self.case_usernames {
                    username_rows.push(
                        text(name)
                            .size(13)
                            .color(iced::Color::from_rgb(0.4, 0.9, 0.4))
                            .into(),
                    );
                }
            }
            let username_table = column(username_rows).spacing(4).width(200);

            row![file_table, username_table].spacing(20).into()
        } else {
            column![].into()
        };

        // Workflow summary
        let workflow = column![
            rule::horizontal(1),
            text("Workflow:").size(16),
            text("  1. Fetch Cardplay - Download cardplay data from BBO hand records").size(13),
            text("  2. Anonymize - Replace player names with anonymous identifiers").size(13),
            text("  3. Analyze DD - Run double-dummy analysis on each card played").size(13),
            text("  4. Statistics - Compute error rates by player and role").size(13),
            text("  5. Display Hand - Inspect individual hands with DD costs").size(13),
            text("  6. Package - Create Excel workbook combining all case data").size(13),
        ]
        .spacing(6);

        // Deal limit
        let deal_limit_section = column![
            rule::horizontal(1),
            text("Testing Options").size(16),
            row![
                checkbox(self.deal_limit_enabled)
                    .label("Limit to first N deals (for testing)")
                    .on_toggle(Message::DealLimitToggled),
                text_input("1000", &self.deal_limit_count)
                    .on_input_maybe(if self.deal_limit_enabled {
                        Some(Message::DealLimitChanged)
                    } else {
                        None
                    })
                    .width(80),
                text("deals").size(13),
            ]
            .spacing(10)
            .align_y(Center),
        ]
        .spacing(10);

        column![
            title,
            subtitle,
            folder_section,
            case_info,
            workflow,
            deal_limit_section,
        ]
        .spacing(16)
        .into()
    }

    // -- Fetch Cardplay tab --
    fn view_fetch_tab(&self) -> Element<'_, Message> {
        let disabled = self.is_running;

        // Board count and estimated duration
        let estimate_note = if let Some(boards) = self.fetch_board_count() {
            // Estimate: ~180ms network overhead + delay_ms per request,
            // plus batch_delay_ms every batch_size requests
            let delay_ms: f64 = self.fetch_delay.parse().unwrap_or(20.0);
            let batch_size: f64 = self.fetch_batch_size.parse().unwrap_or(100.0);
            let batch_delay_ms: f64 = self.fetch_batch_delay.parse().unwrap_or(500.0);
            let per_board_ms = delay_ms + 180.0; // 180ms empirical network overhead
            let batches = (boards as f64 / batch_size).floor();
            let total_secs = (boards as f64 * per_board_ms + batches * batch_delay_ms) / 1000.0;
            let mins = (total_secs / 60.0).floor() as u64;
            let secs = (total_secs % 60.0).round() as u64;

            let limit_text = if self.deal_limit().is_some() {
                format!(
                    "{} boards (limited from {})  ~  {} min {} sec",
                    boards,
                    self.fetch_row_count.unwrap_or(0),
                    mins,
                    secs
                )
            } else {
                format!("{} boards  ~  {} min {} sec", boards, mins, secs)
            };
            column![text(limit_text).size(12)]
        } else {
            column![]
        };

        let fetch_input_short = self.shorten_path(&self.fetch_input);
        let fetch_output_short = self.shorten_path(&self.fetch_output);
        let lookup_path = pipeline::derive_lookup_path(&PathBuf::from(&self.fetch_output));
        let lookup_short = self.shorten_path(&lookup_path.display().to_string());
        let form = column![
            text("Fetch cardplay data from BBO TinyURLs, and save into a CSV file.").size(14),
            estimate_note,
            file_display_row("Input CSV:", &fetch_input_short),
            file_display_row("Lookup CSV:", &lookup_short),
            file_display_row("Output CSV:", &fetch_output_short),
            checkbox(self.fetch_resume)
                .label("Resume from previous run")
                .on_toggle_maybe(if disabled {
                    None
                } else {
                    Some(Message::FetchResumeToggled)
                }),
        ]
        .spacing(12);

        // Collapsible advanced settings
        let advanced_header = button(
            text(if self.fetch_advanced_open {
                "v Advanced Settings"
            } else {
                "> Advanced Settings"
            })
            .size(13),
        )
        .on_press_maybe(if disabled {
            None
        } else {
            Some(Message::ToggleFetchAdvanced)
        })
        .style(button::text);

        let advanced_panel = if self.fetch_advanced_open {
            column![row![
                text("Delay (ms):").width(100),
                text_input("20", &self.fetch_delay)
                    .on_input_maybe(if disabled {
                        None
                    } else {
                        Some(Message::FetchDelayChanged)
                    })
                    .width(70),
                text("Batch Size:").width(90),
                text_input("100", &self.fetch_batch_size)
                    .on_input_maybe(if disabled {
                        None
                    } else {
                        Some(Message::FetchBatchSizeChanged)
                    })
                    .width(70),
                text("Batch Delay (ms):").width(130),
                text_input("500", &self.fetch_batch_delay)
                    .on_input_maybe(if disabled {
                        None
                    } else {
                        Some(Message::FetchBatchDelayChanged)
                    })
                    .width(70),
            ]
            .spacing(10)
            .align_y(Center),]
            .spacing(8)
        } else {
            column![]
        };

        let buttons = if self.is_running && self.running_tab == Some(TabId::Fetch) {
            row![
                button(text("Cancel")).on_press(Message::FetchCancel),
                text(&self.status_text),
            ]
            .spacing(10)
            .align_y(Center)
        } else {
            let mut start = button(text("Start Fetch"));
            if !disabled {
                start = start.on_press(Message::FetchStart);
            }
            let mut r = row![start].spacing(10);
            if !self.status_text.is_empty() {
                r = r.push(text(&self.status_text).size(13));
            }
            r.align_y(Center)
        };

        let progress_section = if self.is_running && self.running_tab == Some(TabId::Fetch) {
            let mut items: Vec<Element<'_, Message>> = Vec::new();
            items.push(progress_bar(0.0..=1.0, self.progress).into());

            let progress_text = format!(
                "{}/{} ({} errors, {} skipped)",
                self.progress_completed,
                self.progress_total,
                self.progress_errors,
                self.progress_skipped
            );
            items.push(text(progress_text).size(13).into());

            // ETA calculation — exclude skipped rows from rate so resume doesn't
            // inflate the speed estimate with instantly-processed cached rows.
            if let Some(start) = self.fetch_start_time {
                let fetched = self
                    .progress_completed
                    .saturating_sub(self.progress_skipped);
                let remaining_items = self.progress_total.saturating_sub(self.progress_completed);

                if fetched > 0 && self.progress_total > 0 {
                    let elapsed = start.elapsed();
                    let rate = fetched as f64 / elapsed.as_secs_f64();
                    let remaining_secs = if rate > 0.0 {
                        remaining_items as f64 / rate
                    } else {
                        0.0
                    };

                    let remaining_dur = std::time::Duration::from_secs_f64(remaining_secs);
                    let remaining_mins = remaining_dur.as_secs() / 60;
                    let remaining_secs_part = remaining_dur.as_secs() % 60;

                    let eta =
                        chrono::Local::now() + chrono::Duration::seconds(remaining_secs as i64);
                    let eta_str = eta.format("%l:%M %p").to_string();

                    let time_text = format!(
                        "~{:>3} min {:>2} sec remaining  |  ETA: {:>8}",
                        remaining_mins,
                        remaining_secs_part,
                        eta_str.trim()
                    );
                    items.push(
                        container(text(time_text).size(13).font(iced::Font::MONOSPACE))
                            .width(Fill)
                            .into(),
                    );
                } else if self.progress_completed > 0 {
                    // Still skipping cached rows — show that we're resuming
                    items.push(
                        text(format!(
                            "Resuming... ({} skipped so far)",
                            self.progress_skipped
                        ))
                        .size(13)
                        .into(),
                    );
                }
            }

            column(items).spacing(4)
        } else {
            column![]
        };

        let log = if !self.log_lines.is_empty() {
            let log_text: String = self
                .log_lines
                .iter()
                .rev()
                .take(50)
                .rev()
                .cloned()
                .collect::<Vec<_>>()
                .join("\n");
            column![
                text("Log:").size(13),
                scrollable(container(text(log_text).size(12)).padding(8)).height(150),
            ]
            .spacing(4)
        } else {
            column![]
        };

        column![
            form,
            advanced_header,
            advanced_panel,
            buttons,
            progress_section,
            log
        ]
        .spacing(10)
        .into()
    }

    // -- Anonymize tab --
    fn view_anonymize_tab(&self) -> Element<'_, Message> {
        let disabled = self.is_running;
        let anon_input_short = self.shorten_path(&self.anon_input);

        let csv_out_short = self.shorten_path(&add_suffix_to_filename(&self.anon_input, "anon"));

        let concise_input_short = self
            .case_files
            .concise_file
            .as_ref()
            .map(|p| self.shorten_path(&p.to_string_lossy()))
            .unwrap_or_default();
        let concise_output_short = self
            .case_files
            .concise_file
            .as_ref()
            .map(|p| {
                let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or("concise");
                let edgar_dir = Path::new(&self.case_folder).join("EDGAR Defense");
                self.shorten_path(&edgar_dir.join(format!("{stem} anon.txt")).to_string_lossy())
            })
            .unwrap_or_default();

        let hotspot_input_short = self
            .case_files
            .hotspot_file
            .as_ref()
            .map(|p| self.shorten_path(&p.to_string_lossy()))
            .unwrap_or_default();
        let hotspot_output_short = self
            .case_files
            .hotspot_file
            .as_ref()
            .map(|p| {
                let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or("hotspot");
                let edgar_dir = Path::new(&self.case_folder).join("EDGAR Defense");
                self.shorten_path(&edgar_dir.join(format!("{stem} anon.txt")).to_string_lossy())
            })
            .unwrap_or_default();

        let form = column![
            text("Anonymize player names across all case files.").size(14),
            file_display_row("Cardplay CSV:", &anon_input_short),
            file_display_row("Concise Report:", &concise_input_short),
            file_display_row("Hotspot Report:", &hotspot_input_short),
            rule::horizontal(1),
            file_display_row("Anon CSV:", &csv_out_short),
            file_display_row("Anon Concise:", &concise_output_short),
            file_display_row("Anon Hotspot:", &hotspot_output_short),
            row![
                text("Mappings:").width(130),
                text_input("player1=Bob,player2=Sally", &self.anon_map)
                    .on_input_maybe(if disabled {
                        None
                    } else {
                        Some(Message::AnonMapChanged)
                    })
                    .width(Fill),
            ]
            .spacing(10)
            .align_y(Center),
        ]
        .spacing(12);

        let buttons = if self.is_running && self.running_tab == Some(TabId::Anonymize) {
            row![
                button(text("Cancel")).on_press(Message::AnonCancel),
                text(&self.status_text),
            ]
            .spacing(10)
            .align_y(Center)
        } else {
            let mut start = button(text("Anonymize"));
            if !disabled && !self.anon_input.is_empty() {
                start = start.on_press(Message::AnonStart);
            }
            let mut r = row![start].spacing(10);
            if !self.status_text.is_empty() {
                r = r.push(text(&self.status_text).size(13));
            }
            r.align_y(Center)
        };

        let progress_section = if self.is_running && self.running_tab == Some(TabId::Anonymize) {
            column![
                progress_bar(0.0..=1.0, self.progress),
                text(format!(
                    "{}/{}",
                    self.progress_completed, self.progress_total
                ))
                .size(13),
            ]
            .spacing(4)
        } else {
            column![]
        };

        column![form, buttons, progress_section].spacing(12).into()
    }

    // -- Analyze DD tab --
    fn view_analyze_tab(&self) -> Element<'_, Message> {
        let disabled = self.is_running;

        let analyze_input_short = self.shorten_path(&self.analyze_input);
        let analyze_output_short = self.shorten_path(&self.analyze_output);
        let form = column![
            text("Run double-dummy analysis on cardplay data.").size(14),
            file_display_row("Input CSV:", &analyze_input_short),
            file_display_row("Output CSV:", &analyze_output_short),
            checkbox(self.analyze_resume)
                .label("Resume from previous run")
                .on_toggle_maybe(if disabled {
                    None
                } else {
                    Some(Message::AnalyzeResumeToggled)
                }),
        ]
        .spacing(12);

        // Collapsible advanced settings
        let advanced_header = button(
            text(if self.analyze_advanced_open {
                "v Advanced Settings"
            } else {
                "> Advanced Settings"
            })
            .size(13),
        )
        .on_press_maybe(if disabled {
            None
        } else {
            Some(Message::ToggleAnalyzeAdvanced)
        })
        .style(button::text);

        let advanced_panel = if self.analyze_advanced_open {
            column![row![
                text("Threads:").width(130),
                text_input("auto", &self.analyze_threads)
                    .on_input_maybe(if disabled {
                        None
                    } else {
                        Some(Message::AnalyzeThreadsChanged)
                    })
                    .width(80),
                text("Checkpoint interval:").width(160),
                text_input("100", &self.analyze_checkpoint)
                    .on_input_maybe(if disabled {
                        None
                    } else {
                        Some(Message::AnalyzeCheckpointChanged)
                    })
                    .width(80),
            ]
            .spacing(10)
            .align_y(Center),]
            .spacing(8)
        } else {
            column![]
        };

        let buttons = if self.is_running && self.running_tab == Some(TabId::Analyze) {
            row![
                button(text("Cancel")).on_press(Message::AnalyzeCancel),
                text(&self.status_text),
            ]
            .spacing(10)
            .align_y(Center)
        } else {
            let mut start = button(text("Start Analysis"));
            if !disabled {
                start = start.on_press(Message::AnalyzeStart);
            }
            let mut r = row![start].spacing(10);
            if !self.status_text.is_empty() {
                r = r.push(text(&self.status_text).size(13));
            }
            r.align_y(Center)
        };

        let progress_section = if self.is_running && self.running_tab == Some(TabId::Analyze) {
            let mut items: Vec<Element<'_, Message>> = Vec::new();
            items.push(progress_bar(0.0..=1.0, self.progress).into());

            let progress_text = format!(
                "{}/{} ({} errors, {} skipped)",
                self.progress_completed,
                self.progress_total,
                self.progress_errors,
                self.progress_skipped
            );
            items.push(text(progress_text).size(13).into());

            // ETA calculation — exclude skipped rows from rate
            if let Some(start) = self.fetch_start_time {
                let processed = self
                    .progress_completed
                    .saturating_sub(self.progress_skipped);
                let remaining_items = self.progress_total.saturating_sub(self.progress_completed);

                if processed > 0 && self.progress_total > 0 {
                    let elapsed = start.elapsed();
                    let rate = processed as f64 / elapsed.as_secs_f64();
                    let remaining_secs = if rate > 0.0 {
                        remaining_items as f64 / rate
                    } else {
                        0.0
                    };

                    let remaining_dur = std::time::Duration::from_secs_f64(remaining_secs);
                    let remaining_mins = remaining_dur.as_secs() / 60;
                    let remaining_secs_part = remaining_dur.as_secs() % 60;

                    let eta =
                        chrono::Local::now() + chrono::Duration::seconds(remaining_secs as i64);
                    let eta_str = eta.format("%l:%M %p").to_string();

                    let time_text = format!(
                        "~{:>3} min {:>2} sec remaining  |  ETA: {:>8}",
                        remaining_mins,
                        remaining_secs_part,
                        eta_str.trim()
                    );
                    items.push(
                        container(text(time_text).size(13).font(iced::Font::MONOSPACE))
                            .width(Fill)
                            .into(),
                    );
                } else if self.progress_completed > 0 {
                    items.push(
                        text(format!(
                            "Resuming... ({} skipped so far)",
                            self.progress_skipped
                        ))
                        .size(13)
                        .into(),
                    );
                }
            }

            column(items).spacing(4)
        } else {
            column![]
        };

        column![
            form,
            advanced_header,
            advanced_panel,
            buttons,
            progress_section
        ]
        .spacing(10)
        .into()
    }

    // -- Statistics tab --
    fn view_stats_tab(&self) -> Element<'_, Message> {
        let disabled = self.is_running;

        let stats_input_short = self.shorten_path(&self.stats_input);
        let form = column![
            text("Compute DD error statistics by player.").size(14),
            file_display_row("Input CSV:", &stats_input_short),
            row![
                text("Top N players:").width(130),
                text_input("10", &self.stats_top_n)
                    .on_input_maybe(if disabled {
                        None
                    } else {
                        Some(Message::StatsTopNChanged)
                    })
                    .width(80),
            ]
            .spacing(10)
            .align_y(Center),
        ]
        .spacing(12);

        let mut run_btn = button(text("Compute Stats"));
        if !disabled {
            run_btn = run_btn.on_press(Message::StatsRun);
        }

        let results = if !self.stats_result.is_empty() {
            column![
                text("Results:").size(13),
                scrollable(
                    container(
                        text(&self.stats_result)
                            .size(12)
                            .font(iced::Font::MONOSPACE)
                    )
                    .padding(8)
                )
                .height(350),
            ]
            .spacing(4)
        } else {
            column![]
        };

        column![form, row![run_btn], results].spacing(16).into()
    }

    // -- Display Hand tab --
    fn view_display_tab(&self) -> Element<'_, Message> {
        let disabled = self.is_running;

        let display_input_short = self.shorten_path(&self.display_input);
        let form = column![
            text("Display a single hand with DD analysis.").size(14),
            file_display_row("Input CSV:", &display_input_short),
            row![
                text("Row #:").width(130),
                text_input("1", &self.display_row)
                    .on_input_maybe(if disabled {
                        None
                    } else {
                        Some(Message::DisplayRowChanged)
                    })
                    .width(80),
            ]
            .spacing(10)
            .align_y(Center),
        ]
        .spacing(12);

        let mut show_btn = button(text("Show Hand"));
        if !disabled {
            show_btn = show_btn.on_press(Message::DisplayShow);
        }

        let results = if !self.display_result.is_empty() {
            column![scrollable(
                container(
                    text(&self.display_result)
                        .size(13)
                        .font(iced::Font::MONOSPACE)
                )
                .padding(8)
            )
            .height(400),]
            .spacing(4)
        } else {
            column![]
        };

        column![form, row![show_btn], results].spacing(16).into()
    }

    // -- Package tab --
    fn view_package_tab(&self) -> Element<'_, Message> {
        let disabled = self.is_running;

        if self.case_files.csv_file.is_none()
            || self.case_files.concise_file.is_none()
            || self.case_files.hotspot_file.is_none()
        {
            return column![
                text("Package").size(20),
                text("Select a case folder on the Welcome tab first. All three files (CSV, Concise Report, Hotspot Report) must be found.")
                    .size(14),
            ]
            .spacing(12)
            .into();
        }

        let mut items: Vec<Element<'_, Message>> = Vec::new();
        items.push(
            text("Create Excel workbook(s) combining all case data.")
                .size(14)
                .into(),
        );

        // File manifest showing what will be created
        let mut manifest: Vec<Element<'_, Message>> = Vec::new();
        manifest.push(text("Files to create:").size(14).into());

        let output_name = Path::new(&self.package_output)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("(unknown)");
        manifest.push(
            text(format!("  {}", output_name))
                .size(12)
                .font(iced::Font::MONOSPACE)
                .into(),
        );

        let edgar_dir = Path::new(&self.case_folder).join("EDGAR Defense");
        let has_anon = pipeline::find_anon_files(&edgar_dir, &self.case_files).is_some();
        if has_anon {
            let subject = self
                .case_files
                .concise_file
                .as_deref()
                .and_then(pipeline::extract_concise_subject)
                .unwrap_or_else(|| "Report".to_string());
            manifest.push(
                text(format!("  EDGAR Defense {} anon.xlsx", subject))
                    .size(12)
                    .font(iced::Font::MONOSPACE)
                    .into(),
            );
        } else {
            manifest.push(
                text("  (anon files not found)")
                    .size(12)
                    .font(iced::Font::MONOSPACE)
                    .color(iced::Color::from_rgb(0.6, 0.6, 0.6))
                    .into(),
            );
        }

        items.push(column(manifest).spacing(4).into());

        let mut pkg_btn = button(text("Create Workbook"));
        if !disabled && !self.package_output.is_empty() {
            pkg_btn = pkg_btn.on_press(Message::PackageStart);
        }
        items.push(row![pkg_btn].into());

        if !self.package_status.is_empty() {
            items.push(text(&self.package_status).size(13).into());
        }

        // Show "Open Workbook" button after successful creation
        if self.package_status.starts_with("Package created") {
            items.push(
                row![button(text("Open Workbook").size(13)).on_press(Message::OpenPackage)].into(),
            );
        }

        column(items).spacing(10).into()
    }
}

// ============================================================================
// Helper widgets
// ============================================================================

/// Render a row in the case file table showing file type, filename, and status.
fn file_status_row<'a>(label: &'a str, file: &Option<PathBuf>) -> Element<'a, Message> {
    let (filename, style_color) = match file {
        Some(path) => {
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("(unknown)");
            (name.to_string(), iced::Color::from_rgb(0.4, 0.9, 0.4))
        }
        None => (
            "-- not found --".to_string(),
            iced::Color::from_rgb(0.6, 0.6, 0.6),
        ),
    };

    row![
        text(label).size(13).width(180),
        text(filename).size(13).color(style_color).width(Fill),
    ]
    .spacing(10)
    .align_y(Center)
    .into()
}

/// Render a tab button, styled differently when active.
fn tab_button(label: &str, tab: TabId, active: TabId) -> Element<'_, Message> {
    let btn = button(text(label).size(14));
    if tab == active {
        btn.style(button::primary).into()
    } else {
        btn.on_press(Message::TabSelected(tab))
            .style(button::secondary)
            .into()
    }
}

/// Render a read-only file path row: label + path in green (or grey if empty).
fn file_display_row<'a>(label: &'a str, path: &str) -> Element<'a, Message> {
    let (display, color) = if path.is_empty() {
        (
            "-- not set --".to_string(),
            iced::Color::from_rgb(0.6, 0.6, 0.6),
        )
    } else {
        (path.to_string(), iced::Color::from_rgb(0.4, 0.9, 0.4))
    };

    row![
        text(label).size(13).width(130),
        text(display).size(13).color(color).width(Fill),
    ]
    .spacing(10)
    .align_y(Center)
    .into()
}

// ============================================================================
// Filename derivation helpers
// ============================================================================

/// Add a suffix before the file extension.
///
/// Example: `data 1000 cardplay.csv` + "anon" -> `data 1000 cardplay anon.csv`
fn add_suffix_to_filename(input: &str, suffix: &str) -> String {
    let path = Path::new(input);
    let dir = path.parent().unwrap_or_else(|| Path::new(""));
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("output");

    dir.join(format!("{} {}.csv", stem, suffix))
        .display()
        .to_string()
}

/// Derive analyze DD output: replace "cardplay" with "DD", or append " DD".
///
/// Example: `data 1000 cardplay anon.csv` -> `data 1000 DD anon.csv`
/// Example: `data.csv` -> `data DD.csv`
fn derive_analyze_output(input: &str) -> String {
    let path = Path::new(input);
    let dir = path.parent().unwrap_or_else(|| Path::new(""));
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("output");

    let new_stem = if stem.contains("cardplay") {
        stem.replace("cardplay", "DD")
    } else {
        format!("{} DD", stem)
    };

    dir.join(format!("{}.csv", new_stem)).display().to_string()
}

// ============================================================================
// Config persistence
// ============================================================================

/// Get the config file path: ~/.edgar-toolkit.conf
fn config_path() -> Option<PathBuf> {
    std::env::var("HOME")
        .ok()
        .map(|home| PathBuf::from(home).join(".edgar-toolkit.conf"))
}

/// Persisted settings loaded from config file.
struct AppConfig {
    deal_limit_enabled: bool,
    deal_limit_count: String,
    case_folder: String,
}

/// Load settings from config file.
fn load_config() -> AppConfig {
    let defaults = AppConfig {
        deal_limit_enabled: true,
        deal_limit_count: "1000".to_string(),
        case_folder: String::new(),
    };
    let path = match config_path() {
        Some(p) => p,
        None => return defaults,
    };

    let content = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(_) => return defaults,
    };

    let mut cfg = defaults;

    for line in content.lines() {
        if let Some((key, value)) = line.split_once('=') {
            match key.trim() {
                "deal_limit_enabled" => cfg.deal_limit_enabled = value.trim() == "true",
                "deal_limit_count" => cfg.deal_limit_count = value.trim().to_string(),
                "case_folder" => cfg.case_folder = value.trim().to_string(),
                _ => {}
            }
        }
    }

    cfg
}

/// Save settings to config file.
fn save_config(enabled: bool, count: &str, case_folder: &str) {
    if let Some(path) = config_path() {
        let content = format!(
            "deal_limit_enabled={}\ndeal_limit_count={}\ncase_folder={}\n",
            enabled, count, case_folder
        );
        let _ = std::fs::write(&path, content);
    }
}

// ============================================================================
// Subprocess runner
// ============================================================================

/// Run anonymize directly via the library and stream progress updates to the UI.
///
/// Returns a stream of `Message` values: `ProgressUpdate` during execution,
/// and a final `TaskFinished` when complete or cancelled.
fn anonymize_stream(
    config: pipeline::AnonymizeAllConfig,
    cancel: Arc<AtomicBool>,
) -> impl futures::Stream<Item = Message> {
    let (tx, rx) = futures::channel::mpsc::unbounded();

    std::thread::spawn(move || {
        let result = pipeline::anonymize_all(&config, |p| {
            let _ = tx.unbounded_send(Message::ProgressUpdate {
                completed: p.completed,
                total: p.total,
                errors: 0,
                skipped: 0,
                phase: p.phase.to_string(),
            });
            !cancel.load(Ordering::Relaxed)
        });

        let _ = tx.unbounded_send(Message::TaskFinished(result.map_err(|e| e.to_string())));
    });

    rx
}

/// Run DD analysis directly via the library and stream progress updates to the UI.
///
/// Returns a stream of `Message` values: `ProgressUpdate` during execution,
/// and a final `TaskFinished` when complete or cancelled.
fn analyze_dd_stream(
    config: pipeline::AnalyzeDdConfig,
    cancel: Arc<AtomicBool>,
) -> impl futures::Stream<Item = Message> {
    let (tx, rx) = futures::channel::mpsc::unbounded();

    std::thread::spawn(move || {
        let result = pipeline::analyze_dd(&config, |p| {
            let _ = tx.unbounded_send(Message::ProgressUpdate {
                completed: p.completed,
                total: p.total,
                errors: p.errors,
                skipped: p.skipped,
                phase: String::new(),
            });
            !cancel.load(Ordering::Relaxed)
        });

        let _ = tx.unbounded_send(Message::TaskFinished(result.map_err(|e| e.to_string())));
    });

    rx
}

/// Run fetch-cardplay directly via the library and stream progress updates to the UI.
///
/// Returns a stream of `Message` values: `ProgressUpdate` during execution,
/// and a final `TaskFinished` when complete or cancelled.
fn fetch_cardplay_stream(
    mut config: pipeline::FetchCardplayConfig,
    deal_limit: Option<usize>,
    cancel: Arc<AtomicBool>,
) -> impl futures::Stream<Item = Message> {
    let (tx, rx) = futures::channel::mpsc::unbounded();

    std::thread::spawn(move || {
        // Truncate CSV if deal-limited
        if let Some(n) = deal_limit {
            match pipeline::truncate_csv(&config.input, n) {
                Ok(p) => config.input = p,
                Err(e) => {
                    let _ = tx.unbounded_send(Message::TaskFinished(Err(format!(
                        "Failed to truncate CSV: {}",
                        e
                    ))));
                    return;
                }
            }
        }

        let result = pipeline::fetch_cardplay(&config, |p| {
            let _ = tx.unbounded_send(Message::ProgressUpdate {
                completed: p.completed,
                total: p.total,
                errors: p.errors,
                skipped: p.skipped,
                phase: String::new(),
            });
            !cancel.load(Ordering::Relaxed)
        });

        let _ = tx.unbounded_send(Message::TaskFinished(result.map_err(|e| e.to_string())));
    });

    rx
}
