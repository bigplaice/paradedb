// Copyright (c) 2023-2025 ParadeDB, Inc.
//
// This file is part of ParadeDB - Postgres for Search and Analytics
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
#![recursion_limit = "512"]

mod aggregate;
mod api;
mod bootstrap;
mod index;
mod postgres;
mod query;
mod schema;

pub mod gucs;
pub mod parallel_worker;

use self::postgres::customscan;
use pgrx::*;

/// Postgres' value for a `norm_selec` that hasn't been assigned
const UNASSIGNED_SELECTIVITY: f64 = -1.0;

/// A hardcoded value when we can't figure out a good selectivity value
const UNKNOWN_SELECTIVITY: f64 = 0.00001;

/// A hardcoded value for parameterized plan queries
const PARAMETERIZED_SELECTIVITY: f64 = 0.10;

/// The selectivity value indicating the entire relation will be returned
const FULL_RELATION_SELECTIVITY: f64 = 1.0;

/// An arbitrary value for what it costs for a plan with one of our operators (@@@) to do whatever
/// initial work it needs to do (open tantivy index, start the query, etc).  The value is largely
/// meaningless but we should be honest that do _something_.
const DEFAULT_STARTUP_COST: f64 = 10.0;

pgrx::pg_module_magic!();

extension_sql!(
    r#"
        GRANT ALL ON SCHEMA paradedb TO PUBLIC;
        GRANT ALL ON SCHEMA pdb TO PUBLIC;
    "#,
    name = "paradedb_grant_all",
    finalize
);

pub fn available_parallelism() -> usize {
    use once_cell::sync::Lazy;

    static AVAILABLE_PARALLELISM: Lazy<usize> = Lazy::new(|| {
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1)
    });
    *AVAILABLE_PARALLELISM
}

/// Initializes option parsing
#[allow(clippy::missing_safety_doc)]
#[allow(non_snake_case)]
#[pg_guard]
pub unsafe extern "C-unwind" fn _PG_init() {
    // initialize environment logging (to stderr) for dependencies that do logging
    // we can't implement our own logger that sends messages to Postgres `ereport()` because
    // of threading concerns
    std::env::set_var("RUST_LOG", "warn");
    std::env::set_var("RUST_LOG_STYLE", "never");
    env_logger::init();

    if cfg!(not(any(feature = "pg17", feature = "pg18")))
        && !pg_sys::process_shared_preload_libraries_in_progress
    {
        error!("pg_search must be loaded via shared_preload_libraries. Add 'pg_search' to shared_preload_libraries in postgresql.conf and restart Postgres.");
    }

    postgres::options::init();
    gucs::init();

    #[cfg(not(any(feature = "pg17", feature = "pg18")))]
    postgres::fake_aminsertcleanup::register();

    #[allow(static_mut_refs)]
    #[allow(deprecated)]
    customscan::register_rel_pathlist(customscan::pdbscan::PdbScan);
    customscan::register_upper_path(customscan::aggregatescan::AggregateScan);

    // Register global planner hook for window function support
    customscan::register_window_aggregate_hook();

    // Initialize the filter query builder
    customscan::aggregatescan::filterquery::init_filter_query_builder();
}

#[pg_extern]
fn random_words(num_words: i32) -> String {
    use rand::Rng;

    let mut rng = rand::rng();
    let letters = "abcdefghijklmnopqrstuvwxyz";
    let mut result = String::new();

    for _ in 0..num_words {
        // Choose a random word length between 3 and 7.
        let word_length = rng.random_range(3..=7);
        let mut word = String::new();

        for _ in 0..word_length {
            // Pick a random letter from the letters string.
            let random_index = rng.random_range(0..letters.len());
            // Safe to use .unwrap() because the index is guaranteed to be valid.
            let letter = letters.chars().nth(random_index).unwrap();
            word.push(letter);
        }
        result.push_str(&word);
        result.push(' ');
    }
    result.trim_end().to_string()
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests

        let mut options: Vec<&'static str> = Vec::new();

        if cfg!(not(any(feature = "pg17", feature = "pg18"))) {
            options.push("shared_preload_libraries='pg_search'");
        }

        options
    }
}

//nsj
#[pg_extern]
fn update_or_add_user_dict(word: &str, freq: Option<i32>, tag: Option<&str>) -> String {
use std::env;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{PathBuf};

    // 1. 获取词典路径
    let exe_path = match env::current_exe() {
        Ok(p) => p,
        Err(e) => return format!("错误: 获取可执行文件路径失败: {}", e),
    };
    let exe_dir = match exe_path.parent() {
        Some(d) => d,
        None => return "错误: 无法获取可执行文件所在目录".to_string(),
    };
    let dict_path: PathBuf = match exe_dir.join("../share/postgresql/user_dict.txt").canonicalize() {
        Ok(p) => p,
        Err(e) => return format!("错误: 构造词典路径失败: {}", e),
    };

    // 默认值（用于新增词条）
    const DEFAULT_FREQ: i32 = 1000;

    let mut lines: Vec<String> = Vec::new();
    let mut found = false;
    let mut need_write = false;

    // 2. 如果文件存在，读取并检查
    if dict_path.exists() {
        let file = match fs::File::open(&dict_path) {
            Ok(f) => f,
            Err(e) => return format!("错误: 无法打开词典文件: {}", e),
        };
        let reader = BufReader::new(file);

        for line_result in reader.lines() {
            let line = match line_result {
                Ok(l) => l,
                Err(e) => return format!("错误: 读取词典文件行失败: {}", e),
            };
            let trimmed = line.trim();

            if trimmed.is_empty() || trimmed.starts_with('#') {
                lines.push(line);
                continue;
            }

            let parts: Vec<&str> = trimmed.split_whitespace().collect();
            if parts.is_empty() {
                lines.push(line);
                continue;
            }

            let existing_word = parts[0];

            if existing_word == word {
                found = true;

                // 原有值
                let old_freq = parts.get(1).and_then(|s| s.parse::<i32>().ok());
                let old_tag = parts.get(2).copied().unwrap_or("");

                // 新值（None 表示不修改）
                let new_freq = freq.or(old_freq);
                let new_tag = tag.unwrap_or(old_tag);

                // 判断是否真的需要修改：只有用户没有明确指定该字段且值未变才算无变化
                let freq_unchanged = freq.is_none() && new_freq == old_freq;
                let tag_unchanged = tag.is_none() && new_tag == old_tag;

                if freq_unchanged && tag_unchanged {
                    lines.push(line);
                    return format!("词条 '{}' 已存在且无需修改，无操作执行", word);
                }

                // 有修改意图
                need_write = true;

                // 构造新行
                let new_line = if let Some(f) = new_freq {
                    if new_tag.is_empty() {
                        format!("{} {}", word, f)
                    } else {
                        format!("{} {} {}", word, f, new_tag)
                    }
                } else if !new_tag.is_empty() {
                    format!("{} {}", word, new_tag)
                } else {
                    word.to_string()
                };

                lines.push(new_line);
            } else {
                lines.push(line);
            }
        }
    }

    // 3. 如果没找到 → 添加新词条
    if !found {
        need_write = true;

        let new_freq = freq.unwrap_or(DEFAULT_FREQ);
        let new_tag = tag.unwrap_or("");

        let new_line = if new_tag.is_empty() {
            format!("{} {}", word, new_freq)
        } else {
            format!("{} {} {}", word, new_freq, new_tag)
        };

        lines.push(new_line);
    }

    // 4. 只有需要时才写文件
    if need_write {
        // 确保父目录存在
        if let Some(parent) = dict_path.parent() {
            if let Err(e) = fs::create_dir_all(parent) {
                return format!("错误: 创建目录失败: {}", e);
            }
        }

        let mut file = match fs::File::create(&dict_path) {
            Ok(f) => f,
            Err(e) => return format!("错误: 创建/写入词典文件失败: {}", e),
        };

        for line in lines {
            if let Err(e) = writeln!(file, "{}", line) {
                return format!("错误: 写入词典文件失败: {}", e);
            }
        }

        if found {
            format!("词条 '{}' 已更新", word)
        } else {
            format!("词条 '{}' 已添加", word)
        }
    } else {
        // 理论上不会走到这里，但保险
        format!("词条 '{}' 已存在且无需修改，无操作执行", word)
    }
}

/// TODO: 批量更新或添加用户自定义词典中的词条
///
/// 参数：word_array: &[&str]，每个元素格式为：
///   "关键词"                → 添加/更新词条，使用默认词频 1000，无词性
///   "关键词,词频"           → 指定词频，无词性
///   "关键词,词频,词性"      → 指定词频和词性
///
/// 返回：字符串，描述所有操作的结果（多行）
//fn update_or_add_user_dict(words: VariadicArray<Option<&str>>) -> String {

#[pg_extern]
fn delete_from_user_dict(words: VariadicArray<&str>) -> String {
use std::env;
use std::fs;
use std::path::PathBuf;
use std::collections::HashSet;

    let words_vec: Vec<&str> = words
        .iter()
        .flatten()  // 跳过 NULL 值（如果有）
        .collect();

    if words_vec.is_empty() {
        return "未提供要删除的词条".to_string();
    }

    // 使用 HashSet 快速检查
    let delete_set: HashSet<&str> = words_vec.iter().copied().collect();

    // 获取当前可执行文件路径
    let exe_path = match env::current_exe() {
        Ok(p) => p,
        Err(e) => return format!("无法获取可执行文件路径: {}", e),
    };

    let exe_dir = match exe_path.parent() {
        Some(d) => d,
        None => return "无法获取可执行文件目录".to_string(),
    };

    // 构造路径: <exe_dir>/../share/postgresql/user_dict.txt
    let mut file_path = PathBuf::from(exe_dir);
    file_path.push("..");
    file_path.push("share");
    file_path.push("postgresql");
    file_path.push("user_dict.txt");

    // 规范化路径
    let file_path = match file_path.canonicalize() {
        Ok(p) => p,
        Err(_) => return "无法规范化文件路径".to_string(),
    };

    // 文件不存在
    if !file_path.exists() {
        return format!("文件不存在: {}", file_path.display());
    }

    // 读取文件内容
    let content = match fs::read_to_string(&file_path) {
        Ok(c) => c,
        Err(e) => return format!("读取文件失败: {}", e),
    };

    // 收集要保留的行
    let mut new_lines: Vec<String> = Vec::new();
    let mut deleted_count = 0;

    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            // 保留空行和注释
            new_lines.push(line.to_string());
            continue;
        }

        if let Some(entry) = trimmed.split_whitespace().next() {
            if delete_set.contains(entry) {
                // 匹配，删除该行（不push）
                deleted_count += 1;
                continue;
            }
        }

        // 不匹配，保留
        new_lines.push(line.to_string());
    }

    if deleted_count == 0 {
        return "未找到匹配的词条，未进行删除".to_string();
    }

    // 写回文件
    let new_content = new_lines.join("\n") + "\n";

    match fs::write(&file_path, new_content) {
        Ok(_) => format!("成功删除 {} 个词条并保存文件: {}", deleted_count, file_path.display()),
        Err(e) => format!("写入文件失败: {}", e),
    }
}
