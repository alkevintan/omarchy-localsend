use crate::rpc::{ApiError, ApiResult};
use localsend::model::transfer::{FileDto, FileMetadata};
use localsend::util::filename::{self, Rules};
use std::fs;
use std::io::ErrorKind;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;

const MAX_SELECTED_ROOTS: usize = 256;
const MAX_SELECTED_FILES: usize = 512;
const MAX_VISITED_ENTRIES: usize = 10_000;
const MAX_RECURSION_DEPTH: usize = 16;
pub const MAX_TEXT_BYTES: usize = 512 * 1024;

#[derive(Clone)]
pub enum OutgoingSource {
    Path {
        path: PathBuf,
        device: u64,
        inode: u64,
        size: u64,
    },
    Bytes(Arc<Vec<u8>>),
}

#[derive(Clone)]
pub struct OutgoingItem {
    pub file: FileDto,
    pub source: OutgoingSource,
    pub source_path: Option<String>,
}

pub fn collect_selection(paths: &[String]) -> ApiResult<Vec<OutgoingItem>> {
    if paths.is_empty() {
        return Err(ApiError::new(
            "invalid_path",
            "At least one path is required",
        ));
    }
    if paths.len() > MAX_SELECTED_ROOTS {
        return Err(ApiError::new(
            "selection_too_large",
            format!("At most {MAX_SELECTED_ROOTS} root paths may be selected"),
        ));
    }

    let mut items = Vec::new();
    let mut visited = 0;
    for raw in paths {
        let path = PathBuf::from(raw);
        if !path.is_absolute() {
            return Err(ApiError::new(
                "invalid_path",
                format!("Path must be absolute: {raw}"),
            ));
        }
        let metadata = fs::symlink_metadata(&path).map_err(|error| path_error(&path, error))?;
        if metadata.file_type().is_symlink() {
            return Err(ApiError::new(
                "invalid_path",
                format!("Symbolic links are not accepted: {}", path.display()),
            ));
        }
        if metadata.is_file() {
            let name = safe_local_component(path.file_name(), &path)?;
            push_file(&path, name, metadata, &mut items)?;
        } else if metadata.is_dir() {
            let root = safe_local_component(path.file_name(), &path)?;
            collect_directory(&path, &root, 0, &mut visited, &mut items)?;
        } else {
            return Err(ApiError::new(
                "invalid_path",
                format!(
                    "Path is not a regular file or directory: {}",
                    path.display()
                ),
            ));
        }
    }

    if items.is_empty() {
        return Err(ApiError::new(
            "empty_selection",
            "The selected directories contain no regular files",
        ));
    }
    Ok(items)
}

pub fn text_item(text: String) -> ApiResult<OutgoingItem> {
    if text.is_empty() {
        return Err(ApiError::new("empty_text", "Text must not be empty"));
    }
    if text.len() > MAX_TEXT_BYTES {
        return Err(ApiError::new(
            "text_too_large",
            format!("Text is limited to {MAX_TEXT_BYTES} UTF-8 bytes"),
        ));
    }

    let id = Uuid::new_v4().to_string();
    let name = format!("{id}.txt");
    let bytes = text.into_bytes();
    Ok(OutgoingItem {
        file: FileDto {
            id: id.clone(),
            file_name: name,
            size: bytes.len() as u64,
            file_type: "text/plain".to_string(),
            sha256: None,
            preview: Some(String::from_utf8_lossy(&bytes).into_owned()),
            metadata: None,
        },
        source: OutgoingSource::Bytes(Arc::new(bytes)),
        source_path: None,
    })
}

fn collect_directory(
    directory: &Path,
    relative: &str,
    depth: usize,
    visited: &mut usize,
    items: &mut Vec<OutgoingItem>,
) -> ApiResult<()> {
    if depth >= MAX_RECURSION_DEPTH {
        return Err(ApiError::new(
            "selection_too_deep",
            format!("Directory nesting exceeds {MAX_RECURSION_DEPTH} levels"),
        ));
    }

    let mut entries = Vec::new();
    for entry in fs::read_dir(directory).map_err(|error| path_error(directory, error))? {
        *visited += 1;
        if *visited > MAX_VISITED_ENTRIES {
            return Err(ApiError::new(
                "selection_too_large",
                format!("Selection may contain at most {MAX_VISITED_ENTRIES} directory entries"),
            ));
        }
        entries.push(entry.map_err(|error| path_error(directory, error))?);
    }
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).map_err(|error| path_error(&path, error))?;
        if metadata.file_type().is_symlink() {
            return Err(ApiError::new(
                "invalid_path",
                format!("Symbolic links are not accepted: {}", path.display()),
            ));
        }
        let component = safe_local_component(Some(&entry.file_name()), &path)?;
        let child_relative = format!("{relative}/{component}");
        if metadata.is_dir() {
            collect_directory(&path, &child_relative, depth + 1, visited, items)?;
        } else if metadata.is_file() {
            push_file(&path, child_relative, metadata, items)?;
        } else {
            return Err(ApiError::new(
                "invalid_path",
                format!("Special files are not accepted: {}", path.display()),
            ));
        }
    }
    Ok(())
}

fn push_file(
    path: &Path,
    relative_name: String,
    metadata: fs::Metadata,
    items: &mut Vec<OutgoingItem>,
) -> ApiResult<()> {
    if items.len() >= MAX_SELECTED_FILES {
        return Err(ApiError::new(
            "selection_too_large",
            format!("At most {MAX_SELECTED_FILES} files may be sent at once"),
        ));
    }
    let source_path = path.to_str().ok_or_else(|| {
        ApiError::new(
            "invalid_path",
            format!("Path is not valid UTF-8: {}", path.display()),
        )
    })?;
    let id = Uuid::new_v4().to_string();
    items.push(OutgoingItem {
        file: FileDto {
            id: id.clone(),
            file_name: relative_name,
            size: metadata.len(),
            file_type: mime_guess::from_path(path)
                .first_or_octet_stream()
                .to_string(),
            sha256: None,
            preview: None,
            metadata: FileMetadata::from_fs_metadata(&metadata),
        },
        source: OutgoingSource::Path {
            path: path.to_path_buf(),
            device: metadata.dev(),
            inode: metadata.ino(),
            size: metadata.len(),
        },
        source_path: Some(source_path.to_string()),
    });
    Ok(())
}

fn safe_local_component(name: Option<&std::ffi::OsStr>, path: &Path) -> ApiResult<String> {
    let name = name.and_then(std::ffi::OsStr::to_str).ok_or_else(|| {
        ApiError::new("invalid_path", format!("Invalid path: {}", path.display()))
    })?;
    Ok(filename::sanitize(name, Rules::Universal))
}

fn path_error(path: &Path, error: std::io::Error) -> ApiError {
    ApiError::new(
        "invalid_path",
        format!("Could not inspect {}: {error}", path.display()),
    )
}

pub fn sanitize_relative_name(name: &str) -> ApiResult<Vec<String>> {
    if name.len() > 4096 {
        return Err(ApiError::new("unsafe_file_name", "File name is too long"));
    }
    if name.starts_with('/')
        || name.starts_with('\\')
        || name
            .as_bytes()
            .get(1)
            .is_some_and(|byte| *byte == b':' && name.as_bytes()[0].is_ascii_alphabetic())
    {
        return Err(ApiError::new(
            "unsafe_file_name",
            "Absolute file names are not accepted",
        ));
    }

    let mut components = Vec::new();
    for component in name.split(['/', '\\']) {
        match component {
            "" | "." => continue,
            ".." => {
                return Err(ApiError::new(
                    "unsafe_file_name",
                    "Parent path components are not accepted",
                ));
            }
            component => components.push(filename::sanitize(component, Rules::current())),
        }
        if components.len() > MAX_RECURSION_DEPTH {
            return Err(ApiError::new(
                "unsafe_file_name",
                "File path contains too many components",
            ));
        }
    }
    if components.is_empty() {
        components.push(filename::sanitize("", Rules::current()));
    }
    Ok(components)
}

pub fn reserve_unique_destination(destination: &Path, name: &str) -> ApiResult<PathBuf> {
    let components = sanitize_relative_name(name)?;
    let mut directory = destination.to_path_buf();
    for component in components.iter().take(components.len() - 1) {
        directory.push(component);
        ensure_directory_component(destination, &directory)?;
    }

    let base = components
        .last()
        .expect("sanitizer always returns a component");
    for index in 0..10_000u32 {
        let candidate_name = if index == 0 {
            base.clone()
        } else {
            counted_name(base, index)
        };
        let candidate = directory.join(candidate_name);
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&candidate)
        {
            Ok(_) => return Ok(candidate),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(ApiError::new(
                    "destination_error",
                    format!("Could not reserve {}: {error}", candidate.display()),
                ));
            }
        }
    }
    Err(ApiError::new(
        "destination_error",
        format!("Could not find a unique name for {base}"),
    ))
}

fn ensure_directory_component(root: &Path, directory: &Path) -> ApiResult<()> {
    match fs::symlink_metadata(directory) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(ApiError::new(
                "unsafe_destination",
                format!(
                    "Destination contains a symbolic link: {}",
                    directory.display()
                ),
            ));
        }
        Ok(metadata) if !metadata.is_dir() => {
            return Err(ApiError::new(
                "destination_error",
                format!(
                    "Destination component is not a directory: {}",
                    directory.display()
                ),
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {
            fs::create_dir(directory).map_err(|error| {
                ApiError::new(
                    "destination_error",
                    format!("Could not create {}: {error}", directory.display()),
                )
            })?;
        }
        Err(error) => {
            return Err(ApiError::new(
                "destination_error",
                format!("Could not inspect {}: {error}", directory.display()),
            ));
        }
    }

    let canonical = fs::canonicalize(directory).map_err(|error| {
        ApiError::new(
            "destination_error",
            format!("Could not resolve {}: {error}", directory.display()),
        )
    })?;
    if !canonical.starts_with(root) {
        return Err(ApiError::new(
            "unsafe_destination",
            "Destination path escapes the download directory",
        ));
    }
    Ok(())
}

fn counted_name(name: &str, index: u32) -> String {
    match name.rsplit_once('.') {
        Some((stem, extension)) if !stem.is_empty() => {
            format!("{stem} ({index}).{extension}")
        }
        _ => format!("{name} ({index})"),
    }
}

pub fn remove_reservations(paths: impl IntoIterator<Item = PathBuf>) {
    for path in paths {
        let _ = fs::remove_file(path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn sanitizes_relative_names_and_rejects_escape_attempts() {
        assert_eq!(
            sanitize_relative_name("outer/inner/file.txt").unwrap(),
            ["outer", "inner", "file.txt"]
        );
        assert_eq!(sanitize_relative_name("a/./b.txt").unwrap(), ["a", "b.txt"]);
        assert!(sanitize_relative_name("../outside.txt").is_err());
        assert!(sanitize_relative_name("a\\..\\outside.txt").is_err());
        assert!(sanitize_relative_name("/etc/passwd").is_err());
        assert!(sanitize_relative_name("C:\\Windows\\file.txt").is_err());
    }

    #[test]
    fn reserves_unique_destinations_atomically() {
        let temp = tempfile::tempdir().unwrap();
        let destination = fs::canonicalize(temp.path()).unwrap();
        fs::write(destination.join("report.txt"), b"existing").unwrap();

        let first = reserve_unique_destination(&destination, "report.txt").unwrap();
        let second = reserve_unique_destination(&destination, "report.txt").unwrap();
        assert_eq!(first.file_name().unwrap(), "report (1).txt");
        assert_eq!(second.file_name().unwrap(), "report (2).txt");
        assert!(first.exists());
        assert!(second.exists());
    }

    #[test]
    fn recursive_selection_preserves_safe_folder_names() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("picked");
        fs::create_dir_all(root.join("sub")).unwrap();
        fs::File::create(root.join("a.txt"))
            .unwrap()
            .write_all(b"a")
            .unwrap();
        fs::File::create(root.join("sub/b.txt"))
            .unwrap()
            .write_all(b"bb")
            .unwrap();

        let items = collect_selection(&[root.to_string_lossy().into_owned()]).unwrap();
        let names: Vec<&str> = items
            .iter()
            .map(|item| item.file.file_name.as_str())
            .collect();
        assert_eq!(names, ["picked/a.txt", "picked/sub/b.txt"]);
        assert_eq!(items.iter().map(|item| item.file.size).sum::<u64>(), 3);
    }
}
