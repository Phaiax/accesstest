use crossbeam_channel::{unbounded, Receiver, Sender};
use failure::Error;
use quicli::prelude::*;
use sha1::{Digest, Sha1};
use structopt::StructOpt;
use walkdir::WalkDir;
//use rayon::prelude::*;

use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Try to find out what is wrong with dads laptop
#[derive(Debug, StructOpt)]
struct Cli {
    /// How many lines to get (0=unlimited)
    #[structopt(long = "count", short = "n", default_value = "0")]
    count: usize,

    #[structopt(long = "follow-links", short = "f")]
    follow_links: bool,

    #[structopt(short = "h")]
    hash: bool,

    #[structopt(long = "progress_every", short = "p", default_value = "1000")]
    progress_every: usize,

    #[structopt(long = "load", short = "l")]
    load_from: Option<String>,

    /// The path to crawl
    path: String,

    #[structopt(flatten)]
    verbosity: Verbosity,
}

#[derive(Debug, Eq, PartialEq)]
struct FileInfo {
    path: PathBuf,
    hash: Option<String>,
    modified: Option<SystemTime>,
    size: u64,
}

struct Progress {
    file: FileInfo,
    previously_known: bool,
    bytes_per_second: Option<f32>,
}

type Db = HashMap<PathBuf, FileInfo>;

type FileList = Vec<FileInfo>;

fn main() -> CliResult {
    let args = Arc::new(Cli::from_args());

    let db = if let Some(ref prev) = args.load_from {
        load_from(&prev)?
    } else {
        Default::default()
    };

    let (s, r) = unbounded();
    let args2 = Arc::clone(&args);
    let collector = thread::spawn(move || collector_thread(&args2, r));

    let filelist = find_files(&args)?;
    make_hashes(&args, filelist, &db, s)?;

    collector.join().unwrap();
    Ok(())
}

fn collector_thread(args: &Cli, progress_channel_r: Receiver<Progress>) {
    let mut total_bytes = 0;
    let mut total_hashed_bytes = 0;
    let mut total_files = 0;

    let starttime = Instant::now();
    //let progress_every = Duration::from_millis(1000);
    //if now.duration_since(last_progress) > progress_every {
    //    last_progress = now;
    use io::Write;
    let stderr = std::io::stderr();

    for progress in progress_channel_r.iter() {
        total_bytes += progress.file.size;
        if !progress.previously_known {
            total_hashed_bytes += progress.file.size;
        }
        total_files += 1;

        println!("{}", progress.file);

        if total_files % args.progress_every == 0 {
            let timediff = Instant::now().duration_since(starttime);
            let bytes_per_second =
                (total_hashed_bytes as f32) / (timediff.as_millis() as f32) * 1000.;
            write!(
                stderr.lock(),
                "\r{:4} MB, {:5} files, {:.1} MB/s | {:.1} MB/s {:<9}",
                total_bytes / 1024 / 1024,
                total_files,
                bytes_per_second / 1024. / 1024.,
                // current file
                progress.bytes_per_second.unwrap_or(0.) / 1024. / 1024.,
                progress
                    .file
                    .path
                    .file_name()
                    .map(|s| s.to_string_lossy())
                    .unwrap_or("".into()),
            )
            .unwrap();
        }
    }

    write!(
        stderr.lock(),
        "\r\nTotal size = {} MB",
        total_bytes / 1024 / 1024
    )
    .unwrap();
}

fn load_from(file: &str) -> Result<Db, Error> {
    let mut hm = HashMap::new();

    let prev = fs::File::open(file)?;
    let prev = io::BufReader::new(prev);
    use io::BufRead;
    for line in prev.lines() {
        let line = line?;
        if let Ok(path_hash_size) = FileInfo::try_from(&line[..]) {
            hm.insert(path_hash_size.path.clone(), path_hash_size);
        }
    }

    Ok(hm)
}

fn find_files(args: &Cli) -> Result<FileList, Error> {
    let w = WalkDir::new(&args.path).follow_links(args.follow_links);

    let witer = w.into_iter().take(if args.count > 0 {
        args.count
    } else {
        std::usize::MAX
    });

    let mut filelist = vec![];
    for entry in witer {
        let entry = entry?;

        let meta = entry.metadata()?;
        if meta.is_file() {
            filelist.push(FileInfo {
                path: entry.path().into(),
                size: meta.len(),
                hash: None,
                modified: Some(meta.modified().unwrap()),
            });
        }
    }

    Ok(filelist)
}

fn make_hashes(
    args: &Cli,
    filelist: FileList,
    db: &Db,
    progress_channel: Sender<Progress>,
) -> CliResult {
    filelist.par_iter().try_for_each(|fileinfo| {
        let mut previously_known = false;

        let hash_bps = if args.hash {
            match db.get(&fileinfo.path) {
                Some(FileInfo {
                    hash: Some(dbhash),
                    size: dbsize,
                    ..
                }) => {
                    if fileinfo.size == *dbsize {
                        previously_known = true;
                        Some((dbhash.clone(), 0.))
                    } else {
                        hash_file(&fileinfo.path, fileinfo.size)
                    }
                }
                _ => {
                    use io::Write;
                    write!(std::io::stderr().lock(), "\n{:?}\n{}\n", fileinfo.path, fileinfo).unwrap();
                    hash_file(&fileinfo.path, fileinfo.size)
                }
            }
        } else {
            None
        };

        let bps = hash_bps.as_ref().map(|s| s.1);

        progress_channel
            .send(Progress {
                file: FileInfo {
                    path: fileinfo.path.clone(),
                    hash: hash_bps.map(|s| s.0),
                    size: fileinfo.size,
                    modified: fileinfo.modified,
                },
                previously_known,
                bytes_per_second: bps,
            })
            .unwrap();

        Some(())
    });

    Ok(())
}

fn hash_file(path: &Path, filesize: u64) -> Option<(String, f32)> {
    let starttime = Instant::now();

    let mut file = match fs::File::open(path) {
        Ok(file) => file,
        Err(_e) => {
            print!("Failed opening {:?}", path);
            return None;
        }
    };
    let mut hasher = Sha1::new();
    let _n = match io::copy(&mut file, &mut hasher) {
        Ok(n) => n,
        Err(_e) => {
            print!("Failed copying {:?}", path);
            return None;
        }
    };
    let hash = hasher.result();
    let hash = format!("{:x}", hash);

    let timediff = Instant::now().duration_since(starttime);
    let bytes_per_second = (filesize as f32) / (timediff.as_millis() as f32) * 1000.;

    Some((hash, bytes_per_second))
}

impl fmt::Display for FileInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "> {}", self.size)?;
        match self.modified {
            Some(modified) => {
                let secs = modified
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_e| fmt::Error)?
                    .as_secs();
                write!(f, " | {}", secs)?
            }
            None => write!(f, " | None")?,
        }
        match &self.hash {
            Some(hash) => write!(f, " | {}", hash)?,
            None => write!(f, " | None")?,
        }
        use std::os::windows::ffi::OsStrExt;
        let pathutf16 = self.path.as_os_str().encode_wide();
        write!(f, " | ")?;
        for c in pathutf16 {
            if c == 0x25 {
                // %
                write!(f, "%0025")?;
            } else if c >= 0x20 && c <= 127 {
                let c = c as u8 as char;
                write!(f, "{}", c)?;
            } else {
                write!(f, "%{:04x}", c)?;
            }
        }
        Ok(())
    }
}

impl TryFrom<&str> for FileInfo {
    type Error = ();
    fn try_from(line: &str) -> Result<FileInfo, ()> {
        if line.starts_with('>') {
            let mut abcd = line[1..].splitn(4, " | ");
            let size: u64 = abcd.next().ok_or(())?.trim().parse().map_err(|_| ())?;
            let modified = match abcd.next().ok_or(())? {
                "None" => None,
                modified => {
                    let modified: u64 = modified.trim().parse().map_err(|_| ())?;
                    Some(
                        UNIX_EPOCH
                            .checked_add(Duration::from_secs(modified))
                            .ok_or(())?,
                    )
                }
            };
            let hash = match abcd.next().ok_or(())? {
                "None" => None,
                e => Some(e.to_owned()),
            };
            let path = abcd.next().ok_or(())?;
            let mut buf = vec![];
            let mut pathchars = path.chars();
            loop {
                match pathchars.next() {
                    Some(c) => {
                        if c == '%' {
                            let n1 = pathchars.next().ok_or(())? as u8 - b'0';
                            let n2 = pathchars.next().ok_or(())? as u8 - b'0';
                            let n3 = pathchars.next().ok_or(())? as u8 - b'0';
                            let n4 = pathchars.next().ok_or(())? as u8 - b'0';
                            let n = ((n1 as u16) << 12) + ((n2 as u16) << 8) + ((n3 as u16) << 4) + (n4 as u16);
                            buf.push(n);
                        } else {
                            buf.push(c as u16);
                        }
                    }
                    None => break,
                }
            }

            use std::os::windows::ffi::OsStringExt;
            let path = std::ffi::OsString::from_wide(&buf);
            let path = PathBuf::from(path);
            return Ok(FileInfo {
                path,
                size,
                modified,
                hash,
            });
        }
        let mut abc = line.split(" bytes: ");
        let a = abc.next().ok_or(())?;
        let a: u64 = a.trim().parse().map_err(|_| ())?;
        let bc = abc.next().ok_or(())?;
        let mut bc = bc.splitn(2, " ");
        let b_or_c = bc.next().ok_or(())?;
        match bc.next() {
            Some(c) => Ok(FileInfo {
                size: a,
                path: c.to_owned().into(),
                hash: Some(b_or_c.to_owned()),
                modified: None,
                // (c.to_owned(), (Some(b_or_c.to_owned()), a))
            }),
            None => Ok(FileInfo {
                path: b_or_c.to_owned().into(),
                hash: None,
                size: a,
                modified: None,
                //(b_or_c.to_owned(), (None, a))),
            }),
        }
    }
}

#[test]
fn test_split_line() {
    assert_eq!(FileInfo::try_from("    556602 bytes: 5172bde22e6ca41d60b4682cafa928add3e94bf6 ..\\..\\10.1007_1-4020-7830-7.pdf"),
               Ok(FileInfo{
                    path: "..\\..\\10.1007_1-4020-7830-7.pdf".to_owned().into(),
                    hash: Some("5172bde22e6ca41d60b4682cafa928add3e94bf6".to_owned()),
                    size: 556602,
                    modified: None, }));

    assert_eq!(
        FileInfo::try_from("    556602 bytes: ..\\..\\10.1007_1-4020-7830-7.pdf"),
        Ok(FileInfo {
            path: "..\\..\\10.1007_1-4020-7830-7.pdf".to_owned().into(),
            hash: None,
            size: 556602,
            modified: None,
        })
    );

    assert_eq!(FileInfo::try_from("    556602 bytes: 5172bde22e6ca41d60b4682cafa928add3e94bf6 ..\\..\\10.1007_1- 4020-7830-7.pdf"),
               Ok(FileInfo{
                path:"..\\..\\10.1007_1- 4020-7830-7.pdf".to_owned().into(), 
                hash: Some("5172bde22e6ca41d60b4682cafa928add3e94bf6".to_owned()), size: 556602, modified: None } 
                    ));

    use std::ops::Add;
    let mut f = FileInfo {
        path : "C:\\%\\123".into(),
        hash : Some ("abcde".to_owned()),
        modified : Some(UNIX_EPOCH.add(Duration::from_secs(10000))),
        size: 10000
    };

    assert_eq!(f, FileInfo::try_from(&format!("{}", f)[..]).unwrap());

    f.hash = None;
    f.modified = None;
    use std::os::windows::ffi::OsStringExt;
    f.path = PathBuf::from(std::ffi::OsString::from_wide(&[0x1234, 0x0001, 0x0000, 0x9999, 0x0034]));
    assert_eq!(f, FileInfo::try_from(&format!("{}", f)[..]).unwrap());
}

#[test]
fn read_db() {
    let db = load_from("accessed_files2").unwrap();
    println!("Read entries: {}", db.len());
}

