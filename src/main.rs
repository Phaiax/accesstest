use crossbeam_channel::{unbounded, Receiver, Sender};
use failure::Error;
use quicli::prelude::*;
use sha1::{Digest, Sha1};
use structopt::StructOpt;
use walkdir::WalkDir;
//use rayon::prelude::*;

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Instant; //, Duration};

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

struct FileInfo {
    path: PathBuf,
    hash: Option<String>,
    size: u64,
}

struct Progress {
    file : FileInfo,
    previously_known: bool,
    bytes_per_second : Option<f32>,
}

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

        match progress.file.hash {
            Some(hash) => println!(
                "{:12} bytes: {} {}",
                progress.file.size,
                hash,
                progress.file.path.display()
            ),
            None => println!("{:12} bytes: {}", progress.file.size, progress.file.path.display()),
        }

        if total_files % args.progress_every == 0 {
            let timediff = Instant::now().duration_since(starttime);
            let bytes_per_second = (total_hashed_bytes as f32) / (timediff.as_millis() as f32) * 1000.;
            write!(
                stderr.lock(),
                "\r{:4} MB, {:5} files, {:.1} MB/s | {:.1} MB/s {:<9}                ",
                total_bytes / 1024 / 1024,
                total_files,
                bytes_per_second / 1024. / 1024.,
                // current file
                progress.bytes_per_second.unwrap_or(0.) / 1024. / 1024.,
                progress.file.path.file_name().map(|s| s.to_string_lossy()).unwrap_or("".into()),
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

type Db = HashMap<PathBuf, (Option<String>, u64)>;

fn load_from(file: &str) -> Result<Db, Error> {
    let mut hm = HashMap::new();

    let prev = fs::File::open(file)?;
    let prev = io::BufReader::new(prev);
    use io::BufRead;
    for line in prev.lines() {
        let line = line?;
        if let Some(path_hash_size) = split_line(&line) {
            hm.insert(path_hash_size.0.into(), path_hash_size.1);
        }
    }

    Ok(hm)
}

type FileList = Vec<(PathBuf, u64)>;

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
            filelist.push((entry.path().into(), meta.len()));
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
    filelist.par_iter().try_for_each(|(path, filesize)| {
        let mut previously_known = false;

        let hash_bps = if args.hash {
            match db.get(path) {
                Some((Some(hash), size)) => {
                    if *size == *filesize {
                        previously_known = true;
                        Some((hash.clone(), 0.))
                    } else {
                        hash_file(path, *filesize)
                    }
                }
                _ => hash_file(path, *filesize),
            }
        } else {
            None
        };

        let bps = hash_bps.as_ref().map(|s| s.1);

        progress_channel
            .send(Progress {
                file : FileInfo {
                    path: path.clone(),
                    hash: hash_bps.map(|s| s.0),
                    size: *filesize
                },
                previously_known,
                bytes_per_second: bps,
            })
            .unwrap();

        Some(())
    });

    Ok(())
}

fn hash_file(path: &Path, filesize : u64) -> Option<(String, f32)> {
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

fn split_line(line: &str) -> Option<(String, (Option<String>, u64))> {
    let mut abc = line.split(" bytes: ");
    let a = abc.next()?;
    let a: u64 = a.trim().parse().ok()?;
    let bc = abc.next()?;
    let mut bc = bc.splitn(2, " ");
    let b_or_c = bc.next()?;
    match bc.next() {
        Some(c) => Some((c.to_owned(), (Some(b_or_c.to_owned()), a))),
        None => Some((b_or_c.to_owned(), (None, a))),
    }
}

#[test]
fn test_split_line() {
    assert_eq!(split_line("    556602 bytes: 5172bde22e6ca41d60b4682cafa928add3e94bf6 ..\\..\\10.1007_1-4020-7830-7.pdf"),
               Some(("..\\..\\10.1007_1-4020-7830-7.pdf".to_owned(), (Some("5172bde22e6ca41d60b4682cafa928add3e94bf6".to_owned()), 556602))));

    assert_eq!(
        split_line("    556602 bytes: ..\\..\\10.1007_1-4020-7830-7.pdf"),
        Some((
            "..\\..\\10.1007_1-4020-7830-7.pdf".to_owned(),
            (None, 556602)
        ))
    );

    assert_eq!(split_line("    556602 bytes: 5172bde22e6ca41d60b4682cafa928add3e94bf6 ..\\..\\10.1007_1- 4020-7830-7.pdf"),
               Some(("..\\..\\10.1007_1- 4020-7830-7.pdf".to_owned(), (Some("5172bde22e6ca41d60b4682cafa928add3e94bf6".to_owned()), 556602))));
}

#[test]
fn read_db() {
    let db = load_from("accessed_files2").unwrap();
    println!("Read entries: {}", db.len());
}
