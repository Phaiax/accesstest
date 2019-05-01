
use quicli::prelude::*;
use structopt::StructOpt;
use walkdir::WalkDir;
use sha1::{Sha1, Digest};
use failure::{Error};
use crossbeam_channel::{unbounded, Sender, Receiver};
//use rayon::prelude::*;

use std::time::Instant;//, Duration};
use std::collections::HashMap;
use std::io;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::sync::Arc;


/// Try to find out what is wrong with dads laptop
#[derive(Debug, StructOpt)]
struct Cli {
    /// How many lines to get (0=unlimited)
    #[structopt(long = "count", short = "n", default_value = "0")]
    count: usize,

    #[structopt(long = "follow-links", short = "f")]
    follow_links : bool,

    #[structopt(short = "h")]
    hash : bool,

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
    previously_known: bool,
}


fn main() -> CliResult {
    let args = Arc::new(Cli::from_args());
    let db = load_from(&args)?;

    let (s, r) = unbounded();
    let args2 = Arc::clone(&args);
    let collector = thread::spawn(move || { collector_thread(&args2, r) });

    let filelist = find_files(&args)?;
    make_hashes(&args, filelist, &db, s)?;

    collector.join().unwrap();
    Ok(())
}

fn collector_thread(args : &Cli, progress_channel_r: Receiver<FileInfo>) {
    let mut total_bytes = 0;
    let mut total_hashed_bytes = 0;
    let mut total_files = 0;

    let starttime = Instant::now();
    //let progress_every = Duration::from_millis(1000);
    //if now.duration_since(last_progress) > progress_every {
    //    last_progress = now;
    use io::Write;
    let stderr = std::io::stderr();

    for fileinfo in progress_channel_r.iter() {
        total_bytes += fileinfo.size;
        if ! fileinfo.previously_known {
            total_hashed_bytes += fileinfo.size;
        }
        total_files += 1;

        match fileinfo.hash {
            Some(hash) => println!("{:12} bytes: {} {}", fileinfo.size, hash, fileinfo.path.display()),
            None => println!("{:12} bytes: {}", fileinfo.size, fileinfo.path.display())
        }

        if total_files % args.progress_every == 0 {
            let timediff = Instant::now().duration_since(starttime);
            let bytes_per_second = (total_hashed_bytes as f32) / (timediff.as_secs() as f32);
            write!(stderr.lock(), "\r{:4} MB, {:5} files, {:.1} MB/s                 ",
                 total_bytes / 1024 / 1024,
                 total_files,
                 bytes_per_second/1024./1024.).unwrap();
        }
    }

    write!(stderr.lock(), "\r\nTotal size = {} MB", total_bytes / 1024 / 1024).unwrap();
}

type Db = HashMap<PathBuf, (Option<String>, u64)>;


fn load_from(args: &Cli) -> Result<Db, Error> {
    let mut hm = HashMap::new();

    if let Some(prev) = &args.load_from {
        let prev = fs::File::open(prev)?;
        let prev = io::BufReader::new(prev);
        use io::BufRead;
        for line in prev.lines() {
            let line = line?;
            if let Some(path_hash_size) = split_line(&line) {
                hm.insert(path_hash_size.0.into(), path_hash_size.1);
            }
        }
    }

    Ok(hm)
}

type FileList = Vec<(PathBuf, u64)>;

fn find_files(args: &Cli) -> Result<FileList, Error> {
    let w = WalkDir::new(&args.path)
        .follow_links(args.follow_links);

    let witer = w.into_iter().take(if args.count > 0 { args.count } else { std::usize::MAX } );
    
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

fn make_hashes(args: &Cli, filelist : FileList, db : &Db, progress_channel : Sender<FileInfo>) -> CliResult {

    filelist.par_iter().try_for_each(|(path, filesize)| {
        let mut previously_known = false;

        let hash = if args.hash {
            match db.get(path) {
                Some((Some(hash), size)) => {
                    if *size == *filesize {
                        previously_known = true;
                        Some(hash.clone())
                    } else {
                        hash_file(path)
                    }
                }
                _ => {
                    hash_file(path)
                }
            }                
        } else {
            None
        };

        
        progress_channel.send(FileInfo{
            path: path.clone(),
            hash,
            size: *filesize,
            previously_known
        }).unwrap();
        
        Some(())

    });

    Ok(())
}

fn hash_file(path : &Path) -> Option<String> {
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
    Some(format!("{:x}", hash))
}


fn split_line(line : &str) -> Option<(String, (Option<String>, u64))> {
    let mut abc = line.split(" bytes: ");
    let a = abc.next()?;
    let a : u64 = a.trim().parse().ok()?;
    let bc = abc.next()?;
    let mut bc = bc.splitn(2, " ");
    let b_or_c = bc.next()?;
    match bc.next() {
        Some(c) => Some((c.to_owned(), (Some(b_or_c.to_owned()), a))),
        None => Some((b_or_c.to_owned(), (None, a)))
    }
}

#[test]
fn test_split_line() {
    assert_eq!(split_line("    556602 bytes: 5172bde22e6ca41d60b4682cafa928add3e94bf6 ..\\..\\10.1007_1-4020-7830-7.pdf"),
               Some(("..\\..\\10.1007_1-4020-7830-7.pdf".to_owned(), (Some("5172bde22e6ca41d60b4682cafa928add3e94bf6".to_owned()), 556602))));

    assert_eq!(split_line("    556602 bytes: ..\\..\\10.1007_1-4020-7830-7.pdf"),
               Some(("..\\..\\10.1007_1-4020-7830-7.pdf".to_owned(), (None, 556602))));

    assert_eq!(split_line("    556602 bytes: 5172bde22e6ca41d60b4682cafa928add3e94bf6 ..\\..\\10.1007_1- 4020-7830-7.pdf"),
               Some(("..\\..\\10.1007_1- 4020-7830-7.pdf".to_owned(), (Some("5172bde22e6ca41d60b4682cafa928add3e94bf6".to_owned()), 556602))));

}