use crossbeam_channel::{unbounded, Receiver, Sender};
use failure::Error;
use quicli::prelude::*;
use structopt::StructOpt;
use walkdir::WalkDir;
//use rayon::prelude::*;

use std::collections::HashMap;
use std::convert::TryFrom;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

mod fileinfo;
use fileinfo::FileInfo;

fn main() -> CliResult {
    App::from_args().run()?;
    Ok(())
}

/// Try to find out what is wrong with dads laptop
#[derive(Debug, StructOpt)]
struct App {
    /// How many lines to get (0=unlimited)
    #[structopt(long = "count", short = "n", default_value = "0")]
    count: usize,

    #[structopt(long = "follow-links", short = "f")]
    follow_links: bool,

    #[structopt(short = "h")]
    hash: bool,

    #[structopt(long = "progress_every", short = "p", default_value = "1000")]
    progress_every: u64,

    #[structopt(long = "load", short = "l")]
    load_from: Option<String>,

    #[structopt(long = "save", short = "s")]
    save_to: Option<String>,

    /// The path to crawl
    path: String,

    #[structopt(flatten)]
    verbosity: Verbosity,
}

impl App {
    pub fn run(self) -> Result<Statistic, Error> {
        let args = Arc::new(self);
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

        Ok(collector.join().expect("Collector thread paniced"))
    }
}

type BytesPerSecond = f32;

struct Progress {
    file: FileInfo,
    previously_known: bool,
    bytes_per_second: Option<BytesPerSecond>,
}

type Db = HashMap<PathBuf, FileInfo>;

type FileList = Vec<FileInfo>;

#[derive(Default, Debug)]
struct Statistic {
    num_hashes_reused: u64,
    total_bytes: u64,
    total_hashed_bytes: u64,
    total_files: u64,
}

fn collector_thread(args: &App, progress_channel_r: Receiver<Progress>) -> Statistic {
    let mut stats = Statistic::default();

    let starttime = Instant::now();
    //let progress_every = Duration::from_millis(1000);
    //if now.duration_since(last_progress) > progress_every {
    //    last_progress = now;
    use io::Write;
    let stderr = std::io::stderr();

    let mut save_to: Box<dyn Write> = match &args.save_to {
        Some(save_to) => Box::new(io::BufWriter::new(
            std::fs::File::create(&save_to[..]).expect("Could not open output file."),
        )),
        None => Box::new(std::io::stdout()),
    };

    for progress in progress_channel_r.iter() {
        stats.total_bytes += progress.file.size;
        if !progress.previously_known {
            stats.total_hashed_bytes += progress.file.size;
        } else {
            stats.num_hashes_reused += 1;
        }
        stats.total_files += 1;

        write!(save_to, "{}\r\n", progress.file).expect("Could not write to output file");

        if stats.total_files % args.progress_every == 0 {
            let timediff = Instant::now().duration_since(starttime);
            let bytes_per_second =
                (stats.total_hashed_bytes as f32) / (timediff.as_millis() as f32) * 1000.;
            write!(
                stderr.lock(),
                "\r{:4} MB, {:5} files, {:.1} MB/s | {:.1} MB/s {:<9}",
                stats.total_bytes / 1024 / 1024,
                stats.total_files,
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
        stats.total_bytes / 1024 / 1024
    )
    .unwrap();

    stats
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

fn find_files(args: &App) -> Result<FileList, Error> {
    let w = WalkDir::new(&args.path).follow_links(args.follow_links);

    let witer = w.into_iter().take(if args.count > 0 {
        args.count as usize
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
    args: &App,
    filelist: FileList,
    db: &Db,
    progress_channel: Sender<Progress>,
) -> Result<(), Error> {
    filelist.par_iter().try_for_each(|fileinfo| {
        let mut fileinfo = fileinfo.clone();
        let mut previously_known = false;

        let bps = if args.hash {
            match db.get(&fileinfo.path) {
                Some(FileInfo { size: dbsize, .. }) => {
                    if fileinfo.size == *dbsize {
                        previously_known = true;
                        //Some((dbhash.clone(), 0.))
                        0.
                    } else {
                        fileinfo.generate_hash().unwrap()
                        //hash_file(&fileinfo.path, fileinfo.size)
                    }
                }
                _ => {
                    // use io::Write;
                    // write!(
                    //     std::io::stderr().lock(),
                    //     "\n{:?}\n{}\n",
                    //     fileinfo.path,
                    //     fileinfo
                    // )
                    // .unwrap();
                    fileinfo.generate_hash().unwrap()
                    //hash_file(&fileinfo.path, fileinfo.size)
                }
            }
        } else {
            0.
        };

        //let bps = hash_bps.as_ref().map(|s| s.1);

        progress_channel
            .send(Progress {
                file: fileinfo,
                previously_known,
                bytes_per_second: Some(bps),
            })
            .unwrap();

        Some(())
    });

    Ok(())
}

#[test]
fn test_excercise_programm() {
    let args = App {
        count: 0,
        follow_links: true,
        hash: true,
        progress_every: 1,
        load_from: None,
        save_to: Some("target/test.db".to_string()),
        path: "test".to_owned(),
        verbosity: Verbosity::from_iter(&[""][..]),
    };
    args.run().unwrap();

    let args = App {
        count: 0,
        follow_links: true,
        hash: true,
        progress_every: 1,
        load_from: Some("target/test.db".to_string()),
        save_to: Some("target/test2.db".to_string()),
        path: "test".to_owned(),
        verbosity: Verbosity::from_iter(&[""][..]),
    };
    let stat = args.run().unwrap();
    assert_eq!(stat.num_hashes_reused, 2);
    assert_eq!(stat.total_hashed_bytes, 0);

    //let db = load_from("accessed_files2").unwrap();
    //println!("Read entries: {}", db.len());
}
