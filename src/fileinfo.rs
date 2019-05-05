
use std::path::PathBuf;
use std::time::{SystemTime, Duration, UNIX_EPOCH};
use std::fmt;
use std::convert::TryFrom;


#[derive(Debug, Eq, PartialEq)]
pub struct FileInfo {
    pub path: PathBuf,
    pub hash: Option<String>,
    pub modified: Option<SystemTime>,
    pub size: u64,
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
fn test_serde_fileinfo() {
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