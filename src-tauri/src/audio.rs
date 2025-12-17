/// Audio-related helpers for file type detection.
///
/// Currently used to detect NIST SPHERE (`.sph`) payloads, which often have no default
/// file handler on macOS/Windows and should prompt the user to pick an opener.
pub fn is_sphere_file(data: &[u8]) -> bool {
    // NIST SPHERE files start with ASCII "NIST_1A" followed by a separator (commonly '\n').
    // Reference header example: "NIST_1A\n   1024\n"
    const MAGIC: &[u8] = b"NIST_1A";
    if data.len() < MAGIC.len() {
        return false;
    }
    &data[..MAGIC.len()] == MAGIC
}

use std::collections::HashMap;
use std::path::Path;

#[cfg(not(target_os = "windows"))]
extern "C" {
    fn litdata_sph_shorten_to_pcm16le(
        sph_path: *const std::os::raw::c_char,
        header_bytes: std::os::raw::c_long,
        pcm_path: *const std::os::raw::c_char,
    ) -> std::os::raw::c_int;
}

#[derive(Debug, Clone)]
struct SphereHeader {
    channel_count: u16,
    sample_rate: u32,
    sample_n_bytes: u16,
    sample_byte_format: Option<String>,
    sample_coding: Option<String>,
}

fn parse_sphere_header(data: &[u8]) -> Result<(SphereHeader, usize), String> {
    if !is_sphere_file(data) {
        return Err("Not a SPHERE file.".to_string());
    }
    if data.len() < 16 {
        return Err("SPHERE file is too short.".to_string());
    }

    let header_bytes = {
        let raw = &data[8..16];
        let trimmed = raw
            .iter()
            .copied()
            .take_while(|b| *b != b'\n' && *b != b'\r')
            .collect::<Vec<u8>>();
        let digits = String::from_utf8_lossy(&trimmed).trim().to_string();
        digits
            .parse::<usize>()
            .map_err(|_| "Unable to parse SPHERE header size.".to_string())?
    };

    if header_bytes == 0 || header_bytes > data.len() {
        return Err("Invalid SPHERE header size.".to_string());
    }

    let header_text = String::from_utf8_lossy(&data[..header_bytes]);
    let mut map = HashMap::<String, String>::new();
    for line in header_text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line == "NIST_1A" || line == "end_head" {
            continue;
        }
        // Common format: `<key> <type> <value...>`
        let mut parts = line.split_whitespace();
        let Some(key) = parts.next() else { continue };
        let Some(_ty) = parts.next() else { continue };
        let rest = parts.collect::<Vec<&str>>().join(" ").trim().to_string();
        if rest.is_empty() {
            continue;
        }
        map.insert(key.to_string(), rest);
    }

    let channel_count = map
        .get("channel_count")
        .or_else(|| map.get("channel_count:"))
        .ok_or_else(|| "Missing `channel_count` in SPHERE header.".to_string())?
        .parse::<u16>()
        .map_err(|_| "Invalid `channel_count` in SPHERE header.".to_string())?;

    let sample_rate = map
        .get("sample_rate")
        .ok_or_else(|| "Missing `sample_rate` in SPHERE header.".to_string())?
        .parse::<u32>()
        .map_err(|_| "Invalid `sample_rate` in SPHERE header.".to_string())?;

    let sample_n_bytes = map
        .get("sample_n_bytes")
        .ok_or_else(|| "Missing `sample_n_bytes` in SPHERE header.".to_string())?
        .parse::<u16>()
        .map_err(|_| "Invalid `sample_n_bytes` in SPHERE header.".to_string())?;

    let sample_byte_format = map.get("sample_byte_format").cloned();
    let sample_coding = map.get("sample_coding").cloned();

    Ok((
        SphereHeader {
            channel_count,
            sample_rate,
            sample_n_bytes,
            sample_byte_format,
            sample_coding,
        },
        header_bytes,
    ))
}

fn mu_law_to_i16(byte: u8) -> i16 {
    // ITU-T G.711 mu-law decoding.
    let byte = !byte;
    let sign = byte & 0x80;
    let exponent = (byte >> 4) & 0x07;
    let mantissa = byte & 0x0F;
    let mut sample = ((mantissa as i16) << 3) + 0x84;
    sample <<= exponent as i16;
    if sign != 0 {
        -sample
    } else {
        sample
    }
}

fn a_law_to_i16(byte: u8) -> i16 {
    // ITU-T G.711 A-law decoding.
    let byte = byte ^ 0x55;
    let sign = byte & 0x80;
    let exponent = (byte >> 4) & 0x07;
    let mantissa = byte & 0x0F;
    let mut sample = if exponent == 0 {
        (mantissa as i16) << 4 | 0x08
    } else {
        ((mantissa as i16) << 4) + 0x108
    };
    if exponent > 1 {
        sample <<= (exponent - 1) as i16;
    }
    if sign != 0 {
        sample
    } else {
        -sample
    }
}

pub fn write_sph_as_wav(sph_bytes: &[u8], out: &std::path::Path) -> Result<(), String> {
    let (header, header_bytes) = parse_sphere_header(sph_bytes)?;

    let coding = header
        .sample_coding
        .as_deref()
        .unwrap_or("pcm")
        .to_lowercase();
    if coding.contains("shorten") {
        return Err("Shorten-compressed SPHERE audio is not supported yet.".to_string());
    }

    let is_big_endian = header
        .sample_byte_format
        .as_deref()
        .map(|s| s.trim() == "10")
        .unwrap_or(false);

    let spec = hound::WavSpec {
        channels: header.channel_count,
        sample_rate: header.sample_rate,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    };

    let mut writer = hound::WavWriter::create(out, spec).map_err(|e| e.to_string())?;
    let payload = sph_bytes
        .get(header_bytes..)
        .ok_or_else(|| "SPHERE payload is missing.".to_string())?;

    match (coding.as_str(), header.sample_n_bytes) {
        (c, 2) if c.contains("pcm") => {
            for chunk in payload.chunks_exact(2) {
                let sample = if is_big_endian {
                    i16::from_be_bytes([chunk[0], chunk[1]])
                } else {
                    i16::from_le_bytes([chunk[0], chunk[1]])
                };
                writer.write_sample(sample).map_err(|e| e.to_string())?;
            }
        }
        (c, 1) if c.contains("pcm") => {
            // Interpret as signed 8-bit PCM and upcast to 16-bit.
            for &b in payload {
                let sample = (b as i8 as i16) << 8;
                writer.write_sample(sample).map_err(|e| e.to_string())?;
            }
        }
        (c, 1) if c.contains("ulaw") || c.contains("mulaw") || c.contains("mu-law") => {
            for &b in payload {
                writer
                    .write_sample(mu_law_to_i16(b))
                    .map_err(|e| e.to_string())?;
            }
        }
        (c, 1) if c.contains("alaw") || c.contains("a-law") => {
            for &b in payload {
                writer
                    .write_sample(a_law_to_i16(b))
                    .map_err(|e| e.to_string())?;
            }
        }
        _ => {
            return Err(format!(
                "Unsupported SPHERE coding (coding={coding}, sample_n_bytes={}).",
                header.sample_n_bytes
            ));
        }
    }

    writer.finalize().map_err(|e| e.to_string())?;
    Ok(())
}

pub fn write_sph_as_wav_with_fallback(
    sph_bytes: &[u8],
    sph_path: &Path,
    wav_path: &Path,
) -> Result<(), String> {
    // Fast path: non-shorten SPHERE can be decoded in pure Rust.
    if write_sph_as_wav(sph_bytes, wav_path).is_ok() {
        return Ok(());
    }

    let (header, header_bytes) = parse_sphere_header(sph_bytes)?;
    let coding = header
        .sample_coding
        .as_deref()
        .unwrap_or("pcm")
        .to_lowercase();
    if !coding.contains("shorten") {
        return Err("Unsupported SPHERE audio encoding.".to_string());
    }

    #[cfg(target_os = "windows")]
    {
        let _ = (sph_path, wav_path);
        return Err(
            "Shorten-compressed SPHERE audio is not supported on Windows builds.".to_string(),
        );
    }

    #[cfg(not(target_os = "windows"))]
    {
        use std::{ffi::CString, fs, io::Read};

        let sph_path_str = sph_path
            .to_str()
            .ok_or_else(|| "Input path is not valid UTF-8.".to_string())?;
        let sph_c =
            CString::new(sph_path_str).map_err(|_| "Input path contains NUL bytes.".to_string())?;

        let pcm_path = wav_path.with_extension("pcm16le");
        let pcm_path_str = pcm_path
            .to_str()
            .ok_or_else(|| "PCM path is not valid UTF-8.".to_string())?;
        let pcm_c =
            CString::new(pcm_path_str).map_err(|_| "PCM path contains NUL bytes.".to_string())?;

        let rc = unsafe {
            litdata_sph_shorten_to_pcm16le(sph_c.as_ptr(), header_bytes as _, pcm_c.as_ptr())
        };
        if rc != 0 {
            return Err(format!("Shorten decode failed (code {rc})."));
        }

        let spec = hound::WavSpec {
            channels: header.channel_count,
            sample_rate: header.sample_rate,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        };

        let mut writer = hound::WavWriter::create(wav_path, spec).map_err(|e| e.to_string())?;
        let mut f = fs::File::open(&pcm_path).map_err(|e| e.to_string())?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).map_err(|e| e.to_string())?;
        for chunk in buf.chunks_exact(2) {
            let sample = i16::from_le_bytes([chunk[0], chunk[1]]);
            writer.write_sample(sample).map_err(|e| e.to_string())?;
        }
        writer.finalize().map_err(|e| e.to_string())?;
        let _ = fs::remove_file(&pcm_path);
        Ok(())
    }
}
