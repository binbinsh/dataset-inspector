pub const PREVIEW_TEXT_CHARS: usize = 8 * 1024;

pub fn preview_utf8_text(data: &[u8]) -> Option<String> {
    let raw = match std::str::from_utf8(data) {
        Ok(text) => text,
        Err(err) if err.error_len().is_none() => {
            std::str::from_utf8(&data[..err.valid_up_to()]).ok()?
        }
        Err(_) => return None,
    };
    Some(raw.chars().take(PREVIEW_TEXT_CHARS).collect())
}

