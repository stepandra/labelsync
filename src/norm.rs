//! Address / field normalization.
//!
//! Rules:
//! * EVM addresses (`0x` + 40 hex) are lowercased (EIP-55 checksum casing is dropped).
//! * Bech32/bech32m addresses (single-case, `<hrp>1<data>` with the bech32 charset)
//!   are lowercased. Bech32 is case-insensitive by spec.
//! * Everything else (base58: BTC legacy/P2SH, Solana, Tron, ...) is case-sensitive
//!   and only trimmed.

const BECH32_CHARSET: &str = "qpzry9x8gf2tvdw0s3jn54khce6mua7l";

pub fn normalize_chain(chain: &str) -> String {
    clean_field(chain).to_ascii_lowercase()
}

/// Collapse tabs/newlines into spaces and trim, so a field can never break TSV.
pub fn clean_field(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c == '\t' || c == '\n' || c == '\r' || c.is_control() {
            out.push(' ');
        } else {
            out.push(c);
        }
    }
    out.trim().to_string()
}

pub fn is_evm(addr: &str) -> bool {
    let b = addr.as_bytes();
    b.len() == 42
        && (addr.starts_with("0x") || addr.starts_with("0X"))
        && b[2..].iter().all(|c| c.is_ascii_hexdigit())
}

pub fn is_bech32(addr: &str) -> bool {
    if addr.len() < 8 || addr.len() > 120 {
        return false;
    }
    let has_lower = addr.chars().any(|c| c.is_ascii_lowercase());
    let has_upper = addr.chars().any(|c| c.is_ascii_uppercase());
    if has_lower && has_upper {
        return false; // mixed case is invalid bech32 -> treat as case-sensitive
    }
    let lower = addr.to_ascii_lowercase();
    let Some(pos) = lower.rfind('1') else {
        return false;
    };
    let (hrp, data) = (&lower[..pos], &lower[pos + 1..]);
    if hrp.is_empty() || hrp.len() > 83 || data.len() < 6 {
        return false;
    }
    if !hrp.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()) {
        return false;
    }
    // require hrp to start with a letter (rules out pure numeric strings)
    if !hrp.chars().next().unwrap().is_ascii_lowercase() {
        return false;
    }
    data.chars().all(|c| BECH32_CHARSET.contains(c))
}

pub fn normalize_address(addr: &str) -> String {
    let a = clean_field(addr);
    if is_evm(&a) {
        return format!("0x{}", a[2..].to_ascii_lowercase());
    }
    if is_bech32(&a) {
        return a.to_ascii_lowercase();
    }
    a
}

pub fn normalize_op(op: &str) -> Result<&'static str, String> {
    match op.trim().to_ascii_lowercase().as_str() {
        "" | "add" | "+" | "set" => Ok("add"),
        "del" | "delete" | "-" | "tombstone" | "revoke" | "remove" => Ok("del"),
        other => Err(format!("unknown op '{other}' (use add|del)")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evm_lowercased() {
        assert_eq!(
            normalize_address(" 0xAbCdEf0123456789aBcDeF0123456789ABCDEF01 "),
            "0xabcdef0123456789abcdef0123456789abcdef01"
        );
        assert_eq!(
            normalize_address("0XABCDEF0123456789ABCDEF0123456789ABCDEF01"),
            "0xabcdef0123456789abcdef0123456789abcdef01"
        );
    }

    #[test]
    fn evm_wrong_length_untouched() {
        assert_eq!(normalize_address("0xABC"), "0xABC");
    }

    #[test]
    fn bech32_lowercased() {
        assert_eq!(
            normalize_address("BC1QAR0SRRR7XFKVY5L643LYDNW9RE59GTZZWF5MDQ"),
            "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq"
        );
        assert_eq!(
            normalize_address("cosmos1hsk6jryyqjfhp5dhc55tc9jtckygx0eph6dd02"),
            "cosmos1hsk6jryyqjfhp5dhc55tc9jtckygx0eph6dd02"
        );
    }

    #[test]
    fn base58_untouched() {
        // BTC legacy
        assert_eq!(
            normalize_address("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"),
            "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"
        );
        // Solana
        assert_eq!(
            normalize_address("So11111111111111111111111111111111111111112"),
            "So11111111111111111111111111111111111111112"
        );
        // Tron
        assert_eq!(
            normalize_address("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"),
            "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
        );
        // mixed-case bech32-looking string is not bech32 -> untouched
        assert_eq!(normalize_address("bc1QAR0srrr7xfkvy5l643"), "bc1QAR0srrr7xfkvy5l643");
    }

    #[test]
    fn fields_cleaned() {
        assert_eq!(clean_field(" a\tb\nc "), "a b c");
        assert_eq!(normalize_chain(" ETH "), "eth");
    }

    #[test]
    fn ops() {
        assert_eq!(normalize_op("").unwrap(), "add");
        assert_eq!(normalize_op("Tombstone").unwrap(), "del");
        assert!(normalize_op("zzz").is_err());
    }
}
