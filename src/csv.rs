//! The CSV dialect of rules.md: `\n` ends a line, every comma separates fields,
//! a field is either free of quotes or wrapped in exactly one pair of them.

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// Field spans (start, end) relative to the line start, with quotes stripped.
pub type Spans<const N: usize> = [(u32, u32); N];

/// Bytes of readable zero padding that must follow the data in every buffer.
pub const PAD: usize = 192;

/// Scalar splitter for any line (no `\n` inside).
pub fn split_scalar<const N: usize>(line: &[u8]) -> Option<Spans<N>> {
    if line.is_empty() {
        return None;
    }
    let mut out = [(0u32, 0u32); N];
    let mut field = 0usize;
    let mut a = 0usize;
    loop {
        let b = match line[a..].iter().position(|&c| c == b',') {
            Some(off) => a + off,
            None => line.len(),
        };
        if field >= N {
            return None;
        }
        let f = &line[a..b];
        let (s, e) = if !f.is_empty() && f[0] == b'"' {
            if f.len() < 2 || f[f.len() - 1] != b'"' || f[1..f.len() - 1].contains(&b'"') {
                return None;
            }
            (a + 1, b - 1)
        } else {
            if f.contains(&b'"') {
                return None;
            }
            (a, b)
        };
        out[field] = (s as u32, e as u32);
        field += 1;
        if b == line.len() {
            break;
        }
        a = b + 1;
    }
    if field != N {
        return None;
    }
    Some(out)
}

#[inline(always)]
unsafe fn masks(p: *const u8) -> ([u64; 2], [u64; 2], [u64; 2]) {
    #[cfg(all(target_feature = "avx512bw", target_feature = "avx512vl"))]
    {
        let nlv = _mm256_set1_epi8(b'\n' as i8);
        let cv = _mm256_set1_epi8(b',' as i8);
        let qv = _mm256_set1_epi8(b'"' as i8);
        let v0 = _mm256_loadu_si256(p as *const __m256i);
        let v1 = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let v2 = _mm256_loadu_si256(p.add(64) as *const __m256i);
        let v3 = _mm256_loadu_si256(p.add(96) as *const __m256i);
        let m = |v, x| _mm256_cmpeq_epi8_mask(v, x) as u64;
        (
            [m(v0, nlv) | (m(v1, nlv) << 32), m(v2, nlv) | (m(v3, nlv) << 32)],
            [m(v0, cv) | (m(v1, cv) << 32), m(v2, cv) | (m(v3, cv) << 32)],
            [m(v0, qv) | (m(v1, qv) << 32), m(v2, qv) | (m(v3, qv) << 32)],
        )
    }
    #[cfg(not(all(target_feature = "avx512bw", target_feature = "avx512vl")))]
    {
        let nlv = _mm256_set1_epi8(b'\n' as i8);
        let cv = _mm256_set1_epi8(b',' as i8);
        let qv = _mm256_set1_epi8(b'"' as i8);
        let mut out = [[0u64; 2]; 3];
        for k in 0..4 {
            let v = _mm256_loadu_si256(p.add(32 * k) as *const __m256i);
            let sh = 32 * (k & 1);
            out[0][k >> 1] |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, nlv)) as u32 as u64) << sh;
            out[1][k >> 1] |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, cv)) as u32 as u64) << sh;
            out[2][k >> 1] |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, qv)) as u32 as u64) << sh;
        }
        (out[0], out[1], out[2])
    }
}

#[inline(always)]
fn low_bits(n: usize) -> u64 {
    if n >= 64 {
        !0
    } else {
        (1u64 << n) - 1
    }
}

/// Splits the line starting at `p`. `avail` is the number of data bytes from
/// `p` to the end of the data (at least 1); `PAD` readable bytes follow the data.
/// Returns (line length without the `\n`, fields if well-formed with N fields).
#[inline(always)]
pub unsafe fn split_line<const N: usize>(p: *const u8, avail: usize) -> (usize, Option<Spans<N>>) {
    split_line_lvl::<N, 9>(p, avail)
}
#[inline(always)]
pub unsafe fn split_line_lvl<const N: usize, const L: u32>(p: *const u8, avail: usize) -> (usize, Option<Spans<N>>) {
    let (nl, cm, qm) = masks(p);
    if L == 0 { let x = nl[0] ^ cm[1] ^ qm[0]; return ((x & 63) as usize + 70, None); }
    let lim: u128 = if avail >= 128 { !0 } else { (1u128 << avail) - 1 };
    let nl = (nl[0] as u128 | ((nl[1] as u128) << 64)) & lim;
    let len = if nl != 0 {
        nl.trailing_zeros() as usize
    } else if avail < 128 {
        avail
    } else {
        // long line: find its end and split it the slow way
        let s = std::slice::from_raw_parts(p, avail);
        let len = s.iter().position(|&c| c == b'\n').unwrap_or(avail);
        return (len, split_scalar::<N>(&s[..len]));
    };
    let region: u128 = if len >= 128 { !0 } else { (1u128 << len) - 1 };
    let mut c = (cm[0] as u128 | ((cm[1] as u128) << 64)) & region;
    let q = (qm[0] as u128 | ((qm[1] as u128) << 64)) & region;
    if c.count_ones() as usize != N - 1 || len == 0 {
        return (len, None);
    }
    if L == 1 { return (len, Some([((c as u32) ^ q as u32, 0); N])); }
    let mut out = [(0u32, 0u32); N];
    let mut a = 0u32;
    for i in 0..N - 1 {
        let b = c.trailing_zeros();
        c &= c.wrapping_sub(1);
        out[i] = (a, b);
        a = b + 1;
    }
    out[N - 1] = (a, len as u32);
    if L == 2 { return (len, Some(out)); }
    if q != 0 {
        // Field starts are bit 0 and the bits after commas; M marks every separator
        // (the commas and the line end). For the start-quoted fields X, M & !(M - X)
        // is the separator ending each of them, so their closing quotes sit one bit
        // lower. The line is well-formed iff the quotes are exactly these pairs and
        // no quoted field has length 1.
        let cm_all = (cm[0] as u128 | ((cm[1] as u128) << 64)) & region;
        let starts = (cm_all << 1) | 1;
        let m = cm_all | (1u128 << len);
        let x = q & starts;
        let ends = (m & !m.wrapping_sub(x)) >> 1;
        if q != (x | ends) || (x & ends) != 0 {
            return (len, None);
        }
        for i in 0..N {
            let (a, b) = out[i];
            let qs = (*p.add(a as usize) == b'"') as u32;
            out[i] = (a + qs, b - qs);
        }
    }
    (len, Some(out))
}

/// Calls `f(line)` for every line of a small, fully loaded file except the header.
pub fn for_each_line(data: &[u8], mut f: impl FnMut(&[u8])) {
    let mut start = 0usize;
    let mut first = true;
    while start < data.len() {
        let end = data[start..].iter().position(|&c| c == b'\n').map_or(data.len(), |o| start + o);
        if !first {
            f(&data[start..end]);
        }
        first = false;
        start = end + 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn check(line: &[u8]) {
        let mut buf = line.to_vec();
        buf.push(b'\n');
        buf.extend_from_slice(b"x,y,z\n");
        buf.resize(buf.len() + PAD + 200, 0);
        let (len, a) = unsafe { split_line::<3>(buf.as_ptr(), line.len() + 7) };
        assert_eq!(len, line.len());
        assert_eq!(a, split_scalar::<3>(line), "{:?}", std::str::from_utf8(line));
    }
    #[test]
    fn random_lines() {
        let mut x: u64 = 0x1234_5678;
        let alpha = [b'a', b'b', b',', b'"', b' ', b'\r', b'1'];
        for _ in 0..300_000 {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let len = ((x >> 33) % 140) as usize;
            let mut line = Vec::with_capacity(len);
            for _ in 0..len {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let r = (x >> 40) as usize;
                // bias toward letters so well-formed lines are common
                let c = if r % 4 == 0 { alpha[(r / 4) % alpha.len()] } else { b'a' };
                line.push(c);
            }
            check(&line);
            let mut buf = line.clone();
            buf.resize(buf.len() + PAD, 0);
            if !line.is_empty() {
                let (l, a) = unsafe { split_line::<3>(buf.as_ptr(), line.len()) };
                assert_eq!(l, line.len());
                assert_eq!(a, split_scalar::<3>(&line));
                let (_, b) = unsafe { split_line::<7>(buf.as_ptr(), line.len()) };
                assert_eq!(b, split_scalar::<7>(&line));
            }
        }
    }
    #[test]
    fn split() {
        for l in [
            &b"a,b,c"[..], b"\"a\",b,\"\"", b"a,b", b"a,b,c,d", b"\"a,b,c", b"a\",b,c", b"\",b,c", b"\"\"\",b,c", b"", b",,",
            b"a, b ,\" c\"", b"ab\"c,d,e",
        ] {
            check(l);
        }
        assert_eq!(split_scalar::<3>(b"\"a\",b,\"\""), Some([(1, 2), (4, 5), (7, 7)]));
        let long = [b'x'; 300];
        let mut l = long.to_vec();
        l.extend_from_slice(b",\"q\",z");
        check(&l);
    }
}
