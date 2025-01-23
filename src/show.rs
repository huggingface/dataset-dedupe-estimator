use std::cmp::min;
use std::fs::File;
use std::io::{self, Write};

#[derive(Copy, Clone)]
struct RGB {
    r: u8,
    g: u8,
    b: u8,
}

#[derive(Copy, Clone)]
struct FRGB {
    r: f32,
    g: f32,
    b: f32,
}

const IMAGE_DIM: usize = 256;
const BLOCK_DIM: usize = 8;
const SEQUENCE_LENGTH: usize = (IMAGE_DIM / BLOCK_DIM) * IMAGE_DIM;

const COLORS: [FRGB; 16] = [
    FRGB {
        r: 0.0,
        g: 255.0,
        b: 0.0,
    }, // Green
    FRGB {
        r: 255.0,
        g: 0.0,
        b: 0.0,
    }, // Red
    FRGB {
        r: 0.0,
        g: 0.0,
        b: 255.0,
    }, // Blue
    FRGB {
        r: 255.0,
        g: 255.0,
        b: 0.0,
    }, // Yellow
    FRGB {
        r: 255.0,
        g: 165.0,
        b: 0.0,
    }, // Orange
    FRGB {
        r: 128.0,
        g: 0.0,
        b: 128.0,
    }, // Purple
    FRGB {
        r: 0.0,
        g: 255.0,
        b: 255.0,
    }, // Cyan
    FRGB {
        r: 255.0,
        g: 0.0,
        b: 255.0,
    }, // Magenta
    FRGB {
        r: 192.0,
        g: 192.0,
        b: 192.0,
    }, // Silver
    FRGB {
        r: 128.0,
        g: 128.0,
        b: 128.0,
    }, // Gray
    FRGB {
        r: 128.0,
        g: 0.0,
        b: 0.0,
    }, // Maroon
    FRGB {
        r: 128.0,
        g: 128.0,
        b: 0.0,
    }, // Olive
    FRGB {
        r: 0.0,
        g: 128.0,
        b: 0.0,
    }, // Dark Green
    FRGB {
        r: 0.0,
        g: 128.0,
        b: 128.0,
    }, // Teal
    FRGB {
        r: 0.0,
        g: 0.0,
        b: 128.0,
    }, // Navy
    FRGB {
        r: 255.0,
        g: 105.0,
        b: 180.0,
    }, // Hot Pink
];

fn interpolate_sample(s: &[usize], pos: f32) -> FRGB {
    if pos == pos.floor() {
        let mut ipos = pos as isize;
        if ipos < 0 {
            ipos = 0;
        }
        if ipos >= s.len() as isize {
            ipos = s.len() as isize - 1;
        }
        COLORS[s[ipos as usize]]
    } else {
        let mut ipos = pos as isize;
        if ipos < 0 {
            ipos = 0;
        }
        if ipos >= s.len() as isize {
            ipos = s.len() as isize - 1;
        }
        let left_weight = 1.0 - (pos - ipos as f32);
        let right_weight = 1.0 - left_weight;
        let color_left = COLORS[s[ipos as usize]];
        let color_right = COLORS[s[min(ipos as usize + 1, s.len() - 1)]];
        FRGB {
            r: left_weight * color_left.r + right_weight * color_right.r,
            g: left_weight * color_left.g + right_weight * color_right.g,
            b: left_weight * color_left.b + right_weight * color_right.b,
        }
    }
}

fn generate_color_sequence(s: &[usize]) -> Vec<RGB> {
    let mut ret = Vec::new();
    for i in 0..SEQUENCE_LENGTH {
        let fpos = (i as f32 * s.len() as f32) / SEQUENCE_LENGTH as f32;
        let fnextpos = ((i + 1) as f32 * s.len() as f32) / SEQUENCE_LENGTH as f32;
        let mut color = FRGB {
            r: 0.0,
            g: 0.0,
            b: 0.0,
        };
        let mut weight = 0.0;
        //for j in (fpos as usize)..(fnextpos as usize) {
        let mut j = fpos;
        while j < fnextpos {
            let sample = interpolate_sample(s, j);
            let w = (fnextpos - fpos).max(1.0);
            color.r += sample.r * w;
            color.g += sample.g * w;
            color.b += sample.b * w;
            weight += w;
            j += 1.0;
        }
        color.r /= weight;
        color.g /= weight;
        color.b /= weight;
        ret.push(RGB {
            r: color.r.min(255.0).max(0.0) as u8,
            g: color.g.min(255.0).max(0.0) as u8,
            b: color.b.min(255.0).max(0.0) as u8,
        });
    }
    ret
}

pub(crate) fn write_ppm(segments: &[usize], filename: &str) -> io::Result<()> {
    let colors = generate_color_sequence(segments);
    let mut fout = File::create(filename)?;
    writeln!(fout, "P6")?;
    writeln!(fout, "{} {}", IMAGE_DIM, IMAGE_DIM)?;
    writeln!(fout, "255")?;
    // each position in colors is an 1x8 block in the image
    for i in 0..IMAGE_DIM {
        for j in 0..IMAGE_DIM {
            let block_x = i / BLOCK_DIM;
            let block_y = j;
            let block_idx = block_x * IMAGE_DIM + block_y;
            let color = colors[block_idx];
            fout.write_all(&[color.r, color.g, color.b])?;
        }
    }
    Ok(())
}
