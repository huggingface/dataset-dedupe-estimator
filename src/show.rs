use png::Encoder;
use std::cmp::min;
use std::fs::File;
use std::io;

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

const COLORS: [FRGB; 32] = [
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
    FRGB {
        r: 255.0,
        g: 20.0,
        b: 147.0,
    }, // Deep Pink
    FRGB {
        r: 75.0,
        g: 0.0,
        b: 130.0,
    }, // Indigo
    FRGB {
        r: 240.0,
        g: 230.0,
        b: 140.0,
    }, // Khaki
    FRGB {
        r: 173.0,
        g: 216.0,
        b: 230.0,
    }, // Light Blue
    FRGB {
        r: 255.0,
        g: 182.0,
        b: 193.0,
    }, // Light Pink
    FRGB {
        r: 144.0,
        g: 238.0,
        b: 144.0,
    }, // Light Green
    FRGB {
        r: 255.0,
        g: 255.0,
        b: 224.0,
    }, // Light Yellow
    FRGB {
        r: 0.0,
        g: 255.0,
        b: 127.0,
    }, // Spring Green
    FRGB {
        r: 70.0,
        g: 130.0,
        b: 180.0,
    }, // Steel Blue
    FRGB {
        r: 210.0,
        g: 105.0,
        b: 30.0,
    }, // Chocolate
    FRGB {
        r: 255.0,
        g: 69.0,
        b: 0.0,
    }, // Orange Red
    FRGB {
        r: 255.0,
        g: 228.0,
        b: 181.0,
    }, // Moccasin
    FRGB {
        r: 255.0,
        g: 218.0,
        b: 185.0,
    }, // Peach Puff
    FRGB {
        r: 255.0,
        g: 240.0,
        b: 245.0,
    }, // Lavender Blush
    FRGB {
        r: 255.0,
        g: 250.0,
        b: 205.0,
    }, // Lemon Chiffon
    FRGB {
        r: 255.0,
        g: 228.0,
        b: 225.0,
    }, // Misty Rose
];

#[inline(always)]
fn getcolor(i: usize) -> FRGB {
    COLORS[i % COLORS.len()]
}

fn interpolate_sample(s: &[usize], pos: f32) -> FRGB {
    if pos == pos.floor() {
        let mut ipos = pos as isize;
        if ipos < 0 {
            ipos = 0;
        }
        if ipos >= s.len() as isize {
            ipos = s.len() as isize - 1;
        }
        return getcolor(s[ipos as usize]);
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
        let color_left = getcolor(s[ipos as usize]);
        let color_right = getcolor(s[min(ipos as usize + 1, s.len() - 1)]);
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
        let mut fpos = (i * s.len()) as f32 / SEQUENCE_LENGTH as f32;
        let fnextpos = ((i + 1) * s.len()) as f32 / SEQUENCE_LENGTH as f32;
        if fpos > (s.len() - 1) as f32 {
            fpos = (s.len() - 1) as f32;
        }
        let mut color = FRGB {
            r: 0.0,
            g: 0.0,
            b: 0.0,
        };
        let mut weight = 0.0;

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

pub(crate) fn write_png(segments: &[usize], filename: &str) -> io::Result<()> {
    let colors = generate_color_sequence(segments);
    let file = File::create(filename)?;
    let ref mut w = io::BufWriter::new(file);

    let mut encoder = Encoder::new(w, IMAGE_DIM as u32, IMAGE_DIM as u32);
    encoder.set_color(png::ColorType::Rgb);
    encoder.set_depth(png::BitDepth::Eight);
    let mut writer = encoder.write_header().unwrap();

    let mut data = Vec::with_capacity(IMAGE_DIM * IMAGE_DIM * 3);
    for i in 0..IMAGE_DIM {
        for j in 0..IMAGE_DIM {
            let block_x = i / BLOCK_DIM;
            let block_y = j;
            let block_idx = block_x * IMAGE_DIM + block_y;
            let color = colors[block_idx];
            data.push(color.r);
            data.push(color.g);
            data.push(color.b);
        }
    }
    writer.write_image_data(&data).unwrap();
    Ok(())
}
