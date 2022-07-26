use crate::geom::{LineString, Point, Polygon};
use crate::lineclip;
use mbtiles_tool::vector_tile;

pub fn zz_enc(n: i32) -> u32 {
  ((n << 1) ^ (n >> 31)) as u32
}

pub fn zz_dec(n: u32) -> i32 {
  ((n >> 1) as i32) ^ (-((n & 1) as i32))
}

pub struct Command {
  pub id: u8,
  pub count: u32,
}
// export const parseCommand = (value: number) => ({id: value & 0x7, count: value >> 3 });
// export const encodeCommand = (cmd: {id: number, count: number}) => (cmd.id & 0x7) | (cmd.count << 3)

pub fn parse_command(value: u32) -> Command {
  Command {
    id: (value & 0x7) as u8,
    count: value >> 3,
  }
}

pub fn encode_command(cmd: Command) -> u32 {
  (cmd.id as u32 & 0x7) | (cmd.count << 3)
}

pub fn clip_points_to_bbox(points: Vec<Point>, min: i32, max: i32) -> Vec<Point> {
  points
    .into_iter()
    .filter(|&Point { x, y }| (min <= x && x <= max && min <= y && y <= max))
    .collect()
}

fn decode_points(geometry: &[u32]) -> Vec<Point> {
  let mut points = Vec::<Point>::new();

  let mut cursor_x: i32 = 0;
  let mut cursor_y: i32 = 0;

  let mut i: usize = 0;

  while i < geometry.len() {
    let cmd = parse_command(geometry[i]);
    if cmd.id == 1 {
      i += 1;
      let starting_i = i;
      while i < starting_i + (cmd.count * 2) as usize {
        let x = zz_dec(geometry[i]);
        let y = zz_dec(geometry[i + 1]);
        cursor_x += x;
        cursor_y += y;

        points.push(Point {
          x: cursor_x,
          y: cursor_y,
        });

        i += 2;
      }
    }
  }
  points
}

fn decode_linestrings(geometry: &[u32]) -> Vec<LineString> {
  let mut lines = Vec::<LineString>::new();

  let mut cursor_x: i32 = 0;
  let mut cursor_y: i32 = 0;
  let mut i: usize = 0;

  let mut coord_buffer = Vec::<Point>::new();

  while i < geometry.len() {
    let cmd = parse_command(geometry[i]);
    if cmd.id == 1 || cmd.id == 2 {
      i += 1;
      let starting_i = i;
      while i < starting_i + (cmd.count * 2) as usize {
        let x = zz_dec(geometry[i]);
        let y = zz_dec(geometry[i + 1]);
        cursor_x += x;
        cursor_y += y;

        if cmd.id == 1 {
          // moveTo in a linestring context means a new line is started at that point
          if !coord_buffer.is_empty() {
            lines.push(LineString {
              points: coord_buffer.clone(),
            });
          }
          coord_buffer = vec![Point {
            x: cursor_x,
            y: cursor_y,
          }];
        } else if cmd.id == 2 {
          coord_buffer.push(Point {
            x: cursor_x,
            y: cursor_y,
          });
        }

        i += 2;
      }
    }
  }

  lines
}

fn decode_polygons(geometry: &[u32]) -> Vec<Polygon> {
  let mut polygons = Vec::<Polygon>::new();

  let mut cursor_x: i32 = 0;
  let mut cursor_y: i32 = 0;
  let mut i: usize = 0;

  let mut coord_buffer = Vec::<Point>::new();

  while i < geometry.len() {
    let cmd = parse_command(geometry[i]);
    if cmd.id == 1 || cmd.id == 2 {
      i += 1;
      let starting_i = i;
      while i < starting_i + (cmd.count * 2) as usize {
        let x = zz_dec(geometry[i]);
        let y = zz_dec(geometry[i + 1]);
        cursor_x += x;
        cursor_y += y;

        if cmd.id == 1 {
          coord_buffer = vec![Point {
            x: cursor_x,
            y: cursor_y,
          }];
        } else if cmd.id == 2 {
          coord_buffer.push(Point {
            x: cursor_x,
            y: cursor_y,
          });
        }

        i += 2;
      }
    } else if cmd.id == 7 {
      // close path -- polygon ends here
      polygons.push(Polygon {
        points: coord_buffer.clone(),
      });
      coord_buffer = vec![];

      i += 1;
    }
  }

  polygons
}

fn encode_points(points: &[Point]) -> Vec<u32> {
  if points.is_empty() {
    return vec![];
  }
  let mut out = vec![encode_command(Command {
    id: 1,
    count: points.len() as u32,
  })];
  let mut c_x: Option<i32> = None;
  let mut c_y: Option<i32> = None;
  for point in points {
    let Point { x, y } = point;
    if let (Some(cx), Some(cy)) = (c_x, c_y) {
      out.push(zz_enc(x - cx));
      out.push(zz_enc(y - cy));
      c_x = Some(*x);
      c_y = Some(*y);
    } else {
      c_x = Some(*x);
      c_y = Some(*y);
      out.push(zz_enc(*x));
      out.push(zz_enc(*y));
    }
  }
  out
}

fn encode_linestrings(linestrings: &[LineString]) -> Vec<u32> {
  if linestrings.is_empty() {
    return vec![];
  }
  let mut out: Vec<u32> = vec![];
  let mut c_x: Option<i32> = None;
  let mut c_y: Option<i32> = None;
  for line in linestrings {
    if line.points.is_empty() {
      // this line was completely clipped out
      continue;
    }
    let first_point = line.points[0];
    out.push(encode_command(Command { id: 1, count: 1 }));

    if let (Some(cx), Some(cy)) = (c_x, c_y) {
      out.push(zz_enc(first_point.x - cx));
      out.push(zz_enc(first_point.y - cy));
      c_x = Some(first_point.x);
      c_y = Some(first_point.y);
    } else {
      c_x = Some(first_point.x);
      c_y = Some(first_point.y);
      out.push(zz_enc(first_point.x));
      out.push(zz_enc(first_point.y));
    }

    out.push(encode_command(Command {
      id: 2,
      count: (line.points.len() - 1) as u32,
    }));

    for point in line.points.iter().skip(1) {
      if let (Some(cx), Some(cy)) = (c_x, c_y) {
        out.push(zz_enc(point.x - cx));
        out.push(zz_enc(point.y - cy));
        c_x = Some(point.x);
        c_y = Some(point.y);
      } else {
        panic!("shouldn't happen");
      }
    }
  }
  out
}

fn encode_polygons(polygons: &[Polygon]) -> Vec<u32> {
  let mut out: Vec<u32> = vec![];
  let mut c_x: Option<i32> = None;
  let mut c_y: Option<i32> = None;
  for polygon in polygons {
    let points = &polygon.points;
    if points.is_empty() {
      // this polygon was completely clipped out
      continue;
    }
    let first_point = points[0];
    out.push(encode_command(Command { id: 1, count: 1 }));
    if let (Some(cx), Some(cy)) = (c_x, c_y) {
      out.push(zz_enc(first_point.x - cx));
      out.push(zz_enc(first_point.y - cy));
      c_x = Some(first_point.x);
      c_y = Some(first_point.y);
    } else {
      c_x = Some(first_point.x);
      c_y = Some(first_point.y);
      out.push(zz_enc(first_point.x));
      out.push(zz_enc(first_point.y));
    }

    out.push(encode_command(Command {
      id: 2,
      count: (points.len() - 1) as u32,
    }));

    for point in points.iter().skip(1) {
      if let (Some(cx), Some(cy)) = (c_x, c_y) {
        out.push(zz_enc(point.x - cx));
        out.push(zz_enc(point.y - cy));
        c_x = Some(point.x);
        c_y = Some(point.y);
      } else {
        panic!("shouldn't happen");
      }
    }

    out.push(encode_command(Command { id: 7, count: 0 }));
  }
  out
}

// how many bits right the extent should be shifted. For example, a tile with extent 4096 will have a buffer of 256. Extent 256 will have a buffer of 16.
const CLIP_BUFFER: u8 = 4;

pub fn clip_geometry(geom_type: i32, geometry: &[u32], extent: u32) -> Vec<u32> {
  let buffer_pixels = (extent >> CLIP_BUFFER) as i32;
  let min = -buffer_pixels;
  let max = (extent as i32) + buffer_pixels;

  if geom_type == vector_tile::tile::GeomType::Point as i32 {
    let points = decode_points(geometry);
    let clipped = clip_points_to_bbox(points, min, max);
    return encode_points(&clipped);
  } else if geom_type == vector_tile::tile::GeomType::Linestring as i32 {
    let lines = decode_linestrings(geometry);
    let mut clipped_lines = Vec::<LineString>::new();
    for line in lines {
      let mut clipped = lineclip::lineclip(line, (min, min, max, max));
      clipped_lines.append(&mut clipped);
    }
    return encode_linestrings(&clipped_lines);
  } else if geom_type == vector_tile::tile::GeomType::Polygon as i32 {
    let polygons = decode_polygons(geometry);
    let clipped_polygons = polygons
      .iter()
      .map(|polygon| lineclip::polygonclip(polygon.clone(), (min, min, max, max)));
    return encode_polygons(&clipped_polygons.collect::<Vec<Polygon>>());
  }

  panic!("Unsupported geometry type");
}

fn scale_geometry(geometry: &mut [u32], new_extent: u32, rel_x: u32, rel_y: u32) -> bool {
  if geometry.is_empty() {
    return false;
  }
  let cmd = parse_command(geometry[0]);
  // non-moveto for first point? I don't know what to do with that
  if cmd.id != 1 {
    return false;
  }
  let orig_x = zz_dec(geometry[1]);
  let orig_y = zz_dec(geometry[2]);
  let scaled_x = orig_x - (new_extent * rel_x) as i32;
  let scaled_y = orig_y - (new_extent * rel_y) as i32;
  geometry[1] = zz_enc(scaled_x);
  geometry[2] = zz_enc(scaled_y);

  true
}

pub fn scale_tile(
  tile: vector_tile::Tile,
  steps: u32,
  rel_x: u32,
  rel_y: u32,
) -> vector_tile::Tile {
  let mut out = tile;
  for mut layer in out.layers.iter_mut() {
    if layer.features.is_empty() {
      continue;
    }
    if layer.extent == None {
      continue;
    }
    let extent = layer.extent.unwrap();
    let tgt_tile_size = extent >> steps;
    layer.extent = Some(tgt_tile_size);

    let mut features: Vec<vector_tile::tile::Feature> = Vec::with_capacity(layer.features.len());
    for original_feature in &layer.features {
      let mut feature = original_feature.clone();

      let mut geometry = feature.geometry.clone();
      if !scale_geometry(&mut geometry, tgt_tile_size, rel_x, rel_y) {
        continue;
      }
      let clipped_geometry = clip_geometry(feature.r#type.unwrap(), &geometry, tgt_tile_size);
      if clipped_geometry.is_empty() {
        // this feature was completely clipped out of the tile, so we can remove it
        continue;
      }
      feature.geometry = clipped_geometry;

      features.push(feature);
    }

    layer.features = features;
  }
  out
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_scale_geometry() {
    let mut input_geom_1 = vec![9, 50, 34];
    scale_geometry(&mut input_geom_1, 1024, 0, 0);
    assert_eq!(input_geom_1, vec![9, 50, 34]);

    let mut input_geom_2 = vec![9, zz_enc(25), zz_enc(17)];
    scale_geometry(&mut input_geom_2, 1024, 1, 0);
    assert_eq!(input_geom_2, vec![9, zz_enc(25 - 1024), zz_enc(17)]);
  }
}
