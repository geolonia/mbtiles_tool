/*
 * Adapted from https://github.com/Turfjs/turf/blob/master/packages/turf-bbox-clip/lib/lineclip.ts
 * which was adapted from https://github.com/mapbox/lineclip/blob/master/index.js
 *
 * mapbox/lineclip
 * ISC License

Copyright (c) 2015, Mapbox

Permission to use, copy, modify, and/or distribute this software for any purpose
with or without fee is hereby granted, provided that the above copyright notice
and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS
OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER
TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF
THIS SOFTWARE.

 * turfjs
 * The MIT License (MIT)

Copyright (c) 2019 Morgan Herlocker

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

// Cohen-Sutherland line clipping algorithm, adapted to efficiently
// handle polylines rather than just segments

use crate::geom::{LineString, Point, Polygon};

type BoundingBox = (i32, i32, i32, i32);

pub fn lineclip(input: LineString, bbox: BoundingBox) -> Vec<LineString> {
  let coords: Vec<Point> = input.points;
  let len = coords.len();
  let mut code_a = bit_code(coords[0], &bbox);
  let mut part = Vec::<Point>::new();
  let mut last_code: u8;
  let mut code_b: u8;

  let mut result = Vec::<LineString>::new();

  for i in 1..len {
    let mut a = coords[i - 1];
    let mut b = coords[i];
    code_b = bit_code(b, &bbox);
    last_code = code_b;

    loop {
      if code_a | code_b == 0 {
        // accept
        part.push(a);

        if code_b != last_code {
          // segment went outside
          part.push(b);

          if i < len - 1 {
            // start a new line
            result.push(LineString {
              points: part.to_vec(),
            });
            part = vec![];
          }
        } else if i == len - 1 {
          part.push(b);
        }
        break;
      } else if code_a & code_b > 0 {
        // trivial reject
        break;
      } else if code_a > 0 {
        // a is outside, intersect with clip edge
        a = intersect(a, b, code_a, &bbox);
        code_a = bit_code(a, &bbox);
      } else {
        // b outside
        b = intersect(a, b, code_b, &bbox);
        code_b = bit_code(b, &bbox);
      }
    }

    code_a = last_code;
  }

  if !part.is_empty() {
    result.push(LineString {
      points: part.to_vec(),
    });
  }

  result
}

// Sutherland-Hodgeman polygon clipping algorithm

pub fn polygonclip(input: Polygon, bbox: BoundingBox) -> Polygon {
  let mut points = input.points;

  let mut result: Vec<Point>;
  let mut prev: Point;
  let mut prev_inside: bool;

  for edge in [1, 2, 4, 8].iter() {
    result = vec![];
    prev = points[points.len() - 1];
    prev_inside = (bit_code(prev, &bbox) & edge) == 0;

    for p in points.iter() {
      let inside = (bit_code(*p, &bbox) & edge) == 0;
      if inside != prev_inside {
        result.push(intersect(prev, *p, *edge, &bbox));
      }
      if inside {
        result.push(*p);
      }
      prev = *p;
      prev_inside = inside;
    }

    points = result;
    if points.is_empty() {
      break;
    }
  }

  Polygon { points }
}

// intersect a segment against one of the 4 lines that make up the bbox

fn intersect(a: Point, b: Point, edge: u8, bbox: &BoundingBox) -> Point {
  if edge & 8 > 0 {
    // top
    return Point {
      x: a.x + ((b.x - a.x) * (bbox.3 - a.y) / (b.y - a.y)),
      y: bbox.3,
    };
  } else if edge & 4 > 0 {
    // bottom
    return Point {
      x: a.x + ((b.x - a.x) * (bbox.1 - a.y) / (b.y - a.y)),
      y: bbox.1,
    };
  } else if edge & 2 > 0 {
    // right
    return Point {
      x: bbox.2,
      y: a.y + ((b.y - a.y) * (bbox.2 - a.x) / (b.x - a.x)),
    };
  } else if edge & 1 > 0 {
    // left
    return Point {
      x: bbox.0,
      y: a.y + ((b.y - a.y) * (bbox.0 - a.x) / (b.x - a.x)),
    };
  }

  panic!("No intersection");
}

// bit code reflects the point position relative to the bbox:

//         left  mid  right
//    top  1001  1000  1010
//    mid  0001  0000  0010
// bottom  0101  0100  0110

fn bit_code(coords: Point, bbox: &BoundingBox) -> u8 {
  let mut code: u8 = 0;

  if coords.x < bbox.0 {
    // left
    code |= 1;
  } else if coords.x > bbox.2 {
    // right
    code |= 2;
  }

  if coords.y < bbox.1 {
    // bottom
    code |= 4;
  } else if coords.y > bbox.3 {
    // top
    code |= 8;
  }

  code
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_lineclip() {
    assert_eq!(
      lineclip(
        LineString {
          points: vec![
            Point { x: -10, y: 10 },
            Point { x: 10, y: 10 },
            Point { x: 10, y: -10 },
            Point { x: 20, y: -10 },
            Point { x: 20, y: 10 },
            Point { x: 40, y: 10 },
            Point { x: 40, y: 20 },
            Point { x: 20, y: 20 },
            Point { x: 20, y: 40 },
            Point { x: 10, y: 40 },
            Point { x: 10, y: 20 },
            Point { x: 5, y: 20 },
            Point { x: -10, y: 20 },
          ]
        },
        (0, 0, 30, 30)
      ),
      vec![
        LineString {
          points: vec![
            Point { x: 0, y: 10 },
            Point { x: 10, y: 10 },
            Point { x: 10, y: 0 }
          ]
        },
        LineString {
          points: vec![
            Point { x: 20, y: 0 },
            Point { x: 20, y: 10 },
            Point { x: 30, y: 10 }
          ]
        },
        LineString {
          points: vec![
            Point { x: 30, y: 20 },
            Point { x: 20, y: 20 },
            Point { x: 20, y: 30 }
          ]
        },
        LineString {
          points: vec![
            Point { x: 10, y: 30 },
            Point { x: 10, y: 20 },
            Point { x: 5, y: 20 },
            Point { x: 0, y: 20 }
          ]
        },
      ]
    );

    assert_eq!(
      lineclip(
        LineString {
          points: vec![
            Point { x: 10, y: -10 },
            Point { x: 5, y: 5 },
            Point { x: 10, y: 10 }
          ]
        },
        (3, 3, 6, 6)
      ),
      vec![LineString {
        points: vec![
          Point { x: 6, y: 3 },
          Point { x: 5, y: 5 },
          Point { x: 6, y: 6 }
        ]
      }]
    );
  }

  #[test]
  fn test_polygonclip() {
    assert_eq!(
      polygonclip(
        Polygon {
          points: vec![
            Point { x: -10, y: 10 },
            Point { x: 0, y: 10 },
            Point { x: 10, y: 10 },
            Point { x: 10, y: 5 },
            Point { x: 10, y: -5 },
            Point { x: 10, y: -10 },
            Point { x: 20, y: -10 },
            Point { x: 20, y: 10 },
            Point { x: 40, y: 10 },
            Point { x: 40, y: 20 },
            Point { x: 20, y: 20 },
            Point { x: 20, y: 40 },
            Point { x: 10, y: 40 },
            Point { x: 10, y: 20 },
            Point { x: 5, y: 20 },
            Point { x: -10, y: 20 },
          ]
        },
        (0, 0, 30, 30)
      ),
      Polygon {
        points: vec![
          Point { x: 0, y: 10 },
          Point { x: 0, y: 10 },
          Point { x: 10, y: 10 },
          Point { x: 10, y: 5 },
          Point { x: 10, y: 0 },
          Point { x: 20, y: 0 },
          Point { x: 20, y: 10 },
          Point { x: 30, y: 10 },
          Point { x: 30, y: 20 },
          Point { x: 20, y: 20 },
          Point { x: 20, y: 30 },
          Point { x: 10, y: 30 },
          Point { x: 10, y: 20 },
          Point { x: 5, y: 20 },
          Point { x: 0, y: 20 },
        ]
      }
    );
  }
}
