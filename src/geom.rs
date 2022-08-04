#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Point {
  pub x: i32,
  pub y: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineString {
  pub points: Vec<Point>,
}
pub type Polygon = LineString;
