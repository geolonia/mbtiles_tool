use std::sync::Arc;

// in order of: x, y, z
pub type Tile = (u32, u32, u32);

#[derive(Clone)]
pub struct TileData {
  pub tile: Tile,
  pub data: Arc<Vec<u8>>,
}

pub fn tile_is_ancestor(tile: &Tile, ancestor: &Tile) -> bool {
  if tile.2 < ancestor.2 {
    return false;
  }
  let z_diff = tile.2 - ancestor.2;
  let tile_at_anc_z = (
    tile.0.checked_shr(z_diff).unwrap_or(0),
    tile.1.checked_shr(z_diff).unwrap_or(0),
  );

  tile_at_anc_z.0 == ancestor.0 && tile_at_anc_z.1 == ancestor.1
}

pub fn get_children(tile: &Tile) -> Vec<Tile> {
  vec![
    (tile.0 * 2, tile.1 * 2, tile.2 + 1),
    (tile.0 * 2 + 1, tile.1 * 2, tile.2 + 1),
    (tile.0 * 2, tile.1 * 2 + 1, tile.2 + 1),
    (tile.0 * 2 + 1, tile.1 * 2 + 1, tile.2 + 1),
  ]
}

// Recursively get all children until `zoom` for a tile.
pub fn get_children_until_zoom(tile: &Tile, zoom: u8) -> Vec<Tile> {
  let mut children = get_children(tile);
  let mut children_to_add = Vec::new();
  for child in children.iter_mut() {
    if (child.2 as u8) < zoom {
      children_to_add.append(&mut get_children_until_zoom(child, zoom));
    }
  }
  children.append(&mut children_to_add);
  children
}

pub const TILE_RELATIVE_POSITION_TRUTH_TABLE: [(u32, u32); 4] = [(0, 0), (1, 0), (1, 1), (0, 1)];

pub fn get_position_in_parent(tile: &Tile) -> ((u32, u32), Tile) {
  let parent = (tile.0 >> 1, tile.1 >> 1, tile.2 - 1);
  let children = vec![
    (parent.0 * 2, parent.1 * 2, parent.2 + 1),     // 0 = 0, 0
    (parent.0 * 2 + 1, parent.1 * 2, parent.2 + 1), // 1 = 1, 0
    (parent.0 * 2 + 1, parent.1 * 2 + 1, parent.2 + 1), // 2 = 1, 1
    (parent.0 * 2, parent.1 * 2 + 1, parent.2 + 1), // 3 = 0, 1
  ];
  let index_in_parent = children
    .iter()
    .position(|&(x, y, z)| x == tile.0 && y == tile.1 && z == tile.2)
    .unwrap();
  (TILE_RELATIVE_POSITION_TRUTH_TABLE[index_in_parent], parent)
}

pub fn get_relative_position_in_ancestor(tile: &Tile, target_zoom: u8) -> (Tile, u32, (u32, u32)) {
  let tgt_z = target_zoom as u32;
  if tgt_z > tile.2 {
    panic!(
      "the requested zoom ({}) is higher than the the tile ({})",
      target_zoom, tile.2
    );
  }
  let steps = tile.2 - tgt_z;
  let mut current_tile = *tile;
  let mut relative_positions: Vec<(u32, u32)> = Vec::new();
  while current_tile.2 > tgt_z {
    let (rp1, new_tile) = get_position_in_parent(&current_tile);
    relative_positions.insert(0, rp1);
    current_tile = new_tile;
  }

  (
    current_tile,
    steps,
    relative_positions
      .into_iter()
      .enumerate()
      .fold((0, 0), |(x, y), (idx, (x1, y1))| {
        let multiplier = 2u32.pow((steps - 1) - (idx) as u32);
        (x + (x1 * multiplier), y + (y1 * multiplier))
      }),
  )
}

pub fn flip_x(tile: Tile) -> Tile {
  let flipped_row = (1 << tile.2) - 1 - tile.1;
  (tile.0, flipped_row, tile.2)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_tile_is_ancestor() {
    assert!(tile_is_ancestor(&(0, 0, 0), &(0, 0, 0)));
    assert!(tile_is_ancestor(&(1, 1, 1), &(0, 0, 0)));
    assert!(tile_is_ancestor(&(3, 3, 2), &(0, 0, 0)));
    assert!(tile_is_ancestor(&(3, 3, 3), &(0, 0, 0)));

    assert!(tile_is_ancestor(&(9, 7, 4), &(4, 3, 3)));
    assert!(!tile_is_ancestor(&(0, 7, 4), &(4, 3, 3)));
  }

  #[test]
  fn test_get_children() {
    assert_eq!(
      get_children(&(0, 0, 0)),
      vec![(0, 0, 1), (1, 0, 1), (0, 1, 1), (1, 1, 1)]
    );
    assert_eq!(
      get_children(&(1, 1, 1)),
      vec![(2, 2, 2), (3, 2, 2), (2, 3, 2), (3, 3, 2)]
    );
    assert_eq!(
      get_children(&(3, 3, 2)),
      vec![(6, 6, 3), (7, 6, 3), (6, 7, 3), (7, 7, 3)]
    );
  }

  #[test]
  fn test_get_children_until_zoom() {
    assert_eq!(
      get_children_until_zoom(&(0, 0, 0), 2),
      vec![
        (0, 0, 1),
        (1, 0, 1),
        (0, 1, 1),
        (1, 1, 1),
        (0, 0, 2),
        (1, 0, 2),
        (0, 1, 2),
        (1, 1, 2),
        (2, 0, 2),
        (3, 0, 2),
        (2, 1, 2),
        (3, 1, 2),
        (0, 2, 2),
        (1, 2, 2),
        (0, 3, 2),
        (1, 3, 2),
        (2, 2, 2),
        (3, 2, 2),
        (2, 3, 2),
        (3, 3, 2)
      ]
    );

    assert_eq!(
      get_children_until_zoom(&(7274, 3224, 13), 14),
      vec![
        (14548, 6448, 14),
        (14549, 6448, 14),
        (14548, 6449, 14),
        (14549, 6449, 14),
      ]
    )
  }

  #[test]
  fn test_get_position_in_parent() {
    assert_eq!(get_position_in_parent(&(0, 0, 1)), ((0, 0), (0, 0, 0)));
    assert_eq!(get_position_in_parent(&(0, 1, 1)), ((0, 1), (0, 0, 0)));
    assert_eq!(get_position_in_parent(&(1, 1, 1)), ((1, 1), (0, 0, 0)));
    assert_eq!(get_position_in_parent(&(1, 0, 1)), ((1, 0), (0, 0, 0)));

    assert_eq!(
      get_position_in_parent(&(14548, 6449, 14)),
      ((0, 1), (7274, 3224, 13))
    );
    assert_eq!(
      get_position_in_parent(&(14548, 6448, 14)),
      ((0, 0), (7274, 3224, 13))
    );
    assert_eq!(
      get_position_in_parent(&(28675, 13057, 15)),
      ((1, 1), (14337, 6528, 14))
    );
  }

  #[test]
  fn test_get_relative_position_in_ancestor() {
    assert_eq!(
      get_relative_position_in_ancestor(&(0, 0, 1), 0),
      ((0, 0, 0), 1, (0, 0))
    );
    assert_eq!(
      get_relative_position_in_ancestor(&(0, 1, 1), 0),
      ((0, 0, 0), 1, (0, 1))
    );
    assert_eq!(
      get_relative_position_in_ancestor(&(1, 1, 1), 0),
      ((0, 0, 0), 1, (1, 1))
    );
    assert_eq!(
      get_relative_position_in_ancestor(&(1, 0, 1), 0),
      ((0, 0, 0), 1, (1, 0))
    );

    assert_eq!(
      get_relative_position_in_ancestor(&(227, 100, 8), 4),
      ((14, 6, 4), 4, (3, 4))
    );

    assert_eq!(
      get_relative_position_in_ancestor(&(28675, 13057, 15), 14),
      ((14337, 6528, 14), 1, (1, 1))
    )
  }
}
