// in order of: x, y, z
pub type Tile = (u32, u32, u32);

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
}
