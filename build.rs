extern crate tonic_build;

fn main() -> Result<(), Box<dyn std::error::Error>> {
  tonic_build::compile_protos("src/raft/raft.proto")?;

  Ok(())
}
