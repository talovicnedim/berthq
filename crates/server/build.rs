// Re-embed the dashboard whenever its build output appears or changes.
fn main() {
    println!("cargo:rerun-if-changed=../../apps/dashboard/build");
}
