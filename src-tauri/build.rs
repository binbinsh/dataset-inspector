fn main() {
    tauri_build::build();

    // Vendored sph2pipe (v2.5) is used to decode Shorten-compressed SPHERE audio.
    // We only build it on Unix-like targets where the original sources compile.
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os == "windows" {
        return;
    }

    println!("cargo:rerun-if-changed=vendor/sph2pipe/sph2pipe.c");
    println!("cargo:rerun-if-changed=vendor/sph2pipe/shorten_x.c");
    println!("cargo:rerun-if-changed=vendor/sph2pipe/file_headers.c");
    println!("cargo:rerun-if-changed=vendor/sph2pipe/sph_convert.h");
    println!("cargo:rerun-if-changed=vendor/sph2pipe/ulaw.h");
    println!("cargo:rerun-if-changed=vendor/sph2pipe/bitshift.h");
    println!("cargo:rerun-if-changed=src/sph2pipe_embed.c");

    cc::Build::new()
        .include("vendor/sph2pipe")
        .include("src")
        .file("vendor/sph2pipe/shorten_x.c")
        .file("vendor/sph2pipe/sph2pipe_shorten_decode.c")
        // Build as GNU89 to match the vendored sources.
        .flag_if_supported("-std=gnu89")
        .warnings(false)
        .compile("litdata_sph2pipe_shorten");
}
