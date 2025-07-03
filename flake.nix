{
  inputs = {
    nixpkgs.url      = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url  = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };
      in
        {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            bun
            clang
            cmake
            dart-sass
            gcc
            jdk
            maven
            openssl
            pkg-config
            rust-bin.stable.latest.default
          ]
            # FIX for darwin: Provides the mig command used by the build scripts
            ++ pkgs.lib.optional pkgs.stdenv.hostPlatform.isDarwin pkgs.darwin.bootstrap_cmds;

          # FIX until new relase of jemallocator:  https://github.com/tikv/jemallocator/pull/116
          CFLAGS=pkgs.lib.optional pkgs.stdenv.hostPlatform.isx86_64"-DJEMALLOC_STRERROR_R_RETURNS_CHAR_WITH_GNU_SOURCE";

          shellHook = ''
            export LIBCLANG_PATH="${pkgs.llvmPackages.libclang.lib}/lib";
          '';
        };
      }
    );
}
