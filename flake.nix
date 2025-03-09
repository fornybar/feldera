{
  outputs = { self, nixpkgs }:
  let
    system = "x86_64-linux";
    pkgs = nixpkgs.legacyPackages.${system};
  in {
    devShells.${system}.default = pkgs.mkShell {
      packages = with pkgs; [
        rustc
        cargo
        bun
        openssl
        pkg-config
        gcc
        cmake
      ]
      # FIX for darwin: Provides the mig command used by the build scripts
      ++ pkgs.lib.optional pkgs.stdenv.hostPlatform.isDarwin pkgs.darwin.bootstrap_cmds;

      # FIX until new relase of jemallocator:  https://github.com/tikv/jemallocator/pull/116
      CFLAGS=pkgs.lib.optional pkgs.stdenv.hostPlatform.isx86_64"-DJEMALLOC_STRERROR_R_RETURNS_CHAR_WITH_GNU_SOURCE";
    };
  };
}
