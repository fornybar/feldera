{ pkgs, config, ... }:
let
  # rustToolchain = pkgs.rust-bin.stable."1.91.1".default;
  # cargoWithOpenSsl = pkgs.writeShellScriptBin "cargo" ''
  #   export OPENSSL_DIR="${pkgs.openssl.dev}"
  #   export OPENSSL_INCLUDE_DIR="${pkgs.openssl.dev}/include"
  #   export OPENSSL_LIB_DIR="${pkgs.openssl.out}/lib"
  #   export OPENSSL_NO_VENDOR=1
  #   export PKG_CONFIG_PATH="${pkgs.openssl.dev}/lib/pkgconfig:${pkgs.pkg-config}/lib/pkgconfig"
  #   exec ${rustToolchain}/bin/cargo "$@"
  # '';
  # pkgConfigWithOpenSsl = pkgs.writeShellScriptBin "pkg-config" ''
  #   export PKG_CONFIG_PATH="${pkgs.openssl.dev}/lib/pkgconfig:${pkgs.pkg-config}/lib/pkgconfig"
  #   exec ${pkgs.pkg-config}/bin/pkg-config "$@"
  # '';
#  embeddedPgLibraryPath = pkgs.lib.makeLibraryPath [
#    pkgs.krb5
#    pkgs.cyrus_sasl
#    pkgs.lz4
#    pkgs.libxml2_13
#    pkgs.zlib
#    pkgs.openssl
#  ];
in {
  languages = {
    rust = {
      enable = true;
      channel = "stable";
      version = "1.91.1";
      components = [ "rustc" "cargo" "clippy" "rustfmt" "rust-analyzer" ];
    };

    java =  {
      enable = true;
      jdk.package = pkgs.jdk21;
      maven.enable = true;
    };

    #python = {
    #  enable = true;
    #  package = pkgs.python310;
    #};

    #    go.enable = true; # Go is required to build aws-lc-fips-sys with FIPS (README.md#L116, .devcontainer/Dockerfile#L17-L36).
    #
    javascript = {
      enable = true;
      package = pkgs.nodejs_20;
      bun.enable = true;
    };
  };

  #git-hooks.enable = true;

  packages = with pkgs; [ # System dependencies beyond language toolchains.
    cmake
    openssl # libssl.dev
    cyrus_sasl # libsasl2-dev ASL headers for Kafka/postgres auth (libsasl2-dev in .devcontainer/Dockerfile#L17-L36).
    # golang-go (required to build aws-lc-fips-sys when using rustls FIPS)
    zstd # libztd-dev
    clang # C/C++ toolchain dependency (README.md#L119, CONTRIBUTING.md#L20, .devcontainer/Dockerfile#L40-L43).
    nats-server # Retained from prior devenv.nix for local dev; no explicit doc reference.
    gcc
    zlib
    graphviz # Required per contributor dependencies (CONTRIBUTING.md#L28).
    nodePackages.mermaid-cli # Render Mermaid diagrams (mmdc) for docs and reviews.

    prek # pre-comitt
    #llvmPackages.libclang # libclang needed by bindgen and builds (.devcontainer/Dockerfile#L40-L43).

    #    #curl # Used by build scripts and tooling ( .devcontainer/Dockerfile#L17-L36 ).
    #    #krb5 # Kerberos authentication library for PostgreSQL (libgssapi-krb5-2 in .devcontainer/Dockerfile#L17-L36).
    #        gettext # Common build tool; included in build-essential equivalence ( .devcontainer/Dockerfile#L17-L36 ).
    #        git # Source control needed for dev workflow ( .devcontainer/Dockerfile#L17-L36 ).
    #        gnumake # Build tool used by native dependencies (build-essential in .devcontainer/Dockerfile#L17-L36).
    #        graphviz # Required per contributor dependencies (CONTRIBUTING.md#L28).
    #        jq # CLI utility used by scripts ( .devcontainer/Dockerfile#L17-L36 ).
    #    #    jdk21 # Java 21 for SQL compiler (README.md#L120, .devcontainer/Dockerfile#L40-L43).
    #    #        maven # SQL compiler build tool (README.md#L121, CONTRIBUTING.md#L27, .devcontainer/Dockerfile#L40-L43).
    #        lz4 # Runtime dependency for embedded PostgreSQL binaries.
    #        libxml2_13 # Runtime dependency for embedded PostgreSQL binaries (libxml2.so.2).
    #        nats-server # Retained from prior devenv.nix for local dev; no explicit doc reference.
    #        numactl # Used in perf tooling and docker setup ( .devcontainer/Dockerfile#L40-L43 ).
    #        protobuf # Protocol buffer compiler ( .devcontainer/Dockerfile#L17-L36 ).
    #        python310Packages.pip # pip needed for python deps in demos ( .devcontainer/Dockerfile#L40-L43, #L60-L62 ).
    #        tzdata # Time zone database needed by embedded PostgreSQL.
    #        unzip # Required for tooling downloads ( .devcontainer/Dockerfile#L17-L36 ).
    #    #postgresql_15
  ]; # End packages.

  enterShell = ''
    machete_bin="$CARGO_INSTALL_ROOT/bin/cargo-machete"
    if ! [ -x "$machete_bin" ]; then
      cargo install --locked --root "$CARGO_INSTALL_ROOT" cargo-machete --version 0.7.0 --force
    elif ! "$machete_bin" --version | grep -Eq '^(cargo-machete )?0\.7\.0$'; then
      cargo install --locked --root "$CARGO_INSTALL_ROOT" cargo-machete --version 0.7.0 --force
    fi
  '';

  env = { # Environment variables to point builds to system toolchains.
    #    JAVA_HOME = "${pkgs.jdk21}"; # Ensure Java tooling resolves JDK 21 (README.md#L120, .devcontainer/Dockerfile#L40-L43).
    LIBCLANG_PATH = pkgs.lib.makeLibraryPath [ pkgs.llvmPackages.libclang ];
    #LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib"; # libclang path; for Rust bindgen ( .devcontainer/Dockerfile#L40-L43 ).
    #OPENSSL_DIR = "${pkgs.openssl.dev}"; # OpenSSL dev root used by Rust crates (README.md#L114).
    #OPENSSL_INCLUDE_DIR = "${pkgs.openssl.dev}/include"; # OpenSSL headers location (README.md#L114).
    #OPENSSL_LIB_DIR = "${pkgs.openssl.out}/lib"; # OpenSSL libs location (README.md#L114).
    #OPENSSL_NO_VENDOR = "1"; # Force using system OpenSSL (README.md#L114).
    #PKG_CONFIG_PATH = "${pkgs.openssl.dev}/lib/pkgconfig"; # OpenSSL pkg-config path.
    CFLAGS="-DJEMALLOC_STRERROR_R_RETURNS_CHAR_WITH_GNU_SOURCE"; # FIX until new relase of jemallocator:  https://github.com/tikv/jemallocator/pull/116
    #    LD_LIBRARY_PATH = embeddedPgLibraryPath; # Needed by embedded PostgreSQL binaries cached in ~/.theseus.
    #    NIX_LD_LIBRARY_PATH = embeddedPgLibraryPath; # Ensure nix-ld resolves the same libs if invoked.
    #    TZDIR = "${pkgs.tzdata}/share/zoneinfo"; # Provide zoneinfo data for embedded PostgreSQL.
  }; # End environment variables.

  tasks = { # devenv tasks for common workflows.
    "feldera:build-compiler" = { # Build SQL compiler helper.
      exec = "./build.sh"; # Command from README build instructions (README.md#L125-L130).
      cwd = "${config.git.root}/sql-to-dbsp-compiler"; # Run from compiler directory (README.md#L125-L130).
    }; # End build-compiler task.

    "feldera:build-manager" = { # Build pipeline-manager helper.
      exec = "cargo build --bin=pipeline-manager"; # Build pipeline-manager binary.
      cwd = config.git.root; # Run from repository root.
    }; # End build-manager task.

    "feldera:run-manager" = { # Run pipeline-manager helper.
      exec = "cargo run --bin=pipeline-manager"; # Command from README run instructions (README.md#L132-L136).
      cwd = config.git.root; # Run from repository root (README.md#L132-L136).
    }; # End run-manager task.

    "feldera:code-check" = { # Run pipeline-manager helper.
      exec = "prek run --show-diff-on-failure --color=always --all-files";
      cwd = config.git.root;
    }; # End run-manager task.

  }; # End tasks.
} # End devenv configuration.
