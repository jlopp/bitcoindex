{
  description = "bitcoindex - Bitcoin blockchain indexer (blk*.dat -> Parquet -> ClickHouse)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };

        # Keep the Nix version in lockstep with Cargo.toml rather than
        # duplicating it: the workspace version is the single source of truth.
        version = (pkgs.lib.importTOML ./Cargo.toml).workspace.package.version;

        # The native build inputs RocksDB's bindgen step and libzmq's
        # pkg-config probe need. libclang is the one bindgen dlopens, so its
        # path is exported rather than just put on PATH.
        nativeBuildInputs = with pkgs; [
          pkg-config
          cmake
          llvmPackages.clang
          llvmPackages.libclang
        ];

        # `zeromq`: zmq-sys probes for a system libzmq before vendoring, so
        #   supplying it avoids building libzmq (and its libsodium) from source.
        # `zstd`: parquet's `zstd` feature pulls in zstd-sys, and nixpkgs'
        #   rustPlatform cargo setup hook exports ZSTD_SYS_USE_PKG_CONFIG=1.
        #   That forces zstd-sys to probe pkg-config for a system libzstd
        #   instead of compiling its vendored copy, so `zstd` must be on the
        #   pkg-config path or the build dies with "No package 'libzstd' found".
        #   This is Nix-only: a plain `cargo build` leaves the var unset and
        #   builds the vendored zstd, so it needs no libzstd-dev.
        # reqwest uses rustls-tls, so no openssl is needed.
        buildInputs = with pkgs; [
          zeromq
          zstd
        ];

        bidx = pkgs.rustPlatform.buildRustPackage {
          pname = "bitcoindex";
          inherit version nativeBuildInputs buildInputs;

          src = ./.;
          cargoLock.lockFile = ./Cargo.lock;

          # rocksdb 8.x relies on a transitive <cstdint> include that newer
          # GCC/clang no longer provide (same workaround as the Dockerfile).
          CXXFLAGS = "-include cstdint";
          LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";

          # Only the two end-user binaries; the other crates are libraries.
          cargoBuildFlags = [
            "-p"
            "bidx-cli"
            "-p"
            "bidx-udf"
          ];

          # The suite needs a live ClickHouse and bitcoind for the loader and
          # live paths, so it is not runnable in the sandbox.
          doCheck = false;

          # Ship the same config + unit the .deb/.rpm carry, so `nix profile
          # install` and the packages agree on layout.
          postInstall = ''
            install -Dm0644 packaging/bidx.toml \
              $out/etc/bidx/bidx.toml
            install -Dm0644 packaging/systemd/bidx-serve.service \
              $out/lib/systemd/system/bidx-serve.service
          '';

          meta = {
            description = "Bitcoin blockchain indexer: blk*.dat -> Parquet -> ClickHouse";
            homepage = "https://github.com/jlopp/bitcoindex";
            license = pkgs.lib.licenses.mit;
            mainProgram = "bidx";
            platforms = pkgs.lib.platforms.linux;
          };
        };
      in
      {
        packages = {
          default = bidx;
          bitcoindex = bidx;
        };

        # `nix develop` gives a shell that can run `cargo build`/`cargo test`
        # directly, with the same native deps the package uses.
        devShells.default = pkgs.mkShell {
          inherit nativeBuildInputs buildInputs;
          packages = with pkgs; [
            rustc
            cargo
            clippy
            rustfmt
          ];
          CXXFLAGS = "-include cstdint";
          LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
          RUST_BACKTRACE = "1";
        };

        formatter = pkgs.nixfmt-rfc-style;
      }
    );
}
