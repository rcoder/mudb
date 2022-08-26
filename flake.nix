{
  description = "Minimal rust wasm32-unknown-unknown example";

  inputs = {
    nixpkgs.url = github:nixos/nixpkgs/nixos-unstable;
    flake-utils.url = github:numtide/flake-utils;
    rust-overlay = {
      url = github:oxalica/rust-overlay;
      inputs.nixpkgs.follows = "nixpkgs";
    };
    /*darwin.url = github:LnL7/nix-darwin;*/
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [
            rust-overlay.overlays.default
        ];
        pkgs = import nixpkgs { inherit system overlays; };
        #frameworks = pkgs.darwin.apple_sdk.frameworks;

        inputs = (with pkgs; [
            just
            rustc
            cargo
            cargo-flamegraph
            rust-analyzer
            pkg-config
            libiconv
            openssl
        ]);/* ++ (with frameworks; [
            Foundation
            Cocoa
            Security
        ]);*/
      in
      {
        defaultPackage = pkgs.rustPlatform.buildRustPackage {
          pname = "mudb";
          version = "0.1.0";

          src = ./.;

          cargoLock = {
            lockFile = ./Cargo.lock;
          };

          nativeBuildInputs = inputs;
        };


        devShell = pkgs.mkShell {
            nativeBuildInputs = inputs;
        };
      }
    );
}
