{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    haskell-flake.url = "path:/Users/shivaraj/personal/haskell-flake";
  };
  outputs = inputs@{ self, nixpkgs, flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = nixpkgs.lib.systems.flakeExposed;
      imports = [ inputs.haskell-flake.flakeModule ];

      perSystem = { self', pkgs, config, ... }: {
        haskellProjects.default = {
          packages = {
            resource-pool.source = "0.4.0.0";
          };
        };
        # haskell-flake doesn't set the default package, but you can do it here.
        packages.default = self'.packages.redis-simple;
      };
    };
}
