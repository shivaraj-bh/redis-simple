{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    haskell-flake.url = "github:srid/haskell-flake";
    hedis.url = "github:juspay/hedis/f8b11e0512b864f165a4a086713b41a0e3d622ea";
    hedis.flake = false;
  };
  outputs = inputs@{ self, nixpkgs, flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = nixpkgs.lib.systems.flakeExposed;
      imports = [ inputs.haskell-flake.flakeModule ];

      perSystem = { self', pkgs, config, ... }: {
        haskellProjects.default = {
          packages = {
            resource-pool.source = "0.4.0.0";
            hedis.source = inputs.hedis;
          };
          settings = {
            redis-simple.benchmark = true;
            hedis.check = false;
            hedis.patches = [ ./patch/hedis_juspay_cluster.patch ];
          };
          devShell.benchmark = true;
          devShell.hoogle = false;
        };
        # haskell-flake doesn't set the default package, but you can do it here.
        packages.default = self'.packages.redis-simple;
      };
    };
}
